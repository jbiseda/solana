//! The `sigverify_stage` implements the signature verification stage of the TPU. It
//! receives a list of lists of packets and outputs the same list, but tags each
//! top-level list with a list of booleans, telling the next stage whether the
//! signature in that packet is valid. It assumes each packet contains one
//! transaction. All processing is done on the CPU by default and on a GPU
//! if perf-libs are available

use crate::sigverify;
use crossbeam_channel::{SendError, Sender as CrossbeamSender};
use solana_measure::measure::Measure;
use solana_metrics::datapoint_debug;
use solana_perf::packet::Packets;
use solana_perf::perf_libs;
use solana_sdk::timing;
use solana_streamer::streamer::{self, PacketReceiver, StreamerError};
use std::sync::mpsc::{Receiver, RecvTimeoutError};
use std::sync::{Arc, Mutex};
use std::thread::{self, Builder, JoinHandle};
use std::time::Instant;
use thiserror::Error;

const RECV_BATCH_MAX_CPU: usize = 1_000;
const RECV_BATCH_MAX_GPU: usize = 5_000;

#[derive(Error, Debug)]
pub enum SigVerifyServiceError {
    #[error("send packets batch error")]
    Send(#[from] SendError<Vec<Packets>>),

    #[error("streamer error")]
    Streamer(#[from] StreamerError),
}

type Result<T> = std::result::Result<T, SigVerifyServiceError>;

pub struct SigVerifyStage {
    thread_hdls: Vec<JoinHandle<()>>,
}

struct SigVerifierStats {
    batch_time_us_hist: histogram::Histogram,
    batch_time_pp_us_hist: histogram::Histogram,
    packets_per_batch: histogram::Histogram,
    batches_hist: histogram::Histogram,
    total_packets: usize,
    total_batches: usize,
}

pub trait SigVerifier {
    fn verify_batch(&self, batch: Vec<Packets>) -> Vec<Packets>;
}

#[derive(Default, Clone)]
pub struct DisabledSigVerifier {}

impl SigVerifier for DisabledSigVerifier {
    fn verify_batch(&self, mut batch: Vec<Packets>) -> Vec<Packets> {
        let before_ts = Instant::now();
        sigverify::ed25519_verify_disabled(&mut batch);
        let after_ts = Instant::now();
        batch
            .iter_mut()
            .for_each(|pkts| pkts.timer.set_verify(before_ts, after_ts));
        batch
    }
}

impl SigVerifyStage {
    #[allow(clippy::new_ret_no_self)]
    pub fn new<T: SigVerifier + 'static + Send + Clone>(
        packet_receiver: Receiver<Packets>,
        verified_sender: CrossbeamSender<Vec<Packets>>,
        verifier: T,
    ) -> Self {
        let thread_hdls = Self::verifier_services(packet_receiver, verified_sender, verifier);
        Self { thread_hdls }
    }

    fn verifier<T: SigVerifier>(
        recvr: &Arc<Mutex<PacketReceiver>>,
        sendr: &CrossbeamSender<Vec<Packets>>,
        id: usize,
        verifier: &T,
        stats: &mut SigVerifierStats,
    ) -> Result<()> {
        let (batch, len, recv_time) = streamer::recv_batch(
            &recvr.lock().expect("'recvr' lock in fn verifier"),
            if perf_libs::api().is_some() {
                RECV_BATCH_MAX_GPU
            } else {
                RECV_BATCH_MAX_CPU
            },
        )?;

        let mut verify_batch_time = Measure::start("sigverify_batch_time");
        let batch_len = batch.len();
        debug!(
            "@{:?} verifier: verifying: {} id: {}",
            timing::timestamp(),
            len,
            id
        );

        let before_ts = Instant::now();
        let mut batch = verifier.verify_batch(batch);
        let after_ts = Instant::now();

        let mut total_packets = 0;
        batch.iter_mut().for_each(|pkts| {
            pkts.timer.set_verify(before_ts, after_ts);
            total_packets += pkts.packets.len();
        });

        sendr.send(batch)?;

        //sendr.send(verifier.verify_batch(batch))?;
        verify_batch_time.stop();

        //        let total_packets: usize = batch.iter().map(|pkts| pkts.packets.len()).sum();

        stats
            .batch_time_us_hist
            .increment(verify_batch_time.as_us())
            .unwrap();
        stats
            .batch_time_pp_us_hist
            .increment(verify_batch_time.as_us() / (total_packets as u64))
            .unwrap();
        stats
            .packets_per_batch
            .increment(total_packets as u64)
            .unwrap();
        stats.total_packets += total_packets;
        stats.total_batches += batch_len;
        stats.batches_hist.increment(batch_len as u64).unwrap();

        debug!(
            "@{:?} verifier: done. batches: {} total verify time: {:?} id: {} verified: {} v/s {}",
            timing::timestamp(),
            batch_len,
            verify_batch_time.as_ms(),
            id,
            len,
            (len as f32 / verify_batch_time.as_s())
        );

        datapoint_debug!(
            "sigverify_stage-total_verify_time",
            ("num_batches", batch_len, i64),
            ("num_packets", len, i64),
            ("verify_time_ms", verify_batch_time.as_ms(), i64),
            ("recv_time", recv_time, i64),
        );

        Ok(())
    }

    fn verifier_service<T: SigVerifier + 'static + Send + Clone>(
        packet_receiver: Arc<Mutex<PacketReceiver>>,
        verified_sender: CrossbeamSender<Vec<Packets>>,
        id: usize,
        verifier: &T,
    ) -> JoinHandle<()> {
        let verifier = verifier.clone();
        let mut stats = SigVerifierStats {
            batch_time_us_hist: histogram::Histogram::new(),
            batch_time_pp_us_hist: histogram::Histogram::new(),
            packets_per_batch: histogram::Histogram::new(),
            batches_hist: histogram::Histogram::new(),
            total_packets: 0,
            total_batches: 0,
        };
        let mut last_stats = Instant::now();

        Builder::new()
            .name(format!("solana-verifier-{}", id))
            .spawn(move || loop {
                if let Err(e) = Self::verifier(
                    &packet_receiver,
                    &verified_sender,
                    id,
                    &verifier,
                    &mut stats,
                ) {
                    match e {
                        SigVerifyServiceError::Streamer(StreamerError::RecvTimeout(
                            RecvTimeoutError::Disconnected,
                        )) => break,
                        SigVerifyServiceError::Streamer(StreamerError::RecvTimeout(
                            RecvTimeoutError::Timeout,
                        )) => (),
                        SigVerifyServiceError::Send(_) => {
                            break;
                        }
                        _ => error!("{:?}", e),
                    }
                }

                if last_stats.elapsed().as_secs() > 2 {
                    let ts = Instant::now();
                    let mut test_hist = histogram::Histogram::new();
                    for i in 0..10_000 {
                        test_hist.increment(i * 7).unwrap();
                    }
                    let test_mean = test_hist.mean().unwrap();
                    let test_50pct = test_hist.percentile(50.0).unwrap();
                    let test_90pct = test_hist.percentile(90.0).unwrap();
                    let hist_elapsed = ts.elapsed().as_micros();

                    datapoint_info!(
                        "verifier_service-timing",
                        ("test_hist_mean", test_mean, i64),
                        ("test_hist_50pct", test_50pct, i64),
                        ("test_hist_90pct", test_90pct, i64),
                        ("test_hist_elapsed_10000", hist_elapsed, i64),
                        (
                            "batch_time_us_50pct",
                            stats.batch_time_us_hist.percentile(50.0).unwrap_or(0),
                            i64
                        ),
                        (
                            "batch_time_us_90pct",
                            stats.batch_time_us_hist.percentile(90.0).unwrap_or(0),
                            i64
                        ),
                        (
                            "batch_time_us_min",
                            stats.batch_time_us_hist.minimum().unwrap_or(0),
                            i64
                        ),
                        (
                            "batch_time_us_max",
                            stats.batch_time_us_hist.maximum().unwrap_or(0),
                            i64
                        ),
                        (
                            "batch_time_us_mean",
                            stats.batch_time_us_hist.mean().unwrap_or(0),
                            i64
                        ),
                        (
                            "batch_time_pp_us_50pct",
                            stats.batch_time_pp_us_hist.percentile(50.0).unwrap_or(0),
                            i64
                        ),
                        (
                            "batch_time_pp_us_90pct",
                            stats.batch_time_pp_us_hist.percentile(90.0).unwrap_or(0),
                            i64
                        ),
                        (
                            "batch_time_pp_us_min",
                            stats.batch_time_pp_us_hist.minimum().unwrap_or(0),
                            i64
                        ),
                        (
                            "batch_time_pp_us_max",
                            stats.batch_time_pp_us_hist.maximum().unwrap_or(0),
                            i64
                        ),
                        (
                            "batch_time_pp_us_mean",
                            stats.batch_time_pp_us_hist.mean().unwrap_or(0),
                            i64
                        ),
                        (
                            "packets_per_batch_50pct",
                            stats.packets_per_batch.percentile(50.0).unwrap_or(0),
                            i64
                        ),
                        (
                            "packets_per_batch_90pct",
                            stats.packets_per_batch.percentile(90.0).unwrap_or(0),
                            i64
                        ),
                        (
                            "packets_per_batch_min",
                            stats.packets_per_batch.minimum().unwrap_or(0),
                            i64
                        ),
                        (
                            "packets_per_batch_max",
                            stats.packets_per_batch.maximum().unwrap_or(0),
                            i64
                        ),
                        (
                            "packets_per_batch_mean",
                            stats.packets_per_batch.mean().unwrap_or(0),
                            i64
                        ),
                        (
                            "batches_50pct",
                            stats.batches_hist.percentile(50.0).unwrap_or(0),
                            i64
                        ),
                        (
                            "batches_90pct",
                            stats.batches_hist.percentile(90.0).unwrap_or(0),
                            i64
                        ),
                        (
                            "batches_min",
                            stats.batches_hist.minimum().unwrap_or(0),
                            i64
                        ),
                        (
                            "batches_max",
                            stats.batches_hist.maximum().unwrap_or(0),
                            i64
                        ),
                        ("batches_mean", stats.batches_hist.mean().unwrap_or(0), i64),
                        ("total_packets", stats.total_packets, i64),
                        ("total_batches", stats.total_batches, i64),
                    );
                    stats.batch_time_us_hist.clear();
                    stats.batch_time_pp_us_hist.clear();
                    stats.packets_per_batch.clear();
                    stats.batches_hist.clear();
                    stats.total_packets = 0;
                    stats.total_batches = 0;
                    last_stats = Instant::now();
                }
            })
            .unwrap()
    }

    fn verifier_services<T: SigVerifier + 'static + Send + Clone>(
        packet_receiver: PacketReceiver,
        verified_sender: CrossbeamSender<Vec<Packets>>,
        verifier: T,
    ) -> Vec<JoinHandle<()>> {
        let receiver = Arc::new(Mutex::new(packet_receiver));
        (0..4)
            .map(|id| {
                Self::verifier_service(receiver.clone(), verified_sender.clone(), id, &verifier)
            })
            .collect()
    }

    pub fn join(self) -> thread::Result<()> {
        for thread_hdl in self.thread_hdls {
            thread_hdl.join()?;
        }
        Ok(())
    }
}
