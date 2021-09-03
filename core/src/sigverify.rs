//! The `sigverify` module provides digital signature verification functions.
//! By default, signatures are verified in parallel using all available CPU
//! cores.  When perf-libs are available signature verification is offloaded
//! to the GPU.
//!

use crate::sigverify_stage::SigVerifier;
use solana_perf::cuda_runtime::PinnedVec;
use solana_perf::packet::Packets;
use solana_perf::recycler::Recycler;
use solana_perf::sigverify;
pub use solana_perf::sigverify::{
    batch_size, ed25519_verify_cpu, ed25519_verify_disabled, init, TxOffset,
};
use std::time::Instant;

#[derive(Clone)]
pub struct TransactionSigVerifier {
    recycler: Recycler<TxOffset>,
    recycler_out: Recycler<PinnedVec<u8>>,
}

impl Default for TransactionSigVerifier {
    fn default() -> Self {
        init();
        Self {
            recycler: Recycler::warmed(50, 4096),
            recycler_out: Recycler::warmed(50, 4096),
        }
    }
}

impl SigVerifier for TransactionSigVerifier {
    fn verify_batch(&self, mut batch: Vec<Packets>) -> Vec<Packets> {
        let before_ts = Instant::now();
        sigverify::ed25519_verify(&mut batch, &self.recycler, &self.recycler_out);
        let after_ts = Instant::now();
        batch
            .iter_mut()
            .for_each(|pkts| pkts.timer.set_verify(before_ts, after_ts));
        batch
    }
}
