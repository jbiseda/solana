#![allow(clippy::rc_buffer)]

use {
    super::{
        broadcast_utils::{self, ReceiveResults},
        *,
    },
    crate::{
        broadcast_stage::broadcast_utils::UnfinishedSlotInfo, cluster_nodes::ClusterNodesCache,
    },
    solana_entry::entry::Entry,
    solana_ledger::shred::{
        ProcessShredsStats, Shred, Shredder, MAX_DATA_SHREDS_PER_FEC_BLOCK,
        SHRED_TICK_REFERENCE_MASK,
    },
    solana_sdk::{
        signature::Keypair,
        timing::{duration_as_us, AtomicInterval},
    },
    std::{sync::RwLock, time::Duration},
};

struct FindLastShredsInSlotIterator<'a> {
    shreds: &'a [Shred],
    begin_search_index: usize,
}

impl<'a> FindLastShredsInSlotIterator<'a> {
    fn new(shreds: &'a [Shred]) -> Self {
        Self {
            shreds,
            begin_search_index: 0,
        }
    }
}

/// Iterator returns slice of shreds over the range (returned_range_start, returned_range_end),
// /from `self.slice`.
///
/// In this slice, `returned_range_end` is the index of the immediate next
/// shred `s` after `returned_range_start`  where `s` is the last shred in some slot.
///
/// If no such `s` exists, then `returned_range_end == self.shreds.len()`.
impl<'a> Iterator for FindLastShredsInSlotIterator<'a> {
    type Item = &'a [Shred];

    fn next(&mut self) -> Option<Self::Item> {
        if self.begin_search_index >= self.shreds.len() {
            return None;
        }

        let mut returned_range_start = self.begin_search_index;
        let mut returned_range_end = self.begin_search_index;

        for shred in self.shreds[self.begin_search_index..].iter() {
            if shred.last_in_slot() {
                if returned_range_start == returned_range_end {
                    returned_range_start += 1;
                } else {
                    break;
                }
            }
            returned_range_end += 1;
        }

        self.begin_search_index = returned_range_end + 1;
        if returned_range_start == returned_range_end {
            None
        } else {
            Some(&self.shreds[returned_range_start..returned_range_end])
        }
    }
}

#[derive(Clone)]
pub struct BroadcastMissingShredsRun {
    process_shreds_stats: ProcessShredsStats,
    transmit_shreds_stats: Arc<Mutex<SlotBroadcastStats<TransmitShredsStats>>>,
    insert_shreds_stats: Arc<Mutex<SlotBroadcastStats<InsertShredsStats>>>,
    unfinished_slot: Option<UnfinishedSlotInfo>,
    current_slot_and_parent: Option<(u64, u64)>,
    slot_broadcast_start: Option<Instant>,
    shred_version: u16,
    last_datapoint_submit: Arc<AtomicInterval>,
    num_batches: usize,
    cluster_nodes_cache: Arc<ClusterNodesCache<BroadcastStage>>,
    broadcast_iter: u64,
}

impl BroadcastMissingShredsRun {
    pub(super) fn new(shred_version: u16) -> Self {
        let cluster_nodes_cache = Arc::new(ClusterNodesCache::<BroadcastStage>::new(
            CLUSTER_NODES_CACHE_NUM_EPOCH_CAP,
            CLUSTER_NODES_CACHE_TTL,
        ));
        Self {
            process_shreds_stats: ProcessShredsStats::default(),
            transmit_shreds_stats: Arc::default(),
            insert_shreds_stats: Arc::default(),
            unfinished_slot: None,
            current_slot_and_parent: None,
            slot_broadcast_start: None,
            shred_version,
            last_datapoint_submit: Arc::default(),
            num_batches: 0,
            cluster_nodes_cache,
            broadcast_iter: 0,
        }
    }

    // If the current slot has changed, generates an empty shred indicating
    // last shred in the previous slot, along with coding shreds for the data
    // shreds buffered.
    fn finish_prev_slot(
        &mut self,
        keypair: &Keypair,
        max_ticks_in_slot: u8,
        stats: &mut ProcessShredsStats,
    ) -> Vec<Shred> {
        let (current_slot, _) = self.current_slot_and_parent.unwrap();
        match self.unfinished_slot {
            None => Vec::default(),
            Some(ref state) if state.slot == current_slot => Vec::default(),
            Some(ref mut state) => {
                let parent_offset = state.slot - state.parent;
                let reference_tick = max_ticks_in_slot & SHRED_TICK_REFERENCE_MASK;
                let fec_set_index =
                    Shredder::fec_set_index(state.next_shred_index, state.fec_set_offset);
                let mut shred = Shred::new_from_data(
                    state.slot,
                    state.next_shred_index,
                    parent_offset as u16,
                    None, // data
                    true, // is_last_in_fec_set
                    true, // is_last_in_slot
                    reference_tick,
                    self.shred_version,
                    fec_set_index.unwrap(),
                );
                Shredder::sign_shred(keypair, &mut shred);
                state.data_shreds_buffer.push(shred.clone());
                let mut shreds = make_coding_shreds(
                    keypair,
                    &mut self.unfinished_slot,
                    true, // is_last_in_slot
                    stats,
                );
                shreds.insert(0, shred);
                self.report_and_reset_stats(true);
                self.unfinished_slot = None;
                shreds
            }
        }
    }

    fn entries_to_data_shreds(
        &mut self,
        keypair: &Keypair,
        entries: &[Entry],
        blockstore: &Blockstore,
        reference_tick: u8,
        is_slot_end: bool,
        process_stats: &mut ProcessShredsStats,
    ) -> Vec<Shred> {
        let (slot, parent_slot) = self.current_slot_and_parent.unwrap();
        let (next_shred_index, fec_set_offset) = match &self.unfinished_slot {
            Some(state) => (state.next_shred_index, state.fec_set_offset),
            None => match blockstore.meta(slot).unwrap() {
                Some(slot_meta) => {
                    let shreds_consumed = slot_meta.consumed as u32;
                    (shreds_consumed, shreds_consumed)
                }
                None => (0, 0),
            },
        };
        let (data_shreds, next_shred_index) =
            Shredder::new(slot, parent_slot, reference_tick, self.shred_version)
                .unwrap()
                .entries_to_data_shreds(
                    keypair,
                    entries,
                    is_slot_end,
                    next_shred_index,
                    fec_set_offset,
                    process_stats,
                );
        let mut data_shreds_buffer = match &mut self.unfinished_slot {
            Some(state) => {
                assert_eq!(state.slot, slot);
                std::mem::take(&mut state.data_shreds_buffer)
            }
            None => Vec::default(),
        };
        data_shreds_buffer.extend(data_shreds.clone());
        self.unfinished_slot = Some(UnfinishedSlotInfo {
            next_shred_index,
            slot,
            parent: parent_slot,
            data_shreds_buffer,
            fec_set_offset,
        });
        data_shreds
    }

    fn process_receive_results(
        &mut self,
        keypair: &Keypair,
        blockstore: &Arc<Blockstore>,
        socket_sender: &Sender<(Arc<Vec<Shred>>, Option<BroadcastShredBatchInfo>)>,
        blockstore_sender: &Sender<(Arc<Vec<Shred>>, Option<BroadcastShredBatchInfo>)>,
        receive_results: ReceiveResults,
    ) -> Result<()> {
        let mut receive_elapsed = receive_results.time_elapsed;
        let num_entries = receive_results.entries.len();
        let bank = receive_results.bank.clone();
        let last_tick_height = receive_results.last_tick_height;
        inc_new_counter_info!("broadcast_service-entries_received", num_entries);
        let old_broadcast_start = self.slot_broadcast_start;
        let old_num_batches = self.num_batches;
        if self.current_slot_and_parent.is_none()
            || bank.slot() != self.current_slot_and_parent.unwrap().0
        {
            self.slot_broadcast_start = Some(Instant::now());
            self.num_batches = 0;
            let slot = bank.slot();
            let parent_slot = bank.parent_slot();

            self.current_slot_and_parent = Some((slot, parent_slot));
            receive_elapsed = Duration::new(0, 0);
        }

        let mut process_stats = ProcessShredsStats::default();

        let mut to_shreds_time = Measure::start("broadcast_to_shreds");

        // 1) Check if slot was interrupted
        let prev_slot_shreds =
            self.finish_prev_slot(keypair, bank.ticks_per_slot() as u8, &mut process_stats);

        // 2) Convert entries to shreds and coding shreds
        let is_last_in_slot = last_tick_height == bank.max_tick_height();
        let reference_tick = bank.tick_height() % bank.ticks_per_slot();
        let data_shreds = self.entries_to_data_shreds(
            keypair,
            &receive_results.entries,
            blockstore,
            reference_tick as u8,
            is_last_in_slot,
            &mut process_stats,
        );
        // Insert the first shred so blockstore stores that the leader started this block
        // This must be done before the blocks are sent out over the wire.
        if !data_shreds.is_empty() && data_shreds[0].index() == 0 {
            let first = vec![data_shreds[0].clone()];
            blockstore
                .insert_shreds(first, None, true)
                .expect("Failed to insert shreds in blockstore");
        }
        to_shreds_time.stop();

        let mut get_leader_schedule_time = Measure::start("broadcast_get_leader_schedule");
        // Broadcast the last shred of the interrupted slot if necessary
        if !prev_slot_shreds.is_empty() {
            let slot = prev_slot_shreds[0].slot();
            let batch_info = Some(BroadcastShredBatchInfo {
                slot,
                num_expected_batches: Some(old_num_batches + 1),
                slot_start_ts: old_broadcast_start.expect(
                    "Old broadcast start time for previous slot must exist if the previous slot
                 was interrupted",
                ),
                was_interrupted: true,
            });
            let shreds = Arc::new(prev_slot_shreds);
            debug_assert!(shreds.iter().all(|shred| shred.slot() == slot));
            socket_sender.send((shreds.clone(), batch_info.clone()))?;
            blockstore_sender.send((shreds, batch_info))?;
        }

        // Increment by two batches, one for the data batch, one for the coding batch.
        self.num_batches += 2;
        let num_expected_batches = {
            if is_last_in_slot {
                Some(self.num_batches)
            } else {
                None
            }
        };
        let batch_info = Some(BroadcastShredBatchInfo {
            slot: bank.slot(),
            num_expected_batches,
            slot_start_ts: self
                .slot_broadcast_start
                .expect("Start timestamp must exist for a slot if we're broadcasting the slot"),
            was_interrupted: false,
        });
        get_leader_schedule_time.stop();

        // Send data shreds
        let data_shreds = Arc::new(data_shreds);
        debug_assert!(data_shreds.iter().all(|shred| shred.slot() == bank.slot()));
        socket_sender.send((data_shreds.clone(), batch_info.clone()))?;
        blockstore_sender.send((data_shreds, batch_info))?;

        // Make no coding shreds so recovery of missing shred
        // cannot happen

        process_stats.shredding_elapsed = to_shreds_time.as_us();
        process_stats.get_leader_schedule_elapsed = get_leader_schedule_time.as_us();
        process_stats.receive_elapsed = duration_as_us(&receive_elapsed);

        self.process_shreds_stats.update(&process_stats);

        if last_tick_height == bank.max_tick_height() {
            self.report_and_reset_stats(false);
            self.unfinished_slot = None;
        }

        Ok(())
    }

    fn insert(
        &mut self,
        blockstore: &Arc<Blockstore>,
        shreds: Arc<Vec<Shred>>,
        broadcast_shred_batch_info: Option<BroadcastShredBatchInfo>,
    ) {
        // Insert shreds into blockstore
        let insert_shreds_start = Instant::now();
        // The first shred is inserted synchronously
        let data_shreds = if !shreds.is_empty() && shreds[0].index() == 0 {
            shreds[1..].to_vec()
        } else {
            shreds.to_vec()
        };
        blockstore
            .insert_shreds(data_shreds, None, true)
            .expect("Failed to insert shreds in blockstore");
        let insert_shreds_elapsed = insert_shreds_start.elapsed();
        let new_insert_shreds_stats = InsertShredsStats {
            insert_shreds_elapsed: duration_as_us(&insert_shreds_elapsed),
            num_shreds: shreds.len(),
        };
        self.update_insertion_metrics(&new_insert_shreds_stats, &broadcast_shred_batch_info);
    }

    fn update_insertion_metrics(
        &mut self,
        new_insertion_shreds_stats: &InsertShredsStats,
        broadcast_shred_batch_info: &Option<BroadcastShredBatchInfo>,
    ) {
        let mut insert_shreds_stats = self.insert_shreds_stats.lock().unwrap();
        insert_shreds_stats.update(new_insertion_shreds_stats, broadcast_shred_batch_info);
    }

    fn broadcast(
        &mut self,
        sock: &UdpSocket,
        cluster_info: &ClusterInfo,
        shreds: Arc<Vec<Shred>>,
        broadcast_shred_batch_info: Option<BroadcastShredBatchInfo>,
        bank_forks: &Arc<RwLock<BankForks>>,
    ) -> Result<()> {
        trace!("Broadcasting {:?} shreds", shreds.len());
        let mut transmit_stats = TransmitShredsStats::default();
        // Broadcast the shreds
        let mut transmit_time = Measure::start("broadcast_shreds");

        error!("MISSING_SHREDS broadcast");

        if shreds.is_empty() {
            return Ok(());
        }

        error!("{}:{} broadcast shreds.len:{} iter:{}", file!(), line!(), shreds.len(), self.broadcast_iter);

        if self.broadcast_iter < 4 {
            for shred_slice in FindLastShredsInSlotIterator::new(&shreds) {
                // Check for last shreds in each slot in case there are multiple slots
                // included in the input `shreds`. We want to avoid broadcasting
                // such shreds
                broadcast_shreds(
                    sock,
                    // `end_send_index` is non-inclusive to avoid broadcasting the last
                    // shred in each slot.
                    shred_slice,
                    &self.cluster_nodes_cache,
                    &self.last_datapoint_submit,
                    &mut transmit_stats,
                    cluster_info,
                    bank_forks,
                    cluster_info.socket_addr_space(),
                )?;
            }
        } else {
            error!("BROADCAST ALL SHREDS {}", shreds.len());
            broadcast_shreds(
                sock,
                &shreds,
                &self.cluster_nodes_cache,
                &self.last_datapoint_submit,
                &mut transmit_stats,
                cluster_info,
                bank_forks,
                cluster_info.socket_addr_space(),
            )?;
        }

        self.broadcast_iter += 1;

        transmit_time.stop();

        transmit_stats.transmit_elapsed = transmit_time.as_us();
        transmit_stats.num_shreds = shreds.len();

        // Process metrics
        self.update_transmit_metrics(&transmit_stats, &broadcast_shred_batch_info);
        Ok(())
    }

    fn update_transmit_metrics(
        &mut self,
        new_transmit_shreds_stats: &TransmitShredsStats,
        broadcast_shred_batch_info: &Option<BroadcastShredBatchInfo>,
    ) {
        let mut transmit_shreds_stats = self.transmit_shreds_stats.lock().unwrap();
        transmit_shreds_stats.update(new_transmit_shreds_stats, broadcast_shred_batch_info);
    }

    fn report_and_reset_stats(&mut self, was_interrupted: bool) {
        let stats = &self.process_shreds_stats;
        let unfinished_slot = self.unfinished_slot.as_ref().unwrap();
        if was_interrupted {
            datapoint_info!(
                "broadcast-process-shreds-interrupted-stats",
                ("slot", unfinished_slot.slot as i64, i64),
                ("shredding_time", stats.shredding_elapsed, i64),
                ("receive_time", stats.receive_elapsed, i64),
                (
                    "num_data_shreds",
                    unfinished_slot.next_shred_index as i64,
                    i64
                ),
                (
                    "get_leader_schedule_time",
                    stats.get_leader_schedule_elapsed,
                    i64
                ),
                ("serialize_shreds_time", stats.serialize_elapsed, i64),
                ("gen_data_time", stats.gen_data_elapsed, i64),
                ("gen_coding_time", stats.gen_coding_elapsed, i64),
                ("sign_coding_time", stats.sign_coding_elapsed, i64),
            );
        } else {
            datapoint_info!(
                "broadcast-process-shreds-stats",
                ("slot", unfinished_slot.slot as i64, i64),
                ("shredding_time", stats.shredding_elapsed, i64),
                ("receive_time", stats.receive_elapsed, i64),
                (
                    "num_data_shreds",
                    unfinished_slot.next_shred_index as i64,
                    i64
                ),
                (
                    "slot_broadcast_time",
                    self.slot_broadcast_start.unwrap().elapsed().as_micros() as i64,
                    i64
                ),
                (
                    "get_leader_schedule_time",
                    stats.get_leader_schedule_elapsed,
                    i64
                ),
                ("serialize_shreds_time", stats.serialize_elapsed, i64),
                ("gen_data_time", stats.gen_data_elapsed, i64),
                ("gen_coding_time", stats.gen_coding_elapsed, i64),
                ("sign_coding_time", stats.sign_coding_elapsed, i64),
            );
        }
        self.process_shreds_stats.reset();
    }
}

// Consumes data_shreds_buffer returning corresponding coding shreds.
fn make_coding_shreds(
    keypair: &Keypair,
    unfinished_slot: &mut Option<UnfinishedSlotInfo>,
    is_slot_end: bool,
    stats: &mut ProcessShredsStats,
) -> Vec<Shred> {
    let data_shreds = match unfinished_slot {
        None => Vec::default(),
        Some(unfinished_slot) => {
            let size = unfinished_slot.data_shreds_buffer.len();
            // Consume a multiple of 32, unless this is the slot end.
            let offset = if is_slot_end {
                0
            } else {
                size % MAX_DATA_SHREDS_PER_FEC_BLOCK as usize
            };
            unfinished_slot
                .data_shreds_buffer
                .drain(0..size - offset)
                .collect()
        }
    };
    Shredder::data_shreds_to_coding_shreds(keypair, &data_shreds, is_slot_end, stats).unwrap()
}

impl BroadcastRun for BroadcastMissingShredsRun {
    fn run(
        &mut self,
        keypair: &Keypair,
        blockstore: &Arc<Blockstore>,
        receiver: &Receiver<WorkingBankEntry>,
        socket_sender: &Sender<(Arc<Vec<Shred>>, Option<BroadcastShredBatchInfo>)>,
        blockstore_sender: &Sender<(Arc<Vec<Shred>>, Option<BroadcastShredBatchInfo>)>,
    ) -> Result<()> {
        let receive_results = broadcast_utils::recv_slot_entries(receiver)?;
        // TODO: Confirm that last chunk of coding shreds
        // will not be lost or delayed for too long.
        self.process_receive_results(
            keypair,
            blockstore,
            socket_sender,
            blockstore_sender,
            receive_results,
        )
    }
    fn transmit(
        &mut self,
        receiver: &Arc<Mutex<TransmitReceiver>>,
        cluster_info: &ClusterInfo,
        sock: &UdpSocket,
        bank_forks: &Arc<RwLock<BankForks>>,
    ) -> Result<()> {
        let (shreds, batch_info) = receiver.lock().unwrap().recv()?;
        self.broadcast(sock, cluster_info, shreds, batch_info, bank_forks)
    }
    fn record(
        &mut self,
        receiver: &Arc<Mutex<RecordReceiver>>,
        blockstore: &Arc<Blockstore>,
    ) -> Result<()> {
        let (shreds, slot_start_ts) = receiver.lock().unwrap().recv()?;
        self.insert(blockstore, shreds, slot_start_ts);
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_find_last_shreds_in_slot_iterator() {
        // Case 1: Empty shreds
        let shreds = vec![];
        assert!(FindLastShredsInSlotIterator::new(&shreds).next().is_none());

        let not_last_shred_in_slot = Shred::new_empty_data_shred();
        let mut last_shred_in_slot = Shred::new_empty_data_shred();
        last_shred_in_slot.set_last_in_slot();

        // Case 2: One shred marked as last
        let shreds = vec![last_shred_in_slot.clone()];
        assert!(FindLastShredsInSlotIterator::new(&shreds).next().is_none());

        // Case 3: Multiple last shreds marked as last
        let shreds = vec![last_shred_in_slot.clone(); 5];
        assert!(FindLastShredsInSlotIterator::new(&shreds).next().is_none());

        // Case 4: Multiple not last shreds
        let shreds = vec![not_last_shred_in_slot.clone(); 5];
        assert_eq!(
            FindLastShredsInSlotIterator::new(&shreds)
                .into_iter()
                .collect::<Vec<&[Shred]>>(),
            vec![&shreds[..]],
        );

        // Case 5: Alternating not last shred, and last shred, with final shred
        // in vector as the last shred
        let shreds = vec![
            not_last_shred_in_slot.clone(),
            last_shred_in_slot.clone(),
            not_last_shred_in_slot.clone(),
            last_shred_in_slot.clone(),
        ];
        assert_eq!(
            FindLastShredsInSlotIterator::new(&shreds)
                .into_iter()
                .collect::<Vec<&[Shred]>>(),
            vec![&[not_last_shred_in_slot.clone()]; 2],
        );

        // Case 6: Alternating not last shred, and last shred, with final shred
        // in vector as the not last shred
        let shreds = vec![
            last_shred_in_slot.clone(),
            not_last_shred_in_slot.clone(),
            last_shred_in_slot,
            not_last_shred_in_slot.clone(),
        ];
        assert_eq!(
            FindLastShredsInSlotIterator::new(&shreds)
                .into_iter()
                .collect::<Vec<&[Shred]>>(),
            vec![&[not_last_shred_in_slot]; 2],
        );
    }
}
