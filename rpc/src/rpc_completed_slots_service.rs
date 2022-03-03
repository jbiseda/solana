use {
    crate::rpc_subscriptions::RpcSubscriptions,
    solana_client::rpc_response::{SlotShredStats, SlotUpdate},
    solana_ledger::blockstore::CompletedSlotsReceiver,
    solana_sdk::timing::timestamp,
    std::{
        sync::Arc,
        thread::{Builder, JoinHandle},
    },
};

fn compute_range_vec(turbine_indices: &Vec<u32>) -> Vec<Vec<u32>> {
    let mut turbine_indices: Vec<_> = turbine_indices.iter().copied().collect();
    turbine_indices.sort_unstable();
    let mut turbine_ranges: Vec<Vec<u32>> = Vec::new();
    let mut iter = turbine_indices.iter();
    let mut left = None;
    let mut prev = None;
    while let Some(x) = iter.next() {
        if left.is_none() {
            left = Some(x);
        }
        if prev.is_none() {
            prev = Some(x);
        } else {
            if *x != prev.unwrap() + 1 {
                if left == prev {
                    turbine_ranges.push(vec![*left.unwrap()]);
                } else {
                    turbine_ranges.push(vec![*left.unwrap(), *prev.unwrap()]);
                }
                left = Some(x);
            }
            prev = Some(x);
        }
    }
    turbine_ranges
}

pub struct RpcCompletedSlotsService;
impl RpcCompletedSlotsService {
    pub fn spawn(
        completed_slots_receiver: CompletedSlotsReceiver,
        rpc_subscriptions: Arc<RpcSubscriptions>,
    ) -> JoinHandle<()> {
        Builder::new()
            .name("solana-rpc-completed-slots-service".to_string())
            .spawn(move || {
                for slots_and_stats in completed_slots_receiver.iter() {
                    for (slot, slot_stats) in slots_and_stats {
                        let stats = slot_stats.map(|stats| {
                            let turbine_ranges = compute_range_vec(&stats.turbine_indices);
                            error!("### RANGES {:?}", &turbine_ranges);
                            SlotShredStats {
                                num_shreds: stats.num_shreds as u64,
                                num_repaired: stats.num_repaired as u64,
                                num_recovered: stats.num_recovered as u64,
                                turbine_indices: stats.turbine_indices,
                            }
                        });
                        error!("### CompletedSlotsService slot {}, {:?}", slot, &stats);
                        rpc_subscriptions.notify_slot_update(SlotUpdate::Completed {
                            slot,
                            timestamp: timestamp(),
                            stats,
                        });
                    }
                }
            })
            .unwrap()
    }
}
