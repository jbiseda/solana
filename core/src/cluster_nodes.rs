use {
    crate::{broadcast_stage::BroadcastStage, retransmit_stage::RetransmitStage},
    itertools::Itertools,
    lru::LruCache,
    rand::SeedableRng,
    rand_chacha::ChaChaRng,
    rayon::{prelude::*, ThreadPool},
    solana_gossip::{
        cluster_info::{compute_retransmit_peers, ClusterInfo},
        contact_info::ContactInfo,
        crds_gossip_pull::CRDS_GOSSIP_PULL_CRDS_TIMEOUT_MS,
        weighted_shuffle::{weighted_best, weighted_shuffle, WeightedShuffle},
    },
    solana_ledger::{
        blockstore::SlotStats, leader_schedule_cache::LeaderScheduleCache, shred::Shred,
    },
    solana_rayon_threadlimit::get_thread_count,
    solana_runtime::bank::Bank,
    solana_sdk::{
        clock::{Epoch, Slot},
        feature_set,
        hash::hashv,
        pubkey::Pubkey,
        timing::timestamp,
    },
    solana_streamer::socket::SocketAddrSpace,
    std::{
        any::TypeId,
        cell::RefCell,
        cmp::Reverse,
        collections::HashMap,
        marker::PhantomData,
        net::SocketAddr,
        ops::Deref,
        sync::{Arc, Mutex},
        time::{Duration, Instant},
    },
};

thread_local!(static PAR_THREAD_POOL: RefCell<ThreadPool> = RefCell::new(rayon::ThreadPoolBuilder::new()
                    .num_threads(get_thread_count())
                    .thread_name(|ix| format!("cluster_nodes_{}", ix))
                    .build()
                    .unwrap()));

#[allow(clippy::large_enum_variant)]
#[derive(Clone, Debug, PartialEq)]
enum NodeId {
    // TVU node obtained through gossip (staked or not).
    ContactInfo(ContactInfo),
    // Staked node with no contact-info in gossip table.
    Pubkey(Pubkey),
}

#[derive(Clone, Debug, PartialEq)]
struct Node {
    node: NodeId,
    stake: u64,
}

#[derive(Debug, Default)]
pub struct ShredDistributionStakes {
    neighborhood: u64,
    parent: u64,
    parent_neighborhood: u64,
}

pub struct ClusterNodes<T> {
    pubkey: Pubkey, // The local node itself.
    // All staked nodes + other known tvu-peers + the node itself;
    // sorted by (stake, pubkey) in descending order.
    nodes: Vec<Node>,
    // Reverse index from nodes pubkey to their index in self.nodes.
    index: HashMap<Pubkey, /*index:*/ usize>,
    weighted_shuffle: WeightedShuffle</*stake:*/ u64>,
    // Weights and indices for sampling peers. weighted_{shuffle,best} expect
    // weights >= 1. For backward compatibility we use max(1, stake) for
    // weights and exclude nodes with no contact-info.
    compat_index: Vec<(/*weight:*/ u64, /*index:*/ usize)>,
    _phantom: PhantomData<T>,
}

type CacheEntry<T> = Option<(/*as of:*/ Instant, Arc<ClusterNodes<T>>)>;

pub struct ClusterNodesCache<T> {
    // Cache entries are wrapped in Arc<Mutex<...>>, so that, when needed, only
    // one thread does the computations to update the entry for the epoch.
    cache: Mutex<LruCache<Epoch, Arc<Mutex<CacheEntry<T>>>>>,
    ttl: Duration, // Time to live.
}

fn get_parent_index(index: usize, fanout: usize) -> Option<usize> {
    if index < fanout {
        return None;
    }
    let offset = index % fanout;
    let anchor = index - offset;
    let neighborhood = anchor / fanout - 1;
    let neighborhood_offset = neighborhood % fanout;
    let parent_anchor = neighborhood - neighborhood_offset;
    let parent_index = parent_anchor + offset;
    Some(parent_index)
}

fn get_neighborhood_stake(index: usize, fanout: usize, nodes: &[&Node]) -> u64 {
    let offset = index % fanout;
    let anchor = index - offset;
    (anchor..)
        .take(fanout)
        .map(|i| nodes.get(i))
        .while_some()
        .map(|n| n.stake)
        .sum()
}

impl Node {
    #[inline]
    fn pubkey(&self) -> Pubkey {
        match &self.node {
            NodeId::Pubkey(pubkey) => *pubkey,
            NodeId::ContactInfo(node) => node.id,
        }
    }

    #[inline]
    fn contact_info(&self) -> Option<&ContactInfo> {
        match &self.node {
            NodeId::Pubkey(_) => None,
            NodeId::ContactInfo(node) => Some(node),
        }
    }
}

impl ShredDistributionStakes {
    fn total(&self) -> u64 {
        self.neighborhood + self.parent_neighborhood
    }
}

impl<T> ClusterNodes<T> {
    pub(crate) fn num_peers(&self) -> usize {
        self.compat_index.len()
    }

    // A peer is considered live if they generated their contact info recently.
    pub(crate) fn num_peers_live(&self, now: u64) -> usize {
        self.compat_index
            .iter()
            .filter_map(|(_, index)| self.nodes[*index].contact_info())
            .filter(|node| {
                let elapsed = if node.wallclock < now {
                    now - node.wallclock
                } else {
                    node.wallclock - now
                };
                elapsed < CRDS_GOSSIP_PULL_CRDS_TIMEOUT_MS
            })
            .count()
    }
}

impl ClusterNodes<BroadcastStage> {
    pub fn new(cluster_info: &ClusterInfo, stakes: &HashMap<Pubkey, u64>) -> Self {
        new_cluster_nodes(cluster_info, stakes)
    }

    pub(crate) fn get_broadcast_addrs(
        &self,
        shred: &Shred,
        root_bank: &Bank,
        fanout: usize,
        socket_addr_space: &SocketAddrSpace,
    ) -> Vec<SocketAddr> {
        const MAX_CONTACT_INFO_AGE: Duration = Duration::from_secs(2 * 60);
        let shred_seed = shred.seed(self.pubkey, root_bank);
        if !enable_turbine_peers_shuffle_patch(shred.slot(), root_bank) {
            if let Some(node) = self.get_broadcast_peer(shred_seed) {
                if socket_addr_space.check(&node.tvu) {
                    return vec![node.tvu];
                }
            }
            return Vec::default();
        }
        let mut rng = ChaChaRng::from_seed(shred_seed);
        let index = match self.weighted_shuffle.first(&mut rng) {
            None => return Vec::default(),
            Some(index) => index,
        };
        if let Some(node) = self.nodes[index].contact_info() {
            let now = timestamp();
            let age = Duration::from_millis(now.saturating_sub(node.wallclock));
            if age < MAX_CONTACT_INFO_AGE
                && ContactInfo::is_valid_address(&node.tvu, socket_addr_space)
            {
                return vec![node.tvu];
            }
        }
        let mut rng = ChaChaRng::from_seed(shred_seed);
        let nodes: Vec<&Node> = self
            .weighted_shuffle
            .clone()
            .shuffle(&mut rng)
            .map(|index| &self.nodes[index])
            .collect();
        if nodes.is_empty() {
            return Vec::default();
        }
        let (neighbors, children) = compute_retransmit_peers(fanout, 0, &nodes);
        neighbors[..1]
            .iter()
            .filter_map(|node| Some(node.contact_info()?.tvu))
            .chain(
                neighbors[1..]
                    .iter()
                    .filter_map(|node| Some(node.contact_info()?.tvu_forwards)),
            )
            .chain(
                children
                    .iter()
                    .filter_map(|node| Some(node.contact_info()?.tvu)),
            )
            .filter(|addr| ContactInfo::is_valid_address(addr, socket_addr_space))
            .collect()
    }

    /// Returns the root of turbine broadcast tree, which the leader sends the
    /// shred to.
    fn get_broadcast_peer(&self, shred_seed: [u8; 32]) -> Option<&ContactInfo> {
        if self.compat_index.is_empty() {
            None
        } else {
            let index = weighted_best(&self.compat_index, shred_seed);
            match &self.nodes[index].node {
                NodeId::ContactInfo(node) => Some(node),
                NodeId::Pubkey(_) => panic!("this should not happen!"),
            }
        }
    }
}

impl ClusterNodes<RetransmitStage> {
    pub fn new(cluster_info: &ClusterInfo, stakes: &HashMap<Pubkey, u64>) -> Self {
        new_cluster_nodes(cluster_info, stakes)
    }

    pub(crate) fn get_retransmit_addrs(
        &self,
        slot_leader: Pubkey,
        shred: &Shred,
        root_bank: &Bank,
        fanout: usize,
    ) -> Vec<SocketAddr> {
        let (neighbors, children) =
            self.get_retransmit_peers(slot_leader, shred, root_bank, fanout);
        // If the node is on the critical path (i.e. the first node in each
        // neighborhood), it should send the packet to tvu socket of its
        // children and also tvu_forward socket of its neighbors. Otherwise it
        // should only forward to tvu_forwards socket of its children.
        if neighbors[0].pubkey() != self.pubkey {
            return children
                .iter()
                .filter_map(|node| Some(node.contact_info()?.tvu_forwards))
                .collect();
        }
        // First neighbor is this node itself, so skip it.
        neighbors[1..]
            .iter()
            .filter_map(|node| Some(node.contact_info()?.tvu_forwards))
            .chain(
                children
                    .iter()
                    .filter_map(|node| Some(node.contact_info()?.tvu)),
            )
            .collect()
    }

    fn get_retransmit_peers(
        &self,
        slot_leader: Pubkey,
        shred: &Shred,
        root_bank: &Bank,
        fanout: usize,
    ) -> (
        Vec<&Node>, // neighbors
        Vec<&Node>, // children
    ) {
        let shred_seed = shred.seed(slot_leader, root_bank);
        if !enable_turbine_peers_shuffle_patch(shred.slot(), root_bank) {
            return self.get_retransmit_peers_compat(shred_seed, fanout, slot_leader);
        }
        let mut weighted_shuffle = self.weighted_shuffle.clone();
        // Exclude slot leader from list of nodes.
        if slot_leader == self.pubkey {
            error!("retransmit from slot leader: {}", slot_leader);
        } else if let Some(index) = self.index.get(&slot_leader) {
            weighted_shuffle.remove_index(*index);
        };
        let mut rng = ChaChaRng::from_seed(shred_seed);
        let nodes: Vec<_> = weighted_shuffle
            .shuffle(&mut rng)
            .map(|index| &self.nodes[index])
            .collect();
        let self_index = nodes
            .iter()
            .position(|node| node.pubkey() == self.pubkey)
            .unwrap();
        let (neighbors, children) = compute_retransmit_peers(fanout, self_index, &nodes);
        // Assert that the node itself is included in the set of neighbors, at
        // the right offset.
        debug_assert_eq!(neighbors[self_index % fanout].pubkey(), self.pubkey);
        (neighbors, children)
    }

    fn get_retransmit_peers_compat(
        &self,
        shred_seed: [u8; 32],
        fanout: usize,
        slot_leader: Pubkey,
    ) -> (
        Vec<&Node>, // neighbors
        Vec<&Node>, // children
    ) {
        // Exclude leader from list of nodes.
        let (weights, index): (Vec<u64>, Vec<usize>) = if slot_leader == self.pubkey {
            error!("retransmit from slot leader: {}", slot_leader);
            self.compat_index.iter().copied().unzip()
        } else {
            self.compat_index
                .iter()
                .filter(|(_, i)| self.nodes[*i].pubkey() != slot_leader)
                .copied()
                .unzip()
        };
        let index: Vec<_> = {
            let shuffle = weighted_shuffle(weights.into_iter(), shred_seed);
            shuffle.into_iter().map(|i| index[i]).collect()
        };
        let self_index = index
            .iter()
            .position(|i| self.nodes[*i].pubkey() == self.pubkey)
            .unwrap();
        let (neighbors, children) = compute_retransmit_peers(fanout, self_index, &index);
        // Assert that the node itself is included in the set of neighbors, at
        // the right offset.
        debug_assert_eq!(
            self.nodes[neighbors[self_index % fanout]].pubkey(),
            self.pubkey
        );
        let neighbors = neighbors.into_iter().map(|i| &self.nodes[i]).collect();
        let children = children.into_iter().map(|i| &self.nodes[i]).collect();
        (neighbors, children)
    }

    pub fn get_deterministic_shred_distribution_stakes_by_slot_and_index(
        &self,
        slot: Slot,
        shred_index: u32,
        root_bank: &Bank,
        fanout: usize,
        leader_schedule_cache: &LeaderScheduleCache,
    ) -> ShredDistributionStakes {
        let leader_pubkey = leader_schedule_cache
            .slot_leader_at(slot, Some(root_bank))
            .unwrap();

        if leader_pubkey == self.pubkey {
            // TODO?
        }

        let weighted_shuffle = self.weighted_shuffle.clone();

        // TODO dedup the shred seed calculation here
        let shred_seed = hashv(&[
            &slot.to_le_bytes(),
            &shred_index.to_le_bytes(),
            &leader_pubkey.to_bytes(),
        ])
        .to_bytes();

        let mut rng = ChaChaRng::from_seed(shred_seed);

        let nodes: Vec<_> = weighted_shuffle
            .shuffle(&mut rng)
            .map(|index| &self.nodes[index])
            .collect();
        let self_index = nodes
            .iter()
            .position(|node| node.pubkey() == self.pubkey)
            .unwrap();

        let mut stakes = ShredDistributionStakes {
            neighborhood: get_neighborhood_stake(self_index, fanout, &nodes),
            parent: u64::default(),
            parent_neighborhood: u64::default(),
        };
        if let Some(parent_index) = get_parent_index(self_index, fanout) {
            stakes.parent = nodes.get(parent_index).map(|n| n.stake).unwrap_or(0);
            stakes.parent_neighborhood = get_neighborhood_stake(parent_index, fanout, &nodes);
        }

        stakes
    }

    pub fn get_deterministic_shred_distribution_stakes_pct_by_slot_and_index(
        &self,
        slot: Slot,
        slot_stats: SlotStats,
        root_bank: &Bank,
        fanout: usize,
        leader_schedule_cache: &LeaderScheduleCache,
    ) -> f64 {
        if slot_stats.turbine_index_set.is_empty() || root_bank.total_epoch_stake() == 0 {
            warn!(
                "slot_stats.turbine_index_set.len()={} total_epoch_stake={}",
                slot_stats.turbine_index_set.len(),
                root_bank.total_epoch_stake()
            );
            return 0.0;
        }

        let indices: Vec<_> = slot_stats.turbine_index_set.iter().map(|idx| *idx).collect();
        let res: Vec<_> = PAR_THREAD_POOL.with(|thread_pool| {
            thread_pool.borrow().install(|| {
                indices
                    .into_par_iter()
                    .with_min_len(8)
                    .map(|index| {
                        let shred_stakes = self
                            .get_deterministic_shred_distribution_stakes_by_slot_and_index(
                                slot,
                                index,
                                root_bank,
                                fanout,
                                leader_schedule_cache,
                            );
                        shred_stakes.total()
                    })
                    .collect()
            })
        });

        let stakes: u64 = res.iter().sum();

        /*
        let mut stakes: u64 = 0;
        for index in slot_stats.turbine_index_set.iter() {
            let shred_stakes = self.get_deterministic_shred_distribution_stakes_by_slot_and_index(
                slot,
                *index,
                root_bank,
                fanout,
                leader_schedule_cache,
            );
            stakes += shred_stakes.total();
        }
        */
        stakes as f64 / slot_stats.num_shreds as f64 / root_bank.total_epoch_stake() as f64 * 100.0
    }

    pub fn get_shred_distribution_stakes(
        &self,
        shred: &Shred,
        root_bank: &Bank,
        fanout: usize,
        leader_schedule_cache: &LeaderScheduleCache,
    ) -> ShredDistributionStakes {
        if !enable_turbine_peers_shuffle_patch(shred.slot(), root_bank) {
            return ShredDistributionStakes::default();
        }

        let leader_pubkey = leader_schedule_cache
            .slot_leader_at(shred.slot(), Some(root_bank))
            .unwrap();

        if leader_pubkey == self.pubkey {
            // TODO?
        }

        let weighted_shuffle = self.weighted_shuffle.clone();
        let shred_seed = shred.seed(leader_pubkey, root_bank);
        let mut rng = ChaChaRng::from_seed(shred_seed);

        let nodes: Vec<_> = weighted_shuffle
            .shuffle(&mut rng)
            .map(|index| &self.nodes[index])
            .collect();
        let self_index = nodes
            .iter()
            .position(|node| node.pubkey() == self.pubkey)
            .unwrap();

        let mut stakes = ShredDistributionStakes {
            neighborhood: get_neighborhood_stake(self_index, fanout, &nodes),
            parent: u64::default(),
            parent_neighborhood: u64::default(),
        };
        if let Some(parent_index) = get_parent_index(self_index, fanout) {
            stakes.parent = nodes.get(parent_index).map(|n| n.stake).unwrap_or(0);
            stakes.parent_neighborhood = get_neighborhood_stake(parent_index, fanout, &nodes);
        }

        stakes
    }

    /*
    pub fn get_shred_distribution_stakes_pct(
        &self,
        shreds: &[Shred],
        root_bank: &Bank,
        fanout: usize,
        leader_schedule_cache: &LeaderScheduleCache,
    ) -> f64 {
        if shreds.is_empty() || root_bank.total_epoch_stake() == 0 {
            debug_assert!(
                false,
                "shreds.len={} total_epoch_stake={}",
                shreds.len(),
                root_bank.total_epoch_stake()
            );
            return 0.0;
        }
        let mut stakes: u64 = 0;
        for shred in shreds {
            let shred_stakes =
                self.get_shred_distribution_stakes(shred, root_bank, fanout, leader_schedule_cache);
            stakes += shred_stakes.total();
        }
        stakes as f64 / shreds.len() as f64 / root_bank.total_epoch_stake() as f64 * 100.0
    }
    */
}

fn new_cluster_nodes<T: 'static>(
    cluster_info: &ClusterInfo,
    stakes: &HashMap<Pubkey, u64>,
) -> ClusterNodes<T> {
    let self_pubkey = cluster_info.id();
    let nodes = get_nodes(cluster_info, stakes);
    let index: HashMap<_, _> = nodes
        .iter()
        .enumerate()
        .map(|(ix, node)| (node.pubkey(), ix))
        .collect();
    let broadcast = TypeId::of::<T>() == TypeId::of::<BroadcastStage>();
    let stakes: Vec<u64> = nodes.iter().map(|node| node.stake).collect();
    let mut weighted_shuffle = WeightedShuffle::new(&stakes).unwrap();
    if broadcast {
        weighted_shuffle.remove_index(index[&self_pubkey]);
    }
    // For backward compatibility:
    //   * nodes which do not have contact-info are excluded.
    //   * stakes are floored at 1.
    // The sorting key here should be equivalent to
    // solana_gossip::deprecated::sorted_stakes_with_index.
    // Leader itself is excluded when sampling broadcast peers.
    let compat_index = nodes
        .iter()
        .enumerate()
        .filter(|(_, node)| node.contact_info().is_some())
        .filter(|(_, node)| !broadcast || node.pubkey() != self_pubkey)
        .sorted_by_key(|(_, node)| Reverse((node.stake.max(1), node.pubkey())))
        .map(|(index, node)| (node.stake.max(1), index))
        .collect();
    ClusterNodes {
        pubkey: self_pubkey,
        nodes,
        index,
        weighted_shuffle,
        compat_index,
        _phantom: PhantomData::default(),
    }
}

// All staked nodes + other known tvu-peers + the node itself;
// sorted by (stake, pubkey) in descending order.
fn get_nodes(cluster_info: &ClusterInfo, stakes: &HashMap<Pubkey, u64>) -> Vec<Node> {
    let self_pubkey = cluster_info.id();
    // The local node itself.
    std::iter::once({
        let stake = stakes.get(&self_pubkey).copied().unwrap_or_default();
        let node = NodeId::from(cluster_info.my_contact_info());
        Node { node, stake }
    })
    // All known tvu-peers from gossip.
    .chain(cluster_info.tvu_peers().into_iter().map(|node| {
        let stake = stakes.get(&node.id).copied().unwrap_or_default();
        let node = NodeId::from(node);
        Node { node, stake }
    }))
    // All staked nodes.
    .chain(
        stakes
            .iter()
            .filter(|(_, stake)| **stake > 0)
            .map(|(&pubkey, &stake)| Node {
                node: NodeId::from(pubkey),
                stake,
            }),
    )
    .sorted_by_key(|node| Reverse((node.stake, node.pubkey())))
    // Since sorted_by_key is stable, in case of duplicates, this
    // will keep nodes with contact-info.
    .dedup_by(|a, b| a.pubkey() == b.pubkey())
    .collect()
}

fn enable_turbine_peers_shuffle_patch(shred_slot: Slot, root_bank: &Bank) -> bool {
    let feature_slot = root_bank
        .feature_set
        .activated_slot(&feature_set::turbine_peers_shuffle::id());
    match feature_slot {
        None => false,
        Some(0) => true,
        Some(feature_slot) => {
            let epoch_schedule = root_bank.epoch_schedule();
            let feature_epoch = epoch_schedule.get_epoch(feature_slot);
            let shred_epoch = epoch_schedule.get_epoch(shred_slot);
            feature_epoch < shred_epoch
        }
    }
}

impl<T> ClusterNodesCache<T> {
    pub fn new(
        // Capacity of underlying LRU-cache in terms of number of epochs.
        cap: usize,
        // A time-to-live eviction policy is enforced to refresh entries in
        // case gossip contact-infos are updated.
        ttl: Duration,
    ) -> Self {
        Self {
            cache: Mutex::new(LruCache::new(cap)),
            ttl,
        }
    }
}

impl<T: 'static> ClusterNodesCache<T> {
    fn get_cache_entry(&self, epoch: Epoch) -> Arc<Mutex<CacheEntry<T>>> {
        let mut cache = self.cache.lock().unwrap();
        match cache.get(&epoch) {
            Some(entry) => Arc::clone(entry),
            None => {
                let entry = Arc::default();
                cache.put(epoch, Arc::clone(&entry));
                entry
            }
        }
    }

    pub(crate) fn get(
        &self,
        shred_slot: Slot,
        root_bank: &Bank,
        working_bank: &Bank,
        cluster_info: &ClusterInfo,
    ) -> Arc<ClusterNodes<T>> {
        let epoch = root_bank.get_leader_schedule_epoch(shred_slot);
        let entry = self.get_cache_entry(epoch);
        // Hold the lock on the entry here so that, if needed, only
        // one thread recomputes cluster-nodes for this epoch.
        let mut entry = entry.lock().unwrap();
        if let Some((asof, nodes)) = entry.deref() {
            if asof.elapsed() < self.ttl {
                return Arc::clone(nodes);
            }
        }
        let epoch_staked_nodes = [root_bank, working_bank]
            .iter()
            .find_map(|bank| bank.epoch_staked_nodes(epoch));
        if epoch_staked_nodes.is_none() {
            inc_new_counter_info!("cluster_nodes-unknown_epoch_staked_nodes", 1);
            if epoch != root_bank.get_leader_schedule_epoch(root_bank.slot()) {
                return self.get(root_bank.slot(), root_bank, working_bank, cluster_info);
            }
            inc_new_counter_info!("cluster_nodes-unknown_epoch_staked_nodes_root", 1);
        }
        let nodes = Arc::new(new_cluster_nodes::<T>(
            cluster_info,
            &epoch_staked_nodes.unwrap_or_default(),
        ));
        *entry = Some((Instant::now(), Arc::clone(&nodes)));
        nodes
    }
}

impl From<ContactInfo> for NodeId {
    fn from(node: ContactInfo) -> Self {
        NodeId::ContactInfo(node)
    }
}

impl From<Pubkey> for NodeId {
    fn from(pubkey: Pubkey) -> Self {
        NodeId::Pubkey(pubkey)
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        rand::{seq::SliceRandom, Rng},
        solana_entry::entry::Entry,
        solana_gossip::{
            crds::GossipRoute,
            crds_value::{CrdsData, CrdsValue},
            deprecated::{
                shuffle_peers_and_index, sorted_retransmit_peers_and_stakes,
                sorted_stakes_with_index,
            },
        },
        solana_ledger::{
            genesis_utils::{
                bootstrap_validator_stake_lamports, create_genesis_config_with_leader,
            },
            shred::Shredder,
        },
        solana_sdk::{
            hash::Hash,
            signature::{Keypair, Signer},
            system_transaction,
            timing::timestamp,
        },
        solana_streamer::socket::SocketAddrSpace,
        std::{iter::repeat_with, sync::Arc},
    };

    // Legacy methods copied for testing backward compatibility.

    fn get_broadcast_peers(
        cluster_info: &ClusterInfo,
        stakes: Option<&HashMap<Pubkey, u64>>,
    ) -> (Vec<ContactInfo>, Vec<(u64, usize)>) {
        let mut peers = cluster_info.tvu_peers();
        let peers_and_stakes = stake_weight_peers(&mut peers, stakes);
        (peers, peers_and_stakes)
    }

    fn stake_weight_peers(
        peers: &mut Vec<ContactInfo>,
        stakes: Option<&HashMap<Pubkey, u64>>,
    ) -> Vec<(u64, usize)> {
        peers.dedup();
        sorted_stakes_with_index(peers, stakes)
    }

    fn make_cluster<R: Rng>(
        rng: &mut R,
    ) -> (
        Vec<ContactInfo>,
        HashMap<Pubkey, u64>, // stakes
        ClusterInfo,
    ) {
        let mut nodes: Vec<_> = repeat_with(|| ContactInfo::new_rand(rng, None))
            .take(1000)
            .collect();
        nodes.shuffle(rng);
        let this_node = nodes[0].clone();
        let mut stakes: HashMap<Pubkey, u64> = nodes
            .iter()
            .filter_map(|node| {
                if rng.gen_ratio(1, 7) {
                    None // No stake for some of the nodes.
                } else {
                    Some((node.id, rng.gen_range(0, 20)))
                }
            })
            .collect();
        // Add some staked nodes with no contact-info.
        stakes.extend(repeat_with(|| (Pubkey::new_unique(), rng.gen_range(0, 20))).take(100));
        let cluster_info = ClusterInfo::new(
            this_node,
            Arc::new(Keypair::new()),
            SocketAddrSpace::Unspecified,
        );
        {
            let now = timestamp();
            let mut gossip_crds = cluster_info.gossip.crds.write().unwrap();
            // First node is pushed to crds table by ClusterInfo constructor.
            for node in nodes.iter().skip(1) {
                let node = CrdsData::ContactInfo(node.clone());
                let node = CrdsValue::new_unsigned(node);
                assert_eq!(
                    gossip_crds.insert(node, now, GossipRoute::LocalMessage),
                    Ok(())
                );
            }
        }
        (nodes, stakes, cluster_info)
    }

    #[test]
    fn test_cluster_nodes_retransmit() {
        let mut rng = rand::thread_rng();
        let (nodes, stakes, cluster_info) = make_cluster(&mut rng);
        let this_node = cluster_info.my_contact_info();
        // ClusterInfo::tvu_peers excludes the node itself.
        assert_eq!(cluster_info.tvu_peers().len(), nodes.len() - 1);
        let cluster_nodes = new_cluster_nodes::<RetransmitStage>(&cluster_info, &stakes);
        // All nodes with contact-info should be in the index.
        assert_eq!(cluster_nodes.compat_index.len(), nodes.len());
        // Staked nodes with no contact-info should be included.
        assert!(cluster_nodes.nodes.len() > nodes.len());
        // Assert that all nodes keep their contact-info.
        // and, all staked nodes are also included.
        {
            let cluster_nodes: HashMap<_, _> = cluster_nodes
                .nodes
                .iter()
                .map(|node| (node.pubkey(), node))
                .collect();
            for node in &nodes {
                assert_eq!(cluster_nodes[&node.id].contact_info().unwrap().id, node.id);
            }
            for (pubkey, stake) in &stakes {
                if *stake > 0 {
                    assert_eq!(cluster_nodes[pubkey].stake, *stake);
                }
            }
        }
        let (peers, stakes_and_index) =
            sorted_retransmit_peers_and_stakes(&cluster_info, Some(&stakes));
        assert_eq!(stakes_and_index.len(), peers.len());
        assert_eq!(cluster_nodes.compat_index.len(), peers.len());
        for (i, node) in cluster_nodes
            .compat_index
            .iter()
            .map(|(_, i)| &cluster_nodes.nodes[*i])
            .enumerate()
        {
            let (stake, index) = stakes_and_index[i];
            // Wallclock may be update by ClusterInfo::push_self.
            if node.pubkey() == this_node.id {
                assert_eq!(this_node.id, peers[index].id)
            } else {
                assert_eq!(node.contact_info().unwrap(), &peers[index]);
            }
            assert_eq!(node.stake.max(1), stake);
        }
        let slot_leader = nodes[1..].choose(&mut rng).unwrap().id;
        // Remove slot leader from peers indices.
        let stakes_and_index: Vec<_> = stakes_and_index
            .into_iter()
            .filter(|(_stake, index)| peers[*index].id != slot_leader)
            .collect();
        assert_eq!(peers.len(), stakes_and_index.len() + 1);
        let mut shred_seed = [0u8; 32];
        rng.fill(&mut shred_seed[..]);
        let (self_index, shuffled_peers_and_stakes) =
            shuffle_peers_and_index(&this_node.id, &peers, &stakes_and_index, shred_seed);
        let shuffled_index: Vec<_> = shuffled_peers_and_stakes
            .into_iter()
            .map(|(_, index)| index)
            .collect();
        assert_eq!(this_node.id, peers[shuffled_index[self_index]].id);
        for fanout in 1..200 {
            let (neighbors_indices, children_indices) =
                compute_retransmit_peers(fanout, self_index, &shuffled_index);
            let (neighbors, children) =
                cluster_nodes.get_retransmit_peers_compat(shred_seed, fanout, slot_leader);
            assert_eq!(children.len(), children_indices.len());
            for (node, index) in children.into_iter().zip(children_indices) {
                assert_eq!(*node.contact_info().unwrap(), peers[index]);
            }
            assert_eq!(neighbors.len(), neighbors_indices.len());
            assert_eq!(neighbors[0].pubkey(), peers[neighbors_indices[0]].id);
            for (node, index) in neighbors.into_iter().zip(neighbors_indices).skip(1) {
                assert_eq!(*node.contact_info().unwrap(), peers[index]);
            }
        }
    }

    #[test]
    fn test_cluster_nodes_broadcast() {
        let mut rng = rand::thread_rng();
        let (nodes, stakes, cluster_info) = make_cluster(&mut rng);
        // ClusterInfo::tvu_peers excludes the node itself.
        assert_eq!(cluster_info.tvu_peers().len(), nodes.len() - 1);
        let cluster_nodes = ClusterNodes::<BroadcastStage>::new(&cluster_info, &stakes);
        // All nodes with contact-info should be in the index.
        // Excluding this node itself.
        assert_eq!(cluster_nodes.compat_index.len() + 1, nodes.len());
        // Staked nodes with no contact-info should be included.
        assert!(cluster_nodes.nodes.len() > nodes.len());
        // Assert that all nodes keep their contact-info.
        // and, all staked nodes are also included.
        {
            let cluster_nodes: HashMap<_, _> = cluster_nodes
                .nodes
                .iter()
                .map(|node| (node.pubkey(), node))
                .collect();
            for node in &nodes {
                assert_eq!(cluster_nodes[&node.id].contact_info().unwrap().id, node.id);
            }
            for (pubkey, stake) in &stakes {
                if *stake > 0 {
                    assert_eq!(cluster_nodes[pubkey].stake, *stake);
                }
            }
        }
        let (peers, peers_and_stakes) = get_broadcast_peers(&cluster_info, Some(&stakes));
        assert_eq!(peers_and_stakes.len(), peers.len());
        assert_eq!(cluster_nodes.compat_index.len(), peers.len());
        for (i, node) in cluster_nodes
            .compat_index
            .iter()
            .map(|(_, i)| &cluster_nodes.nodes[*i])
            .enumerate()
        {
            let (stake, index) = peers_and_stakes[i];
            assert_eq!(node.contact_info().unwrap(), &peers[index]);
            assert_eq!(node.stake.max(1), stake);
        }
        for _ in 0..100 {
            let mut shred_seed = [0u8; 32];
            rng.fill(&mut shred_seed[..]);
            let index = weighted_best(&peers_and_stakes, shred_seed);
            let peer = cluster_nodes.get_broadcast_peer(shred_seed).unwrap();
            assert_eq!(*peer, peers[index]);
        }
    }

    fn make_test_shreds() -> (Vec<Shred>, Vec<Shred>) {
        let keypair = Arc::new(Keypair::new());
        let slot = 1;
        let parent_slot = 0;
        let shredder = Shredder::new(slot, parent_slot, 0, 0).unwrap();
        let entries: Vec<_> = (0..5)
            .map(|_| {
                let keypair0 = Keypair::new();
                let keypair1 = Keypair::new();
                let tx0 =
                    system_transaction::transfer(&keypair0, &keypair1.pubkey(), 1, Hash::default());
                Entry::new(&Hash::default(), 1, vec![tx0])
            })
            .collect();

        shredder.entries_to_shreds(
            &keypair, &entries, true, // is_last_in_slot
            0,    // next_shred_index
            0,    // next_code_index
        )
    }

    #[test]
    fn test_get_shred_distribution_stakes() {
        let pubkey = solana_sdk::pubkey::new_rand();
        let mut genesis_config = create_genesis_config_with_leader(
            1_000_000,
            &pubkey,
            bootstrap_validator_stake_lamports(),
        )
        .genesis_config;
        genesis_config.epoch_schedule.warmup = false;

        let mut bank = Bank::new_for_tests(&genesis_config);
        bank.activate_feature(&feature_set::turbine_peers_shuffle::id());
        bank.activate_feature(&feature_set::deterministic_shred_seed_enabled::id());
        let bank = Arc::new(bank);

        let leader_schedule_cache = Arc::new(LeaderScheduleCache::new_from_bank(&bank));

        let mut rng = rand::thread_rng();
        let (_nodes, stakes, cluster_info) = make_cluster(&mut rng);

        let cluster_nodes = ClusterNodes::<RetransmitStage>::new(&cluster_info, &stakes);

        let (data_shreds, _coding_shreds) = make_test_shreds();

        const MAX_STAKE_PER_NODE: u64 = 19;

        let fanout = 10;
        for shred in &data_shreds {
            let shred_stakes = cluster_nodes.get_shred_distribution_stakes(
                shred,
                &bank,
                fanout,
                &leader_schedule_cache,
            );
            let max_stake_per_neighborhood = MAX_STAKE_PER_NODE * fanout as u64;
            assert!(shred_stakes.neighborhood <= max_stake_per_neighborhood);
            assert!(shred_stakes.parent_neighborhood <= max_stake_per_neighborhood);
        }

        let fanout = 200;
        for shred in &data_shreds {
            let shred_stakes = cluster_nodes.get_shred_distribution_stakes(
                shred,
                &bank,
                fanout,
                &leader_schedule_cache,
            );
            let max_stake_per_neighborhood = MAX_STAKE_PER_NODE * fanout as u64;
            assert!(shred_stakes.neighborhood <= max_stake_per_neighborhood);
            assert!(shred_stakes.parent_neighborhood <= max_stake_per_neighborhood);
        }
    }
}
