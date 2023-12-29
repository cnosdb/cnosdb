use models::meta_data::ReplicationSet;
use rand::seq::SliceRandom;

use crate::reader::replica_selection::ReplicaSelectionPolicy;

/// 随机选择vnode副本
pub struct RandomReplicaSelectionPolicy {}

impl RandomReplicaSelectionPolicy {
    pub fn new() -> Self {
        Self {}
    }
}

impl ReplicaSelectionPolicy for RandomReplicaSelectionPolicy {
    fn select(&self, mut replica_sets: Vec<ReplicationSet>, limit: isize) -> Vec<ReplicationSet> {
        if limit >= 0 {
            for replica_set in replica_sets.iter_mut() {
                replica_set.vnodes.shuffle(&mut rand::thread_rng());
                replica_set.vnodes.truncate(limit.try_into().unwrap());
            }
        }

        replica_sets
    }
}
