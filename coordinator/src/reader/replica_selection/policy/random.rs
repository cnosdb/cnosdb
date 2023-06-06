use models::meta_data::VnodeInfo;
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
    fn select(&self, shards: Vec<Vec<VnodeInfo>>, limit: isize) -> Vec<Vec<VnodeInfo>> {
        if limit < 0 {
            return shards;
        }

        shards
            .into_iter()
            .map(|mut replicas| {
                replicas.shuffle(&mut rand::thread_rng());
                replicas
                    .into_iter()
                    .take(limit as usize)
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>()
    }
}
