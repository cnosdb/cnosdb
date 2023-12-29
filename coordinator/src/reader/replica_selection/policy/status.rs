use models::meta_data::{ReplicationSet, VnodeStatus};

use crate::reader::replica_selection::ReplicaSelectionPolicy;

/// 基于状态的vnode副本选择策略
pub struct StatusReplicaSelectionPolicy {}

impl StatusReplicaSelectionPolicy {
    pub fn new() -> Self {
        Self {}
    }
}

impl ReplicaSelectionPolicy for StatusReplicaSelectionPolicy {
    fn select(&self, mut replica_sets: Vec<ReplicationSet>, limit: isize) -> Vec<ReplicationSet> {
        if limit >= 0 {
            for replica_set in replica_sets.iter_mut() {
                replica_set.vnodes.sort_by_key(|k| {
                    // The smaller the score, the easier it is to be selected
                    match k.status {
                        VnodeStatus::Running => 0,
                        VnodeStatus::Copying => 1,
                        VnodeStatus::Broken => i32::MAX,
                    }
                });

                replica_set
                    .vnodes
                    .retain(|e| e.status != VnodeStatus::Broken);

                replica_set.vnodes.truncate(limit.try_into().unwrap());
            }
        }

        replica_sets
    }
}

#[cfg(test)]
mod tests {
    use models::meta_data::{ReplicationSet, VnodeInfo, VnodeStatus};

    use super::StatusReplicaSelectionPolicy;
    use crate::reader::replica_selection::ReplicaSelectionPolicy;

    #[test]
    fn test_topology_aware_replica_selection_policy() {
        let policy = StatusReplicaSelectionPolicy::new();

        #[rustfmt::skip]
        let vnodes: Vec<ReplicationSet> = vec![
            ReplicationSet::new(1, 0, 0, vec![
                VnodeInfo { id: 0, node_id: 0, status: VnodeStatus::Running },
                VnodeInfo { id: 1, node_id: 0, status: VnodeStatus::Running },
                VnodeInfo { id: 2, node_id: 0, status: VnodeStatus::Broken },
            ]),
            ReplicationSet::new(2, 0, 0, vec![
                VnodeInfo { id: 3, node_id: 0, status: VnodeStatus::Running },
                VnodeInfo { id: 4, node_id: 0, status: VnodeStatus::Broken },
            ]),
            ReplicationSet::new(3, 0, 0, vec![
                VnodeInfo { id: 5, node_id: 0, status: VnodeStatus::Broken },
            ]),
        ];

        let selected_vnodes = policy.select(vnodes, 2);

        #[rustfmt::skip]
        let expected_vnodes: Vec<ReplicationSet> = vec![
            ReplicationSet::new(1, 0, 0, vec![
                VnodeInfo { id: 0, node_id: 0, status: VnodeStatus::Running },
                VnodeInfo { id: 1, node_id: 0, status: VnodeStatus::Running },
            ]),
            ReplicationSet::new(2, 0, 0, vec![
                VnodeInfo { id: 3, node_id: 0, status: VnodeStatus::Running },
            ]),
            ReplicationSet::new(3, 0, 0, vec![]),
        ];

        assert_eq!(selected_vnodes, expected_vnodes)
    }
}
