use models::meta_data::{VnodeInfo, VnodeStatus};

use crate::reader::replica_selection::ReplicaSelectionPolicy;

/// 基于状态的vnode副本选择策略
pub struct StatusReplicaSelectionPolicy {}

impl StatusReplicaSelectionPolicy {
    pub fn new() -> Self {
        Self {}
    }
}

impl ReplicaSelectionPolicy for StatusReplicaSelectionPolicy {
    fn select(&self, shards: Vec<Vec<VnodeInfo>>, limit: isize) -> Vec<Vec<VnodeInfo>> {
        if limit < 0 {
            return shards;
        }

        shards
            .into_iter()
            .map(|mut replicas| {
                replicas.sort_by_key(|k| {
                    // The smaller the score, the easier it is to be selected
                    match k.status {
                        VnodeStatus::Running => 0,
                        VnodeStatus::Copying => 1,
                        VnodeStatus::Broken => i32::MAX,
                    }
                });

                replicas
                    .into_iter()
                    .filter(|e| e.status != VnodeStatus::Broken)
                    .take(limit as usize)
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>()
    }
}

#[cfg(test)]
mod tests {
    use models::meta_data::{VnodeInfo, VnodeStatus};

    use super::StatusReplicaSelectionPolicy;
    use crate::reader::replica_selection::ReplicaSelectionPolicy;

    #[test]
    fn test_topology_aware_replica_selection_policy() {
        let policy = StatusReplicaSelectionPolicy::new();

        #[rustfmt::skip]
        let vnodes: Vec<Vec<VnodeInfo>> = vec![
            vec![
                VnodeInfo { id: 0, node_id: 0, status: VnodeStatus::Running },
                VnodeInfo { id: 1, node_id: 0, status: VnodeStatus::Running },
                VnodeInfo { id: 2, node_id: 0, status: VnodeStatus::Broken },
            ],
            vec![
                VnodeInfo { id: 3, node_id: 0, status: VnodeStatus::Running },
                VnodeInfo { id: 4, node_id: 0, status: VnodeStatus::Broken },
            ],
            vec![
                VnodeInfo { id: 5, node_id: 0, status: VnodeStatus::Broken },
            ],
        ];

        let selected_vnodes = policy.select(vnodes, 2);

        #[rustfmt::skip]
        let expected_vnodes: Vec<Vec<VnodeInfo>> = vec![
            vec![
                VnodeInfo { id: 0, node_id: 0, status: VnodeStatus::Running },
                VnodeInfo { id: 1, node_id: 0, status: VnodeStatus::Running },
            ],
            vec![
                VnodeInfo { id: 3, node_id: 0, status: VnodeStatus::Running },
            ],
            vec![],
        ];

        assert_eq!(selected_vnodes, expected_vnodes)
    }
}
