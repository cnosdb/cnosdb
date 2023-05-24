use meta::model::MetaRef;
use models::meta_data::VnodeInfo;

use crate::reader::replica_selection::ReplicaSelectionPolicy;

/// 基于拓扑感知的vnode副本选择策略
///
/// TODO 目前仅感知是否在同一节点，后续需要考虑机架、机房
pub struct TopologyAwareReplicaSelectionPolicy {
    // TODO 节点管理器
    node_manager: MetaRef,
}

impl TopologyAwareReplicaSelectionPolicy {
    pub fn new(node_manager: MetaRef) -> Self {
        Self { node_manager }
    }
}

impl ReplicaSelectionPolicy for TopologyAwareReplicaSelectionPolicy {
    fn select(&self, shards: Vec<Vec<VnodeInfo>>, limit: isize) -> Vec<Vec<VnodeInfo>> {
        if limit < 0 {
            return shards;
        }

        let is_same_host = |id| self.node_manager.node_id() == id;

        shards
            .into_iter()
            .map(|mut replicas| {
                replicas.sort_by_key(|k| if is_same_host(k.node_id) { 0 } else { i32::MAX });

                replicas
                    .into_iter()
                    .take(limit as usize)
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use meta::model::meta_client_mock::MockMetaManager;
    use models::meta_data::VnodeInfo;

    use super::TopologyAwareReplicaSelectionPolicy;
    use crate::reader::replica_selection::ReplicaSelectionPolicy;

    #[test]
    fn test_topology_aware_replica_selection_policy() {
        let meta = Arc::new(MockMetaManager::default());
        let policy = TopologyAwareReplicaSelectionPolicy::new(meta);

        #[rustfmt::skip]
        let vnodes: Vec<Vec<VnodeInfo>> = vec![
            vec![
                VnodeInfo::new(1, 0),
                VnodeInfo::new(2, 0),
                VnodeInfo::new(3, 1),
            ],
            vec![
                VnodeInfo::new(4, 0),
                VnodeInfo::new(5, 1),
            ],
            vec![
                VnodeInfo::new(6, 0)
            ],
        ];

        let selected_vnodes = policy.select(vnodes, 2);

        #[rustfmt::skip]
        let expected_vnodes: Vec<Vec<VnodeInfo>> = vec![
            vec![
                VnodeInfo::new(1, 0),
                VnodeInfo::new(2, 0),
            ],
            vec![
                VnodeInfo::new(4, 0),
                VnodeInfo::new(5, 1),
            ],
            vec![
                VnodeInfo::new(6, 0),
            ],
        ];

        assert_eq!(selected_vnodes, expected_vnodes)
    }
}
