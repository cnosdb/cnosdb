use meta::model::MetaRef;
use models::meta_data::ReplicationSet;

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
    fn select(&self, mut replica_sets: Vec<ReplicationSet>, limit: isize) -> Vec<ReplicationSet> {
        if limit >= 0 {
            let is_same_host = |id| self.node_manager.node_id() == id;

            for replica_set in replica_sets.iter_mut() {
                replica_set
                    .vnodes
                    .sort_by_key(|k| if is_same_host(k.node_id) { 0 } else { i32::MAX });

                replica_set.vnodes.truncate(limit.try_into().unwrap());
            }
        }

        replica_sets
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use meta::model::meta_admin::AdminMeta;
    use models::meta_data::{ReplicationSet, VnodeInfo};

    use super::TopologyAwareReplicaSelectionPolicy;
    use crate::reader::replica_selection::ReplicaSelectionPolicy;

    #[test]
    fn test_topology_aware_replica_selection_policy() {
        let meta = Arc::new(AdminMeta::mock());
        let policy = TopologyAwareReplicaSelectionPolicy::new(meta);

        #[rustfmt::skip]
        let vnodes: Vec<ReplicationSet> = vec![
            ReplicationSet::new(1, 0, 0, vec![
                VnodeInfo::new(1, 0),
                VnodeInfo::new(2, 0),
                VnodeInfo::new(3, 1),
            ]),
            ReplicationSet::new(2, 0, 0, vec![
                VnodeInfo::new(4, 0),
                VnodeInfo::new(5, 1),
            ]),
            ReplicationSet::new(3, 0, 0, vec![
                VnodeInfo::new(6, 0)
            ]),
        ];

        let selected_vnodes = policy.select(vnodes, 2);

        #[rustfmt::skip]
        let expected_vnodes: Vec<ReplicationSet> = vec![
            ReplicationSet::new(1, 0, 0, vec![
                VnodeInfo::new(1, 0),
                VnodeInfo::new(2, 0),
            ]),
            ReplicationSet::new(2, 0, 0, vec![
                VnodeInfo::new(4, 0),
                VnodeInfo::new(5, 1),
            ]),
            ReplicationSet::new(3, 0, 0, vec![
                VnodeInfo::new(6, 0),
            ]),
        ];

        assert_eq!(selected_vnodes, expected_vnodes)
    }
}
