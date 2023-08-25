use std::collections::HashMap;
use std::sync::Arc;

use models::meta_data::ReplicationSetId;
use parking_lot::RwLock;

use crate::raft_node::RaftNode;
use crate::state_store::StateStorage;

pub struct MultiRaft {
    raft_nodes: HashMap<ReplicationSetId, Arc<RaftNode>>,
}

impl Default for MultiRaft {
    fn default() -> Self {
        Self::new()
    }
}

impl MultiRaft {
    pub fn new() -> Self {
        Self {
            raft_nodes: HashMap::new(),
        }
    }

    pub fn get_node(&self, id: ReplicationSetId) -> Option<Arc<RaftNode>> {
        self.raft_nodes.get(&id).cloned()
    }

    pub fn exist(&self, id: ReplicationSetId) -> bool {
        self.raft_nodes.contains_key(&id)
    }

    pub fn add_node(&mut self, node: Arc<RaftNode>) {
        self.raft_nodes.insert(node.group_id(), node);
    }
}
