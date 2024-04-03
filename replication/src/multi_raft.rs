use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use models::meta_data::ReplicationSetId;
use tokio::sync::RwLock;
use trace::info;

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

    pub fn add_node(&mut self, node: Arc<RaftNode>) {
        self.raft_nodes.insert(node.group_id(), node);
    }

    pub fn rm_node(&mut self, id: ReplicationSetId) {
        self.raft_nodes.remove(&id);
    }

    pub fn get_node(&self, id: ReplicationSetId) -> Option<Arc<RaftNode>> {
        self.raft_nodes.get(&id).cloned()
    }

    pub fn exist(&self, id: ReplicationSetId) -> bool {
        self.raft_nodes.contains_key(&id)
    }

    pub async fn trigger_snapshot_purge_logs(nodes: Arc<RwLock<MultiRaft>>, dur: Duration) {
        loop {
            tokio::time::sleep(dur).await;

            info!("------------ Begin nodes trigger snapshot ------------");
            let nodes = nodes.read().await;
            for (_, node) in nodes.raft_nodes.iter() {
                let raft = node.raw_raft();
                let trigger = raft.trigger();

                trigger.snapshot().await;
                info!(
                    "# Trigger group id: {} raft id: {}",
                    node.group_id(),
                    node.raft_id()
                );
            }
            info!("------------- End nodes trigger snapshot -------------");
        }
    }
}
