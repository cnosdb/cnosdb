use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use models::meta_data::ReplicationSetId;
use tokio::sync::RwLock;
use trace::info;

use crate::errors::{AlreadyShutdownSnafu, ReplicationError, ReplicationResult};
use crate::raft_node::RaftNode;
use crate::state_store::StateStorage;

#[derive(Clone)]
enum Status {
    Running,
    Shutdown(Instant),
}

pub struct MultiRaft {
    raft_nodes: HashMap<ReplicationSetId, (Arc<RaftNode>, Status)>,
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
        let id = node.group_id();
        let node = (node, Status::Running);

        self.raft_nodes.insert(id, node);
    }

    pub async fn shutdown(&mut self, id: ReplicationSetId) -> ReplicationResult<()> {
        if let Some((mut node, status)) = self.raft_nodes.get(&id).cloned() {
            if let Status::Running = status {
                node.shutdown().await?;
                let status = Status::Shutdown(Instant::now());
                self.raft_nodes.insert(id, (node, status));
            }
        }

        Ok(())
    }

    pub fn get_node(&self, id: ReplicationSetId) -> ReplicationResult<Option<Arc<RaftNode>>> {
        if let Some(node) = self.raft_nodes.get(&id).cloned() {
            match node.1 {
                Status::Running => Ok(Some(node.0)),
                Status::Shutdown(_) => Err(ReplicationError::AlreadyShutdown { id }),
            }
        } else {
            Ok(None)
        }
    }

    pub async fn trigger_snapshot_purge_logs(nodes: Arc<RwLock<MultiRaft>>, dur: Duration) {
        loop {
            tokio::time::sleep(dur).await;

            info!("------------ Begin nodes trigger snapshot ------------");
            let nodes = nodes.read().await;
            for (_, (node, status)) in nodes.raft_nodes.iter() {
                if let Status::Shutdown(_) = status {
                    continue;
                }

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

    fn can_retain(node: Arc<RaftNode>, status: Status) -> bool {
        if let Status::Shutdown(inst) = status {
            if inst.elapsed() > Duration::from_secs(60) {
                info!(
                    "# Clear node: group id: {} raft id: {}",
                    node.group_id(),
                    node.raft_id()
                );

                return false;
            }
        }
        true
    }

    pub async fn clear_shutdown_nodes(nodes: Arc<RwLock<MultiRaft>>) {
        loop {
            tokio::time::sleep(Duration::from_secs(60)).await;

            info!("------------ Begin clear shutdown nodes ------------");
            let mut nodes = nodes.write().await;
            nodes
                .raft_nodes
                .retain(|id, (node, status)| MultiRaft::can_retain(node.clone(), status.clone()));

            info!("------------- End clear shutdown nodes -------------");
        }
    }
}
