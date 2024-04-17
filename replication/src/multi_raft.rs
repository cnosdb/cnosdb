use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use models::meta_data::ReplicationSetId;
use tokio::sync::RwLock;
use tokio::time::interval_at;
use trace::info;

use crate::errors::{ReplicationError, ReplicationResult};
use crate::raft_node::RaftNode;

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
        if let Some((node, Status::Running)) = self.raft_nodes.get(&id).cloned() {
            node.shutdown().await?;
            let status = Status::Shutdown(Instant::now());
            self.raft_nodes.insert(id, (node, status));
        }

        Ok(())
    }

    pub fn get_node(&self, id: ReplicationSetId) -> ReplicationResult<Option<Arc<RaftNode>>> {
        if let Some((node, status)) = self.raft_nodes.get(&id).cloned() {
            match status {
                Status::Running => Ok(Some(node)),
                Status::Shutdown(_) => Err(ReplicationError::AlreadyShutdown { id }),
            }
        } else {
            Ok(None)
        }
    }

    pub async fn raft_nodes_manager(
        nodes: Arc<RwLock<MultiRaft>>,
        trigger_snapshot_interval: Duration,
    ) {
        let start = Instant::now() + trigger_snapshot_interval;
        let mut trigger_snapshot_ticker = interval_at(start.into(), trigger_snapshot_interval);

        let clear_shutdown_interval = Duration::from_secs(2 * 60);
        let start = Instant::now() + clear_shutdown_interval;
        let mut clear_shutdown_ticker = interval_at(start.into(), clear_shutdown_interval);

        loop {
            tokio::select! {
                _= clear_shutdown_ticker.tick() => {MultiRaft::clear_shutdown_nodes(nodes.clone()).await;}
                _= trigger_snapshot_ticker.tick() => {MultiRaft::trigger_snapshot_purge_logs(nodes.clone()).await;}
            }
        }
    }

    async fn trigger_snapshot_purge_logs(nodes: Arc<RwLock<MultiRaft>>) {
        let nodes = nodes.read().await;
        for (_, (node, status)) in nodes.raft_nodes.iter() {
            if let Status::Shutdown(_) = status {
                continue;
            }

            let engine_metrics = match node.engine_metrics().await {
                Ok(metrics) => metrics,
                Err(err) => {
                    info!("get engine metrics failed: {:?}", err);
                    continue;
                }
            };

            info!(
                "# Engine Metrics group id: {} raft id: {}; {:?}",
                node.group_id(),
                node.raft_id(),
                engine_metrics
            );

            if engine_metrics.flushed_apply_id <= engine_metrics.snapshot_apply_id {
                continue;
            }

            let raft = node.raw_raft();
            let trigger = raft.trigger();
            let _ = trigger.snapshot().await;
        }
    }

    async fn clear_shutdown_nodes(nodes: Arc<RwLock<MultiRaft>>) {
        let mut nodes = nodes.write().await;
        nodes
            .raft_nodes
            .retain(|_id, (node, status)| MultiRaft::can_retain(node.clone(), status.clone()));
    }

    fn can_retain(node: Arc<RaftNode>, status: Status) -> bool {
        if let Status::Shutdown(inst) = status {
            if inst.elapsed() > Duration::from_secs(60) {
                info!(
                    "# Clear shutdown node group id: {} raft id: {}",
                    node.group_id(),
                    node.raft_id()
                );

                return false;
            }
        }
        true
    }
}

#[cfg(test)]
pub mod test {
    use std::time::Duration;

    #[tokio::test]
    async fn test_select() {
        let mut ticker1 = tokio::time::interval(Duration::from_secs(3));
        let mut ticker2 = tokio::time::interval(Duration::from_secs(5));

        for _ in 0..5 {
            tokio::select! {
                _= ticker1.tick() => {println!("------tick1");}
                _= ticker2.tick() => {println!("------tick2");}
            }

            println!("---- {:?}", std::time::Instant::now())
        }
    }
}
