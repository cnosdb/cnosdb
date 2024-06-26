use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use models::meta_data::ReplicationSetId;
use tokio::sync::RwLock;
use tokio::time::interval_at;
use trace::info;

use crate::errors::{ReplicationError, ReplicationResult};
use crate::metrics::ReplicationMetrics;
use crate::raft_node::RaftNode;

#[derive(Clone, PartialEq)]
enum Status {
    Running,
    Shutdown(Instant),
}

#[derive(Clone)]
struct RaftNodeWrapper {
    stat: Status,
    raft: Arc<RaftNode>,
    metrics: ReplicationMetrics,
}

pub struct MultiRaft {
    nodes: HashMap<ReplicationSetId, RaftNodeWrapper>,
}

impl Default for MultiRaft {
    fn default() -> Self {
        Self::new()
    }
}

impl MultiRaft {
    pub fn new() -> Self {
        Self {
            nodes: HashMap::new(),
        }
    }

    pub fn add_node(&mut self, node: Arc<RaftNode>, metrics: ReplicationMetrics) {
        let id = node.group_id();
        if let Some(item) = self.nodes.get(&id) {
            item.metrics.drop();
        }

        let wrapper = RaftNodeWrapper {
            stat: Status::Running,
            raft: node,
            metrics,
        };
        self.nodes.insert(id, wrapper);
    }

    pub async fn shutdown(&mut self, id: ReplicationSetId) -> ReplicationResult<()> {
        if let Some(item) = self.nodes.get_mut(&id) {
            if item.stat == Status::Running {
                item.raft.shutdown().await?;
                item.stat = Status::Shutdown(Instant::now());
            }

            item.metrics.drop();
        }

        Ok(())
    }

    pub fn get_node(&self, id: ReplicationSetId) -> ReplicationResult<Option<Arc<RaftNode>>> {
        if let Some(item) = self.nodes.get(&id) {
            if item.stat == Status::Running {
                Ok(Some(item.raft.clone()))
            } else {
                Err(ReplicationError::AlreadyShutdown { id })
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

        let update_metrics_interval = Duration::from_secs(10);
        let start = Instant::now() + update_metrics_interval;
        let mut update_metrics_ticker = interval_at(start.into(), update_metrics_interval);

        loop {
            tokio::select! {
                _= clear_shutdown_ticker.tick() => {MultiRaft::clear_shutdown_nodes(nodes.clone()).await;}
                _= trigger_snapshot_ticker.tick() => {MultiRaft::trigger_snapshot_purge_logs(nodes.clone()).await;}
                _=update_metrics_ticker.tick() =>{MultiRaft::update_metrics_values(nodes.clone()).await;}
            }
        }
    }

    async fn trigger_snapshot_purge_logs(nodes: Arc<RwLock<MultiRaft>>) {
        let nodes = nodes.read().await;
        for (_, item) in nodes.nodes.iter() {
            if let Status::Shutdown(_) = item.stat {
                continue;
            }

            let engine_metrics = match item.raft.engine_metrics().await {
                Ok(metrics) => metrics,
                Err(err) => {
                    info!("get engine metrics failed: {:?}", err);
                    continue;
                }
            };

            info!(
                "# Engine Metrics group id: {} raft id: {}; {:?}",
                item.raft.group_id(),
                item.raft.raft_id(),
                engine_metrics
            );

            if engine_metrics.flushed_apply_id <= engine_metrics.snapshot_apply_id {
                continue;
            }

            let raft = item.raft.raw_raft();
            let trigger = raft.trigger();
            let _ = trigger.snapshot().await;
        }
    }

    async fn update_metrics_values(nodes: Arc<RwLock<MultiRaft>>) {
        let mut nodes = nodes.write().await;
        for (_, item) in nodes.nodes.iter_mut() {
            if let Ok(metrics) = item.raft.metrics().await {
                item.metrics.update_values(metrics);
            }
        }
    }

    async fn clear_shutdown_nodes(nodes: Arc<RwLock<MultiRaft>>) {
        let mut nodes = nodes.write().await;
        nodes
            .nodes
            .retain(|_id, item| MultiRaft::can_retain(item.raft.clone(), item.stat.clone()));
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
