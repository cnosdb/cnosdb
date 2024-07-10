use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

use openraft::storage::Adaptor;
use openraft::{OptionalSend, RaftMetrics};
use tracing::info;

use crate::errors::{RaftInternalErrSnafu, ReplicationResult};
use crate::network_client::NetworkConn;
use crate::node_store::NodeStorage;
use crate::{
    EngineMetrics, EntriesMetrics, OpenRaftNode, RaftNodeId, RaftNodeInfo, ReplicationConfig,
};

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct RaftNodeMetrics {
    pub raft: RaftMetrics<RaftNodeId, RaftNodeInfo>,
    pub engine: EngineMetrics,
    pub entries: EntriesMetrics,
}

#[derive(Clone)]
pub struct RaftNode {
    id: RaftNodeId,
    info: RaftNodeInfo,
    storage: Arc<NodeStorage>,

    raft: OpenRaftNode,
}

impl RaftNode {
    pub async fn new(
        id: RaftNodeId,
        info: RaftNodeInfo,
        storage: Arc<NodeStorage>,
        config: ReplicationConfig,
    ) -> ReplicationResult<Self> {
        let hb: u64 = config.heartbeat_interval;
        let keep_logs = config.raft_logs_to_keep;
        let raft_config = openraft::Config {
            enable_tick: true,
            enable_elect: true,
            enable_heartbeat: true,
            heartbeat_interval: hb,
            election_timeout_min: 3 * hb,
            election_timeout_max: 5 * hb,
            install_snapshot_timeout: config.install_snapshot_timeout,
            replication_lag_threshold: keep_logs,
            snapshot_policy: config.snapshot_policy.clone(),
            max_in_snapshot_log_to_keep: keep_logs,
            cluster_name: config.cluster_name.clone(),
            ..Default::default()
        };

        let raft_config = Arc::new(raft_config.validate().unwrap());
        let (log_store, state_machine) = Adaptor::new(storage.clone());

        let network = NetworkConn::new(config.clone());
        let raft = openraft::Raft::new(id, raft_config, network, log_store, state_machine)
            .await
            .map_err(|err| {
                RaftInternalErrSnafu {
                    msg: format!("New raft({}) execute failed: {}", id, err),
                }
                .build()
            })?;

        Ok(Self {
            id,
            info,
            storage,
            raft,
        })
    }

    pub fn raft_id(&self) -> RaftNodeId {
        self.id
    }

    pub fn group_id(&self) -> u32 {
        self.info.group_id
    }

    pub fn raw_raft(&self) -> OpenRaftNode {
        self.raft.clone()
    }

    /// Initialize a single-node cluster.
    pub async fn raft_init(
        &self,
        nodes: BTreeMap<RaftNodeId, RaftNodeInfo>,
    ) -> ReplicationResult<()> {
        let mut nodes = nodes.clone();
        nodes.insert(self.id, self.info.clone());

        let result = self.raft.initialize(nodes).await;
        info!(
            "Initialize raft group: {}, id: {}, {:?}",
            self.info.group_id, self.id, result
        );
        if let Err(openraft::error::RaftError::APIError(
            openraft::error::InitializeError::NotAllowed(_),
        )) = result
        {
            Ok(())
        } else if let Err(err) = result {
            Err(RaftInternalErrSnafu {
                msg: format!("Initialize raft group failed: {}", err),
            }
            .build())
        } else {
            Ok(())
        }
    }

    /// Add a node as **Learner**.
    pub async fn raft_add_learner(
        &self,
        id: RaftNodeId,
        info: RaftNodeInfo,
    ) -> ReplicationResult<()> {
        self.raft.add_learner(id, info, true).await.map_err(|err| {
            RaftInternalErrSnafu {
                msg: format!("Addlearner raft execute failed: {}", err),
            }
            .build()
        })?;

        Ok(())
    }

    /// Changes specified learners to members, or remove members.
    pub async fn raft_change_membership(
        &self,
        list: BTreeSet<RaftNodeId>,
        retain: bool,
    ) -> ReplicationResult<()> {
        self.raft
            .change_membership(list, retain)
            .await
            .map_err(|err| {
                RaftInternalErrSnafu {
                    msg: format!("Change membership raft execute failed: {}", err),
                }
                .build()
            })?;

        Ok(())
    }

    pub async fn shutdown(&self) -> ReplicationResult<()> {
        self.raft.shutdown().await.map_err(|err| {
            RaftInternalErrSnafu {
                msg: err.to_string(),
            }
            .build()
        })?;

        self.storage.destory().await?;

        Ok(())
    }

    pub async fn wait_condition<FUN>(
        &self,
        func: FUN,
        timeout: Duration,
        message: String,
    ) -> ReplicationResult<RaftMetrics<RaftNodeId, RaftNodeInfo>>
    where
        FUN: Fn(&RaftMetrics<RaftNodeId, RaftNodeInfo>) -> bool + OptionalSend,
    {
        let waiter = self.raft.wait(Some(timeout));

        waiter.metrics(func, &message).await.map_err(|err| {
            RaftInternalErrSnafu {
                msg: format!("wait condition: {}, error: {}", message, err),
            }
            .build()
        })
    }

    pub async fn metrics(&self) -> ReplicationResult<RaftNodeMetrics> {
        let engine_metrics = self.storage.engine_metrics().await?;
        let entries_metrics = self.storage.entries_metrics().await?;
        Ok(RaftNodeMetrics {
            raft: self.raft.metrics().borrow().clone(),
            engine: engine_metrics,
            entries: entries_metrics,
        })
    }

    /// Get the latest metrics of the cluster
    pub fn raft_metrics(&self) -> RaftMetrics<RaftNodeId, RaftNodeInfo> {
        self.raft.metrics().borrow().clone()
    }

    pub async fn engine_metrics(&self) -> ReplicationResult<EngineMetrics> {
        self.storage.engine_metrics().await
    }

    pub async fn sync_wal_writer(&self) {
        let _ = self.storage.sync_wal_writer().await;
    }
}
