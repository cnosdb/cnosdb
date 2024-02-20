use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use openraft::storage::Adaptor;
use openraft::{RaftMetrics, SnapshotPolicy};
use tracing::info;

use crate::errors::{ReplicationError, ReplicationResult};
use crate::network_client::NetworkConn;
use crate::node_store::NodeStorage;
use crate::{ApplyStorageRef, OpenRaftNode, RaftNodeId, RaftNodeInfo, ReplicationConfig};

#[derive(Clone)]
pub struct RaftNode {
    id: RaftNodeId,
    info: RaftNodeInfo,
    storage: Arc<NodeStorage>,

    raft: OpenRaftNode,
    config: ReplicationConfig,
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
            snapshot_policy: SnapshotPolicy::LogsSinceLast(keep_logs),
            max_in_snapshot_log_to_keep: keep_logs,
            cluster_name: config.cluster_name.clone(),
            ..Default::default()
        };

        let raft_config = Arc::new(raft_config.validate().unwrap());
        let (log_store, state_machine) = Adaptor::new(storage.clone());

        let network = NetworkConn::new(config.clone());
        let raft = openraft::Raft::new(id, raft_config, network, log_store, state_machine)
            .await
            .map_err(|err| ReplicationError::RaftInternalErr {
                msg: format!("New raft({}) execute failed: {}", id, err),
            })?;

        Ok(Self {
            id,
            info,
            storage,
            raft,
            config,
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
            Err(ReplicationError::RaftInternalErr {
                msg: format!("Initialize raft group failed: {}", err),
            })
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
            ReplicationError::RaftInternalErr {
                msg: format!("Addlearner raft execute failed: {}", err),
            }
        })?;

        Ok(())
    }

    /// Changes specified learners to members, or remove members.
    pub async fn raft_change_membership(
        &self,
        list: BTreeSet<RaftNodeId>,
    ) -> ReplicationResult<()> {
        self.raft
            .change_membership(list, false)
            .await
            .map_err(|err| ReplicationError::RaftInternalErr {
                msg: format!("Change membership raft execute failed: {}", err),
            })?;

        Ok(())
    }

    pub async fn shutdown(&self) -> ReplicationResult<()> {
        self.raft
            .shutdown()
            .await
            .map_err(|err| ReplicationError::RaftInternalErr {
                msg: err.to_string(),
            })?;

        self.storage.destory().await?;

        Ok(())
    }

    /// Get the latest metrics of the cluster
    pub fn raft_metrics(&self) -> RaftMetrics<RaftNodeId, RaftNodeInfo> {
        self.raft.metrics().borrow().clone()
    }
}
