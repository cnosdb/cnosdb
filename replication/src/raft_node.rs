use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use openraft::storage::Adaptor;
use openraft::RaftMetrics;
use tracing::info;

use crate::apply_store::ApplyStorageRef;
use crate::errors::{ReplicationError, ReplicationResult};
use crate::network_client::NetworkConn;
use crate::node_store::NodeStorage;
use crate::{OpenRaftNode, RaftNodeId, RaftNodeInfo};

#[derive(Clone)]
pub struct RaftNode {
    id: RaftNodeId,
    info: RaftNodeInfo,
    storage: Arc<NodeStorage>,
    engine: ApplyStorageRef,

    raft: OpenRaftNode,
    config: Arc<openraft::Config>,
}

impl RaftNode {
    pub async fn new(
        id: RaftNodeId,
        info: RaftNodeInfo,
        config: openraft::Config,
        storage: Arc<NodeStorage>,
        engine: ApplyStorageRef,
    ) -> ReplicationResult<Self> {
        let config = Arc::new(config.validate().unwrap());

        let (log_store, state_machine) = Adaptor::new(storage.clone());

        let network = NetworkConn::new();
        let raft = openraft::Raft::new(id, config.clone(), network, log_store, state_machine)
            .await
            .map_err(|err| ReplicationError::RaftInternalErr {
                msg: format!("New raft execute failed: {}", err),
            })?;

        Ok(Self {
            id,
            info,
            storage,
            raft,
            config,
            engine,
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
        info!("Initialize raft Status: {:?}", result);
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

        // self.raft
        //     .initialize(nodes)
        //     .await
        //     .map_err(|err| ReplicationError::RaftInternalErr {
        //         msg: format!("Initialize raft execute failed: {}", err),
        //     })?;

        // Ok(())
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

    pub fn apply_store(&self) -> ApplyStorageRef {
        self.engine.clone()
    }

    // pub async fn raft_vote(
    //     &self,
    //     vote: VoteRequest<RaftNodeId>,
    // ) -> ReplicationResult<VoteResponse<RaftNodeId>> {
    //     let rsp = self
    //         .raft
    //         .vote(vote)
    //         .await
    //         .map_err(|err| ReplicationError::RaftInternalErr {
    //             msg: format!("Vote raft execute failed: {}", err),
    //         })?;

    //     Ok(rsp)
    // }

    // pub async fn raft_append(
    //     &self,
    //     req: AppendEntriesRequest<TypeConfig>,
    // ) -> ReplicationResult<AppendEntriesResponse<RaftNodeId>> {
    //     let rsp = self.raft.append_entries(req).await.map_err(|err| {
    //         ReplicationError::RaftInternalErr {
    //             msg: format!("Append raft execute failed: {}", err),
    //         }
    //     })?;

    //     Ok(rsp)
    // }

    // pub async fn raft_snapshot(
    //     &self,
    //     req: InstallSnapshotRequest<TypeConfig>,
    // ) -> ReplicationResult<InstallSnapshotResponse<RaftNodeId>> {
    //     let rsp = self.raft.install_snapshot(req).await.map_err(|err| {
    //         ReplicationError::RaftInternalErr {
    //             msg: format!("Snapshot raft execute failed: {}", err),
    //         }
    //     })?;

    //     Ok(rsp)
    // }

    // pub async fn test_write_data(
    //     &self,
    //     req: Request,
    // ) -> Result<
    //     ClientWriteResponse<TypeConfig>,
    //     openraft::error::RaftError<u64, ClientWriteError<u64, RaftNodeInfo>>,
    // > {
    //     let response = self.raft.client_write(req).await;

    //     response
    // }
}
