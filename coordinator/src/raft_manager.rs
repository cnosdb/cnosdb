use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use meta::model::MetaRef;
use models::meta_data::*;
use models::schema::Precision;
use protos::kv_service::tskv_service_client::TskvServiceClient;
use protos::kv_service::{OpenRaftNodeRequest, WritePointsRequest};
use replication::apply_store::{ApplyStorageRef, HeedApplyStorage};
use replication::entry_store::{EntryStorageRef, HeedEntryStorage};
use replication::multi_raft::MultiRaft;
use replication::node_store::NodeStorage;
use replication::raft_node::RaftNode;
use replication::state_store::StateStorage;
use replication::{RaftNodeId, RaftNodeInfo};
use tokio::sync::RwLock;
use tonic::transport;
use tower::timeout::Timeout;
use tracing::info;

use crate::errors::*;

pub struct RaftWriteRequest {
    pub points: WritePointsRequest,
    pub precision: Precision,
}

pub struct RaftNodesManager {
    node_id: u64,
    meta: MetaRef,
    grpc_addr: String,
    raft_state: Arc<StateStorage>,
    raft_nodes: Arc<RwLock<MultiRaft>>,
}

impl RaftNodesManager {
    pub fn new(node_id: u64, grpc_addr: String, meta: MetaRef) -> Self {
        let state = StateStorage::open(format!("/tmp/cnosdb/{}-state", node_id)).unwrap();

        Self {
            meta,
            node_id,
            grpc_addr,
            raft_state: Arc::new(state),
            raft_nodes: Arc::new(RwLock::new(MultiRaft::new())),
        }
    }

    pub fn multi_raft(&self) -> Arc<RwLock<MultiRaft>> {
        self.raft_nodes.clone()
    }

    pub async fn get_node_or_build(
        &self,
        replica: &ReplicationSet,
    ) -> CoordinatorResult<Arc<RaftNode>> {
        if let Some(node) = self.raft_nodes.read().await.get_node(replica.id) {
            return Ok(node);
        }

        self.build_replica_group(replica).await
    }

    async fn build_replica_group(
        &self,
        replica: &ReplicationSet,
    ) -> CoordinatorResult<Arc<RaftNode>> {
        if replica.leader_node_id() != self.node_id {
            return Err(CoordinatorError::LeaderIsWrong {
                msg: format!(
                    "build replica group node_id: {}, replica:{:?}",
                    self.node_id, replica
                ),
            });
        }

        let mut nodes = self.raft_nodes.write().await;
        if let Some(node) = nodes.get_node(replica.id) {
            return Ok(node);
        }

        let is_init = self.raft_state.is_already_init(replica.id)?;
        let leader_vid = replica.leader_vnode_id();
        let raft_node = self.open_raft_node(leader_vid, replica.id).await?;
        if is_init {
            nodes.add_node(raft_node.clone());
            return Ok(raft_node);
        }
        raft_node.raft_init().await?;

        let mut followers = BTreeSet::new();
        for vnode in &replica.vnodes {
            let raft_id = vnode.id as RaftNodeId;
            followers.insert(raft_id);
            if vnode.id == leader_vid {
                continue;
            }

            let info = RaftNodeInfo {
                group_id: replica.id,
                address: self.meta.node_info_by_id(vnode.node_id).await?.grpc_addr,
            };

            self.open_remote_raft_node(vnode.node_id, vnode.id, replica.id)
                .await?;

            raft_node.raft_add_learner(raft_id, info).await?;
        }

        info!("build raft group: {}.{:?}", replica.id, followers);
        if followers.len() > 1 {
            raft_node.raft_change_membership(followers).await?;
        }

        nodes.add_node(raft_node.clone());

        Ok(raft_node)
    }

    pub async fn open_raft_node(
        &self,
        id: VnodeId,
        group_id: ReplicationSetId,
    ) -> CoordinatorResult<Arc<RaftNode>> {
        info!("open local raft node: {}.{}", id, group_id);
        let mut nodes = self.raft_nodes.write().await;
        if let Some(node) = nodes.get_node(group_id) {
            return Ok(node);
        }
        let id = id as u64;
        let path = format!("/tmp/cnosdb/{}/{}", self.node_id, id);

        let entry = HeedEntryStorage::open(format!("{}-entry", path))?;
        let engine = HeedApplyStorage::open(format!("{}-engine", path))?;

        let entry: EntryStorageRef = Arc::new(entry);
        let engine: ApplyStorageRef = Arc::new(engine);

        let info = RaftNodeInfo {
            group_id,
            address: self.grpc_addr.clone(),
        };

        let storage = NodeStorage::open(
            id,
            info.clone(),
            self.raft_state.clone(),
            engine.clone(),
            entry,
        )?;
        let storage = Arc::new(storage);

        let config = openraft::Config {
            heartbeat_interval: 500,
            election_timeout_min: 1500,
            election_timeout_max: 3000,
            ..Default::default()
        };

        let node = RaftNode::new(id, info, config, storage, engine).await?;
        let node = Arc::new(node);
        nodes.add_node(node.clone());

        Ok(node)
    }

    async fn open_remote_raft_node(
        &self,
        node_id: NodeId,
        vnode_id: VnodeId,
        replica_id: ReplicationSetId,
    ) -> CoordinatorResult<()> {
        info!(
            "open remote raft node: {}.{}.{}",
            node_id, vnode_id, replica_id
        );

        let channel = self.meta.get_node_conn(node_id).await?;
        let timeout_channel = Timeout::new(channel, Duration::from_millis(5 * 1000));
        let mut client = TskvServiceClient::<Timeout<transport::Channel>>::new(timeout_channel);
        let cmd = tonic::Request::new(OpenRaftNodeRequest {
            vnode_id,
            replica_id,
        });

        let response = client
            .exec_open_raft_node(cmd)
            .await
            .map_err(|err| CoordinatorError::GRPCRequest {
                msg: err.to_string(),
            })?
            .into_inner();

        crate::status_response_to_result(&response)
    }
}
