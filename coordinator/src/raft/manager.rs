use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
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
    meta: MetaRef,
    config: config::Config,
    raft_state: Arc<StateStorage>,
    raft_nodes: Arc<RwLock<MultiRaft>>,
}

impl RaftNodesManager {
    pub fn new(config: config::Config, meta: MetaRef) -> Self {
        let path = PathBuf::from(config.storage.path.clone()).join("raft-state");
        let state = StateStorage::open(path).unwrap();

        Self {
            meta,
            config,
            raft_state: Arc::new(state),
            raft_nodes: Arc::new(RwLock::new(MultiRaft::new())),
        }
    }

    pub fn node_id(&self) -> u64 {
        self.config.node_basic.node_id
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

        let result = self.build_replica_group(replica).await;
        if let Err(err) = &result {
            info!("build replica group failed: {:?}, {:?}", replica, err);
        } else {
            info!("build replica group success: {:?}", replica);
        }

        result
    }

    pub async fn exec_open_raft_node(
        &self,
        id: VnodeId,
        group_id: ReplicationSetId,
    ) -> CoordinatorResult<Arc<RaftNode>> {
        info!("exec open raft node: {}.{}", group_id, id);
        let mut nodes = self.raft_nodes.write().await;
        if let Some(node) = nodes.get_node(group_id) {
            return Ok(node);
        }

        let node = self.open_raft_node(id, group_id).await?;
        self.raft_state.set_init_flag(group_id)?;
        nodes.add_node(node.clone());

        Ok(node)
    }

    async fn build_replica_group(
        &self,
        replica: &ReplicationSet,
    ) -> CoordinatorResult<Arc<RaftNode>> {
        let mut nodes = self.raft_nodes.write().await;
        if let Some(node) = nodes.get_node(replica.id) {
            return Ok(node);
        }

        let leader_id = replica.leader_vnode_id();
        let raft_node = self.open_raft_node(leader_id, replica.id).await?;
        if self.raft_state.is_already_init(replica.id)? {
            info!("raft group already init: {:?}", replica);
            nodes.add_node(raft_node.clone());
            return Ok(raft_node);
        }

        let mut cluster_nodes = BTreeMap::new();
        for vnode in &replica.vnodes {
            self.open_remote_raft_node(vnode.node_id, vnode.id, replica.id)
                .await?;
            info!("success open remote raft: {}", vnode.node_id,);

            let raft_id = vnode.id as RaftNodeId;
            let info = RaftNodeInfo {
                group_id: replica.id,
                address: self.meta.node_info_by_id(vnode.node_id).await?.grpc_addr,
            };
            cluster_nodes.insert(raft_id, info);
        }
        raft_node.raft_init(cluster_nodes).await?;
        self.raft_state.set_init_flag(replica.id)?;

        nodes.add_node(raft_node.clone());

        Ok(raft_node)
    }

    pub async fn add_follower_to_group(
        &self,
        tenant: &str,
        id: VnodeId,
        info: RaftNodeInfo,
    ) -> CoordinatorResult<()> {
        let group_id = info.group_id;
        let replica = self
            .meta
            .tenant_meta(tenant)
            .await
            .ok_or(CoordinatorError::TenantNotFound {
                name: tenant.to_owned(),
            })?
            .get_replication_set(group_id)
            .ok_or(CoordinatorError::ReplicationSetNotFound { id: group_id })?;

        let raft_node = self.get_node_or_build(&replica).await?;
        raft_node.raft_add_learner(id.into(), info).await?;

        let mut members = BTreeSet::new();
        members.insert(id as RaftNodeId);
        for vnode in replica.vnodes.iter() {
            members.insert(vnode.id as RaftNodeId);
        }
        raft_node.raft_change_membership(members).await?;

        Ok(())
    }

    pub async fn remove_node_from_group(
        &self,
        tenant: &str,
        id: VnodeId,
        group_id: ReplicationSetId,
    ) -> CoordinatorResult<()> {
        let replica = self
            .meta
            .tenant_meta(tenant)
            .await
            .ok_or(CoordinatorError::TenantNotFound {
                name: tenant.to_owned(),
            })?
            .get_replication_set(group_id)
            .ok_or(CoordinatorError::ReplicationSetNotFound { id: group_id })?;

        let raft_node = self.get_node_or_build(&replica).await?;
        let mut members = BTreeSet::new();
        for vnode in replica.vnodes.iter() {
            members.insert(vnode.id as RaftNodeId);
        }
        members.remove(&id.into());
        raft_node.raft_change_membership(members).await?;

        Ok(())
    }

    async fn open_raft_node(
        &self,
        id: VnodeId,
        group_id: ReplicationSetId,
    ) -> CoordinatorResult<Arc<RaftNode>> {
        info!("open local raft node: {}.{}", group_id, id);
        let id = id as u64;
        let path = format!("/tmp/cnosdb/{}/{}", self.node_id(), id);

        let entry = HeedEntryStorage::open(format!("{}-entry", path))?;
        let engine = HeedApplyStorage::open(format!("{}-engine", path))?;

        let entry: EntryStorageRef = Arc::new(entry);
        let engine: ApplyStorageRef = Arc::new(engine);

        let grpc_addr = models::utils::build_address(
            self.config.host.clone(),
            self.config.cluster.grpc_listen_port,
        );
        let info = RaftNodeInfo {
            group_id,
            address: grpc_addr,
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

        Ok(Arc::new(node))
    }

    async fn open_remote_raft_node(
        &self,
        node_id: NodeId,
        vnode_id: VnodeId,
        replica_id: ReplicationSetId,
    ) -> CoordinatorResult<()> {
        info!(
            "open remote raft node: {}.{}.{}",
            node_id, replica_id, vnode_id
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
