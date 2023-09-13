use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use meta::model::MetaRef;
use models::meta_data::*;
use models::schema::Precision;
use protos::kv_service::tskv_service_client::TskvServiceClient;
use protos::kv_service::*;
use replication::apply_store::ApplyStorageRef;
use replication::entry_store::EntryStorageRef;
use replication::multi_raft::MultiRaft;
use replication::node_store::NodeStorage;
use replication::raft_node::RaftNode;
use replication::state_store::StateStorage;
use replication::{RaftNodeId, RaftNodeInfo};
use tokio::sync::RwLock;
use tonic::transport;
use tower::timeout::Timeout;
use tracing::info;
use tskv::{wal, EngineRef};

use crate::errors::*;
use crate::{get_replica_all_info, update_replication_set};

pub struct RaftWriteRequest {
    pub points: WritePointsRequest,
    pub precision: Precision,
}

pub struct RaftNodesManager {
    meta: MetaRef,
    config: config::Config,
    kv_inst: Option<EngineRef>,
    raft_state: Arc<StateStorage>,
    raft_nodes: Arc<RwLock<MultiRaft>>,
}

impl RaftNodesManager {
    pub fn new(config: config::Config, meta: MetaRef, kv_inst: Option<EngineRef>) -> Self {
        let path = PathBuf::from(config.storage.path.clone()).join("raft-state");
        let state = StateStorage::open(path).unwrap();

        Self {
            meta,
            config,
            kv_inst,
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

    pub async fn metrics(&self, group_id: u32) -> String {
        if let Some(node) = self.raft_nodes.read().await.get_node(group_id) {
            serde_json::to_string(&node.raft_metrics())
                .unwrap_or("encode raft metrics to json failed".to_string())
        } else {
            format!("Not found raft group: {}", group_id)
        }
    }

    pub async fn get_node_or_build(
        &self,
        tenant: &str,
        db_name: &str,
        replica: &ReplicationSet,
    ) -> CoordinatorResult<Arc<RaftNode>> {
        if let Some(node) = self.raft_nodes.read().await.get_node(replica.id) {
            return Ok(node);
        }

        let result = self.build_replica_group(tenant, db_name, replica).await;
        if let Err(err) = &result {
            info!("build replica group failed: {:?}, {:?}", replica, err);
        } else {
            info!("build replica group success: {:?}", replica);
        }

        result
    }

    pub async fn exec_open_raft_node(
        &self,
        tenant: &str,
        db_name: &str,
        id: VnodeId,
        group_id: ReplicationSetId,
    ) -> CoordinatorResult<()> {
        info!("exec open raft node: {}.{}", group_id, id);
        let mut nodes = self.raft_nodes.write().await;
        if nodes.get_node(group_id).is_some() {
            return Ok(());
        }

        let node = self.open_raft_node(tenant, db_name, id, group_id).await?;
        nodes.add_node(node);

        Ok(())
    }

    pub async fn exec_drop_raft_node(
        &self,
        _tenant: &str,
        _db_name: &str,
        id: VnodeId,
        group_id: ReplicationSetId,
    ) -> CoordinatorResult<()> {
        info!("exec drop raft node: {}.{}", group_id, id);
        let mut nodes = self.raft_nodes.write().await;
        if let Some(raft_node) = nodes.get_node(group_id) {
            raft_node.shutdown().await?;
            nodes.rm_node(group_id);
            info!("success remove raft node({}) from group({})", id, group_id)
        } else {
            info!("can't found raft node({}) from group({})", id, group_id)
        }

        Ok(())
    }

    async fn build_replica_group(
        &self,
        tenant: &str,
        db_name: &str,
        replica: &ReplicationSet,
    ) -> CoordinatorResult<Arc<RaftNode>> {
        if replica.leader_node_id != self.node_id() {
            return Err(CoordinatorError::LeaderIsWrong {
                replica: replica.clone(),
            });
        }

        let mut nodes = self.raft_nodes.write().await;
        if let Some(node) = nodes.get_node(replica.id) {
            return Ok(node);
        }

        let mut cluster_nodes = BTreeMap::new();
        let leader_id = replica.leader_vnode_id;
        let raft_node = self
            .open_raft_node(tenant, db_name, leader_id, replica.id)
            .await?;
        for vnode in &replica.vnodes {
            let raft_id = vnode.id as RaftNodeId;
            let info = RaftNodeInfo {
                group_id: replica.id,
                address: self.meta.node_info_by_id(vnode.node_id).await?.grpc_addr,
            };
            cluster_nodes.insert(raft_id, info);

            if vnode.node_id == self.node_id() {
                continue;
            }

            self.open_remote_raft_node(tenant, db_name, vnode, replica.id)
                .await?;
            info!("success open remote raft: {}", vnode.node_id,);
        }

        info!("init raft group: {:?}", replica);
        raft_node.raft_init(cluster_nodes).await?;
        self.try_wait_leader_elected(raft_node.clone()).await;

        nodes.add_node(raft_node.clone());

        Ok(raft_node)
    }

    async fn try_wait_leader_elected(&self, raft_node: Arc<RaftNode>) {
        for _ in 0..10 {
            let result = raft_node.raw_raft().is_leader().await;
            info!("try wait leader elected, check leader: {:?}", result);
            if let Err(err) = result {
                if let Some(openraft::error::ForwardToLeader {
                    leader_id: Some(_id),
                    leader_node: Some(_node),
                }) = err.forward_to_leader()
                {
                    break;
                }
            } else {
                break;
            }

            tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
        }
    }

    pub async fn destory_replica_group(
        &self,
        tenant: &str,
        db_name: &str,
        replica_id: ReplicationSetId,
    ) -> CoordinatorResult<()> {
        let all_info = get_replica_all_info(self.meta.clone(), tenant, replica_id).await?;
        let replica = all_info.replica_set.clone();
        if replica.leader_node_id != self.node_id() {
            return Err(CoordinatorError::LeaderIsWrong {
                replica: replica.clone(),
            });
        }

        let raft_node = self.get_node_or_build(tenant, db_name, &replica).await?;
        let mut members = BTreeSet::new();
        members.insert(raft_node.raft_id());
        raft_node.raft_change_membership(members).await?;

        for vnode in replica.vnodes.iter() {
            if vnode.node_id == self.node_id() {
                continue;
            }

            let result = self
                .drop_remote_raft_node(tenant, db_name, vnode, replica.id)
                .await;
            info!("destory replica group drop vnode: {:?},{:?}", vnode, result);
        }

        raft_node.shutdown().await?;

        update_replication_set(
            self.meta.clone(),
            tenant,
            db_name,
            all_info.bucket_id,
            replica.id,
            &replica.vnodes,
            &[],
        )
        .await?;

        Ok(())
    }

    pub async fn add_follower_to_group(
        &self,
        tenant: &str,
        db_name: &str,
        follower_nid: NodeId,
        replica_id: ReplicationSetId,
    ) -> CoordinatorResult<()> {
        let follower_addr = self.meta.node_info_by_id(follower_nid).await?.grpc_addr;
        let all_info = get_replica_all_info(self.meta.clone(), tenant, replica_id).await?;
        let replica = all_info.replica_set.clone();
        if replica.leader_node_id != self.node_id() {
            return Err(CoordinatorError::LeaderIsWrong {
                replica: replica.clone(),
            });
        }

        let raft_node = self.get_node_or_build(tenant, db_name, &replica).await?;

        let new_vnode_id = self.meta.retain_id(1).await?;
        let new_vnode = VnodeInfo {
            id: new_vnode_id,
            node_id: follower_nid,
            status: VnodeStatus::Running,
        };
        self.open_remote_raft_node(tenant, db_name, &new_vnode, replica.id)
            .await?;

        let raft_node_info = RaftNodeInfo {
            group_id: replica_id,
            address: follower_addr,
        };
        raft_node
            .raft_add_learner(new_vnode_id.into(), raft_node_info)
            .await?;

        let mut members = BTreeSet::new();
        members.insert(new_vnode_id as RaftNodeId);
        for vnode in replica.vnodes.iter() {
            members.insert(vnode.id as RaftNodeId);
        }
        raft_node.raft_change_membership(members).await?;

        update_replication_set(
            self.meta.clone(),
            tenant,
            db_name,
            all_info.bucket_id,
            replica.id,
            &[],
            &[new_vnode],
        )
        .await?;

        Ok(())
    }

    pub async fn remove_node_from_group(
        &self,
        tenant: &str,
        db_name: &str,
        vnode_id: VnodeId,
        replica_id: ReplicationSetId,
    ) -> CoordinatorResult<()> {
        let all_info = get_replica_all_info(self.meta.clone(), tenant, replica_id).await?;
        let replica = all_info.replica_set.clone();
        if replica.leader_node_id != self.node_id() {
            return Err(CoordinatorError::LeaderIsWrong {
                replica: replica.clone(),
            });
        }

        let mut members = BTreeSet::new();
        for vnode in replica.vnodes.iter() {
            if vnode.id != vnode_id {
                members.insert(vnode.id as RaftNodeId);
            }
        }

        if let Some(vnode) = replica.vnode(vnode_id) {
            let raft_node = self.get_node_or_build(tenant, db_name, &replica).await?;
            raft_node.raft_change_membership(members).await?;

            if vnode.node_id == self.node_id() {
                self.exec_drop_raft_node(tenant, db_name, vnode.id, replica.id)
                    .await?;
            } else {
                self.drop_remote_raft_node(tenant, db_name, &vnode, replica.id)
                    .await?;
            }

            update_replication_set(
                self.meta.clone(),
                tenant,
                db_name,
                all_info.bucket_id,
                replica.id,
                &[vnode],
                &[],
            )
            .await?;
        }

        Ok(())
    }

    async fn open_raft_node(
        &self,
        tenant: &str,
        db_name: &str,
        vnode_id: VnodeId,
        group_id: ReplicationSetId,
    ) -> CoordinatorResult<Arc<RaftNode>> {
        info!("open local raft node: {}.{}", group_id, vnode_id);

        let entry = self.raft_node_logs(tenant, db_name, vnode_id).await?;
        let engine = self.raft_node_engine(tenant, db_name, vnode_id).await?;

        let grpc_addr = models::utils::build_address(
            self.config.host.clone(),
            self.config.cluster.grpc_listen_port,
        );
        let info = RaftNodeInfo {
            group_id,
            address: grpc_addr,
        };

        let raft_id = vnode_id as u64;
        let storage = NodeStorage::open(
            raft_id,
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

        let node = RaftNode::new(raft_id, info, config, storage, engine).await?;

        Ok(Arc::new(node))
    }

    async fn raft_node_logs(
        &self,
        tenant: &str,
        db_name: &str,
        vnode_id: VnodeId,
    ) -> CoordinatorResult<EntryStorageRef> {
        let owner = models::schema::make_owner(tenant, db_name);
        let wal_option = tskv::kv_option::WalOptions::from(&self.config);

        let wal = wal::VnodeWal::new(Arc::new(wal_option), Arc::new(owner), vnode_id).await?;
        let raft_logs = wal::raft::RaftEntryStorage::new(wal);
        Ok(Arc::new(raft_logs))
    }

    async fn raft_node_engine(
        &self,
        tenant: &str,
        db_name: &str,
        vnode_id: VnodeId,
    ) -> CoordinatorResult<ApplyStorageRef> {
        let kv_inst = self.kv_inst.clone();
        let storage = kv_inst.ok_or(CoordinatorError::KvInstanceNotFound {
            node_id: self.node_id(),
        })?;

        let engine = super::TskvEngineStorage::open(tenant, db_name, vnode_id, storage);
        let engine: ApplyStorageRef = Arc::new(engine);
        Ok(engine)
    }

    async fn drop_remote_raft_node(
        &self,
        tenant: &str,
        db_name: &str,
        vnode: &VnodeInfo,
        replica_id: ReplicationSetId,
    ) -> CoordinatorResult<()> {
        let channel = self.meta.get_node_conn(vnode.node_id).await?;
        let timeout_channel = Timeout::new(channel, Duration::from_secs(5));
        let mut client = TskvServiceClient::<Timeout<transport::Channel>>::new(timeout_channel);
        let cmd = tonic::Request::new(DropRaftNodeRequest {
            replica_id,
            vnode_id: vnode.id,
            db_name: db_name.to_string(),
            tenant: tenant.to_string(),
        });

        let response = client
            .exec_drop_raft_node(cmd)
            .await
            .map_err(|err| CoordinatorError::GRPCRequest {
                msg: err.to_string(),
            })?
            .into_inner();

        crate::status_response_to_result(&response)
    }

    async fn open_remote_raft_node(
        &self,
        tenant: &str,
        db_name: &str,
        vnode: &VnodeInfo,
        replica_id: ReplicationSetId,
    ) -> CoordinatorResult<()> {
        info!(
            "open remote raft node: {}.{}.{}",
            vnode.node_id, replica_id, vnode.id
        );

        let channel = self.meta.get_node_conn(vnode.node_id).await?;
        let timeout_channel = Timeout::new(channel, Duration::from_secs(5));
        let mut client = TskvServiceClient::<Timeout<transport::Channel>>::new(timeout_channel);
        let cmd = tonic::Request::new(OpenRaftNodeRequest {
            replica_id,
            vnode_id: vnode.id,
            db_name: db_name.to_string(),
            tenant: tenant.to_string(),
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
