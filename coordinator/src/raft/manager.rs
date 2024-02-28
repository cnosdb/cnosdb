use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use meta::model::MetaRef;
use models::meta_data::*;
use protos::kv_service::*;
use protos::{tskv_service_time_out_client, DEFAULT_GRPC_SERVER_MESSAGE_LEN};
use replication::multi_raft::MultiRaft;
use replication::node_store::NodeStorage;
use replication::raft_node::RaftNode;
use replication::state_store::{RaftNodeSummary, StateStorage};
use replication::{ApplyStorageRef, EntryStorageRef, RaftNodeId, RaftNodeInfo, ReplicationConfig};
use tokio::sync::RwLock;
use tracing::info;
use tskv::{wal, EngineRef};

use super::TskvEngineStorage;
use crate::errors::*;
use crate::{get_replica_all_info, update_replication_set};

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
        let state = StateStorage::open(path, config.cluster.lmdb_max_map_size).unwrap();

        Self {
            meta,
            config,
            kv_inst,
            raft_state: Arc::new(state),
            raft_nodes: Arc::new(RwLock::new(MultiRaft::new())),
        }
    }

    pub fn node_id(&self) -> u64 {
        self.config.global.node_id
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

    pub async fn start_all_raft_node(&self) -> CoordinatorResult<()> {
        let nodes_summary = self.raft_state.all_nodes_summary()?;
        let mut nodes = self.raft_nodes.write().await;
        for summary in nodes_summary {
            let node = self
                .open_raft_node(
                    &summary.tenant,
                    &summary.db_name,
                    summary.raft_id as VnodeId,
                    summary.group_id,
                )
                .await?;

            info!("start raft node: {:?} Success", summary);

            nodes.add_node(node);
        }

        Ok(())
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
        let group_id = raft_node.group_id();
        for _ in 0..10 {
            let result = raft_node.raw_raft().is_leader().await;
            info!("wait leader elected group: {}, {:?}", group_id, result);
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

        self.raft_nodes.write().await.rm_node(replica_id);

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
        info!("Opening local raft node: {group_id}.{vnode_id}");

        let (engine, raft_logs) = self
            .open_vnode_storage(tenant, db_name, vnode_id, group_id)
            .await?;

        let port = self.config.service.grpc_listen_port.unwrap_or(0);
        let grpc_addr = models::utils::build_address(&self.config.global.host, port);
        let info = RaftNodeInfo {
            group_id,
            address: grpc_addr,
        };

        let raft_id = vnode_id as u64;
        let storage = Arc::new(NodeStorage::open(
            raft_id,
            info.clone(),
            self.raft_state.clone(),
            engine.clone(),
            raft_logs,
        )?);

        let repl_config = self.replication_config();
        let node = RaftNode::new(raft_id, info, storage, repl_config).await?;

        let summary = RaftNodeSummary {
            raft_id,
            group_id,
            tenant: tenant.to_string(),
            db_name: db_name.to_string(),
        };

        self.raft_state.set_node_summary(group_id, &summary)?;

        Ok(Arc::new(node))
    }

    async fn open_vnode_storage(
        &self,
        tenant: &str,
        db_name: &str,
        vnode_id: VnodeId,
        group_id: ReplicationSetId,
    ) -> CoordinatorResult<(ApplyStorageRef, EntryStorageRef)> {
        // 1. open vnode store
        let kv_inst = self.kv_inst.clone();
        let storage = kv_inst.ok_or(CoordinatorError::KvInstanceNotFound {
            node_id: self.node_id(),
        })?;
        let mut vnode_store = storage.open_tsfamily(tenant, db_name, vnode_id).await?;

        // 2. open raft logs storage
        let owner = models::schema::make_owner(tenant, db_name);
        let wal_option = tskv::kv_option::WalOptions::from(&self.config);
        let wal = wal::VnodeWal::new(Arc::new(wal_option), Arc::new(owner), vnode_id).await?;
        let mut raft_logs = wal::raft_store::RaftEntryStorage::new(wal);

        // 3. recover data...
        let _apply_id = self.raft_state.get_last_applied_log(group_id)?;
        raft_logs.recover(&mut vnode_store).await?;

        // 4. open raft apply storage
        let engine = TskvEngineStorage::open(
            tenant,
            db_name,
            vnode_id,
            self.meta.clone(),
            vnode_store.clone(),
            storage,
            self.config.service.grpc_enable_gzip,
        );

        let engine = Arc::new(RwLock::new(engine));
        let raft_logs = Arc::new(RwLock::new(raft_logs));

        Ok((engine, raft_logs))
    }

    fn replication_config(&self) -> ReplicationConfig {
        ReplicationConfig {
            cluster_name: self.config.global.cluster_name.clone(),
            lmdb_max_map_size: self.config.cluster.lmdb_max_map_size,
            grpc_enable_gzip: self.config.service.grpc_enable_gzip,
            heartbeat_interval: self.config.cluster.heartbeat_interval,
            raft_logs_to_keep: self.config.cluster.raft_logs_to_keep,
            send_append_entries_timeout: self.config.cluster.send_append_entries_timeout,
            install_snapshot_timeout: self.config.cluster.install_snapshot_timeout,
        }
    }

    async fn drop_remote_raft_node(
        &self,
        tenant: &str,
        db_name: &str,
        vnode: &VnodeInfo,
        replica_id: ReplicationSetId,
    ) -> CoordinatorResult<()> {
        let channel = self.meta.get_node_conn(vnode.node_id).await?;
        let mut client = tskv_service_time_out_client(
            channel,
            Duration::from_secs(5),
            DEFAULT_GRPC_SERVER_MESSAGE_LEN,
            self.config.service.grpc_enable_gzip,
        );

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
        let mut client = tskv_service_time_out_client(
            channel,
            Duration::from_secs(5),
            DEFAULT_GRPC_SERVER_MESSAGE_LEN,
            self.config.service.grpc_enable_gzip,
        );
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
