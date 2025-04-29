use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use meta::model::MetaRef;
use metrics::metric_register::MetricsRegister;
use models::meta_data::*;
use models::schema::database_schema::make_owner;
use openraft::SnapshotPolicy;
use protos::kv_service::*;
use replication::metrics::ReplicationMetrics;
use replication::multi_raft::MultiRaft;
use replication::node_store::NodeStorage;
use replication::raft_node::RaftNode;
use replication::state_store::{RaftNodeSummary, StateStorage};
use replication::{ApplyStorageRef, EntryStorageRef, RaftNodeId, RaftNodeInfo, ReplicationConfig};
use snafu::ResultExt;
use tokio::runtime::Runtime;
use tokio::sync::RwLock;
use tracing::info;
use tskv::wal::wal_store::RaftEntryStorage;
use tskv::{wal, EngineRef};

use super::TskvEngineStorage;
use crate::errors::{
    CommonSnafu, CoordinatorError, CoordinatorResult, LeaderIsWrongSnafu, MetaSnafu,
    RaftNodeNotFoundSnafu, ReplicationSnafu, TskvSnafu,
};
use crate::tskv_executor::{TskvAdminRequest, TskvLeaderExecutor};
use crate::{get_replica_all_info, update_replication_set};

pub struct RaftNodesManager {
    meta: MetaRef,
    config: config::tskv::Config,
    kv_inst: Option<EngineRef>,
    raft_state: Arc<StateStorage>,
    raft_nodes: Arc<RwLock<MultiRaft>>,

    register: Arc<MetricsRegister>,
}

impl RaftNodesManager {
    pub fn new(
        config: config::tskv::Config,
        meta: MetaRef,
        kv_inst: Option<EngineRef>,
        register: Arc<MetricsRegister>,
    ) -> Self {
        let path = PathBuf::from(config.storage.path.clone()).join("raft-state");
        let state =
            StateStorage::open(path, config.cluster.lmdb_max_map_size.try_into().unwrap()).unwrap();

        Self {
            meta,
            config,
            kv_inst,
            register,
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
        if let Ok(Some(node)) = self.raft_nodes.read().await.get_node(group_id) {
            let res = node.metrics().await;
            if let Ok(res) = res {
                serde_json::to_string(&res)
                    .unwrap_or_else(|_| "encode  metrics to json failed".to_string())
            } else {
                res.unwrap_err().to_string()
            }
        } else {
            format!("Not found raft group: {}", group_id)
        }
    }

    pub async fn start_all_raft_node(
        runtime: Arc<Runtime>,
        manager: Arc<RaftNodesManager>,
    ) -> CoordinatorResult<()> {
        let nodes_summary = manager
            .raft_state
            .all_nodes_summary()
            .context(ReplicationSnafu)?;
        let mut nodes = manager.raft_nodes.write().await;
        let mut futures = Vec::with_capacity(nodes_summary.len());
        for summary in nodes_summary {
            let manager = manager.clone();
            let future = runtime.spawn(async move {
                match manager
                    .open_raft_node(
                        &summary.tenant,
                        &summary.db_name,
                        summary.raft_id as VnodeId,
                        summary.group_id,
                    )
                    .await
                {
                    Ok(node) => {
                        info!("start raft node: {:?} Success", summary);
                        Ok((node, summary))
                    }

                    Err(err) => {
                        info!("start raft node: {:?} Failed: {}", summary, err);
                        Err(err)
                    }
                }
            });
            futures.push(future);
        }

        for future in futures {
            let (node, summary) = future.await.map_err(|e| {
                CommonSnafu {
                    msg: format!("start all raft node failed: {:?}", e),
                }
                .build()
            })??;

            let metrics = ReplicationMetrics::new(
                manager.register.clone(),
                &summary.tenant,
                &summary.db_name,
                summary.group_id,
                summary.raft_id as RaftNodeId,
            );
            nodes.add_node(node, metrics);
        }
        Ok(())
    }

    pub async fn get_node_or_build(
        &self,
        tenant: &str,
        db_name: &str,
        replica: &ReplicationSet,
    ) -> CoordinatorResult<Arc<RaftNode>> {
        if let Some(node) = self
            .raft_nodes
            .read()
            .await
            .get_node(replica.id)
            .context(ReplicationSnafu)?
        {
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
        if let Ok(Some(node)) = nodes.get_node(group_id) {
            if node.raft_id() == id as u64 {
                return Ok(());
            } else {
                return Err(CoordinatorError::RaftGroupError {
                    msg: "raft node already exit".to_string(),
                });
            }
        }

        let node = self.open_raft_node(tenant, db_name, id, group_id).await?;

        let raft_id = id as RaftNodeId;
        let register = self.register.clone();
        let metrics = ReplicationMetrics::new(register, tenant, db_name, group_id, raft_id);
        nodes.add_node(node, metrics);

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
        self.raft_nodes
            .write()
            .await
            .shutdown(group_id)
            .await
            .context(ReplicationSnafu)?;

        Ok(())
    }

    pub async fn build_replica_group(
        &self,
        tenant: &str,
        db_name: &str,
        replica: &ReplicationSet,
    ) -> CoordinatorResult<Arc<RaftNode>> {
        if replica.leader_node_id != self.node_id() {
            return Err(LeaderIsWrongSnafu {
                replica: replica.clone(),
            }
            .build());
        }

        let mut nodes = self.raft_nodes.write().await;
        if let Some(node) = nodes.get_node(replica.id).context(ReplicationSnafu)? {
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
                address: self
                    .meta
                    .node_info_by_id(vnode.node_id)
                    .await
                    .context(MetaSnafu)?
                    .grpc_addr,
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
        raft_node
            .raft_init(cluster_nodes)
            .await
            .context(ReplicationSnafu)?;
        self.try_wait_leader_elected(raft_node.clone()).await;

        let register = self.register.clone();
        let metrics = ReplicationMetrics::new(
            register,
            tenant,
            db_name,
            raft_node.group_id(),
            raft_node.raft_id(),
        );
        nodes.add_node(raft_node.clone(), metrics);

        Ok(raft_node)
    }

    async fn try_wait_leader_elected(&self, raft_node: Arc<RaftNode>) {
        let group_id = raft_node.group_id();
        for _ in 0..10 {
            let result = self.assert_leader_node(raft_node.clone()).await;
            info!("wait leader elected group: {}, {:?}", group_id, result);
            if result.is_ok() {
                break;
            }

            if let Err(CoordinatorError::RaftForwardToLeader {
                replica_id: _,
                leader_vnode_id: _,
            }) = result
            {
                break;
            }

            tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
        }
    }

    async fn assert_leader_node(&self, raft_node: Arc<RaftNode>) -> CoordinatorResult<()> {
        let result = raft_node.raw_raft().ensure_linearizable().await;
        if let Err(err) = result {
            if let Some(openraft::error::ForwardToLeader {
                leader_id: Some(id),
                leader_node: Some(node),
            }) = err.forward_to_leader()
            {
                Err(CoordinatorError::RaftForwardToLeader {
                    replica_id: node.group_id,
                    leader_vnode_id: *id as u32,
                })
            } else {
                Err(CoordinatorError::RaftGroupError {
                    msg: format!("group-{}, is_leader failed: {}", raft_node.group_id(), err),
                })
            }
        } else {
            Ok(())
        }
    }

    pub async fn promote_follower_to_leader(
        &self,
        tenant: &str,
        db_name: &str,
        new_leader_id: VnodeId,
        replica_id: ReplicationSetId,
    ) -> CoordinatorResult<()> {
        let all_info = get_replica_all_info(self.meta.clone(), tenant, replica_id).await?;
        let replica = &all_info.replica_set;
        if replica.leader_vnode_id == new_leader_id {
            return Ok(());
        }

        if replica.vnode(new_leader_id).is_none() {
            return Err(RaftNodeNotFoundSnafu {
                vnode_id: new_leader_id,
                replica_id: replica.id,
            }
            .build());
        }

        let raft_node = self.get_node_or_build(tenant, db_name, replica).await?;
        self.assert_leader_node(raft_node.clone()).await?;

        let mut members = BTreeSet::new();
        members.insert(new_leader_id as RaftNodeId);
        raft_node
            .raft_change_membership(members, true)
            .await
            .context(ReplicationSnafu)?;

        for _ in 0..100 {
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
            if let Some(id) = raft_node.raw_raft().current_leader().await {
                if id == new_leader_id as RaftNodeId {
                    break;
                }
            }
        }

        let request = AdminCommand {
            tenant: tenant.to_string(),
            command: Some(admin_command::Command::LearnerToFollower(
                LearnerToFollowerRequest {
                    replica_id: replica.id,
                    db_name: db_name.to_string(),
                },
            )),
        };

        let caller = TskvAdminRequest {
            request,
            meta: self.meta.clone(),
            timeout: Duration::from_secs(3600),
            enable_gzip: self.config.service.grpc_enable_gzip,
        };
        let executor = TskvLeaderExecutor {
            meta: self.meta.clone(),
        };
        executor.do_request(tenant, replica, &caller).await?;
        Ok(())
    }

    pub async fn learner_to_follower(
        &self,
        tenant: &str,
        db_name: &str,
        replica_id: ReplicationSetId,
    ) -> CoordinatorResult<()> {
        let all_info = get_replica_all_info(self.meta.clone(), tenant, replica_id).await?;
        let replica = &all_info.replica_set;

        let raft_node = self.get_node_or_build(tenant, db_name, replica).await?;
        self.assert_leader_node(raft_node.clone()).await?;

        let mut members = BTreeSet::new();
        for vnode in replica.vnodes.iter() {
            members.insert(vnode.id as RaftNodeId);
        }
        raft_node
            .raft_change_membership(members, true)
            .await
            .context(ReplicationSnafu)?;

        Ok(())
    }

    pub async fn destroy_replica_group(
        &self,
        tenant: &str,
        db_name: &str,
        replica_id: ReplicationSetId,
    ) -> CoordinatorResult<()> {
        let all_info = get_replica_all_info(self.meta.clone(), tenant, replica_id).await?;
        let replica = all_info.replica_set.clone();

        let raft_node = self.get_node_or_build(tenant, db_name, &replica).await?;
        self.assert_leader_node(raft_node.clone()).await?;

        let mut members = BTreeSet::new();
        members.insert(raft_node.raft_id());
        raft_node
            .raft_change_membership(members, false)
            .await
            .context(ReplicationSnafu)?;

        for vnode in replica.vnodes.iter() {
            if vnode.node_id == self.node_id() {
                continue;
            }

            self.drop_remote_raft_node(tenant, db_name, vnode, replica.id)
                .await?;
        }

        self.raft_nodes
            .write()
            .await
            .shutdown(replica_id)
            .await
            .context(ReplicationSnafu)?;

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
        let follower_addr = self
            .meta
            .node_info_by_id(follower_nid)
            .await
            .context(MetaSnafu)?
            .grpc_addr;
        let all_info = get_replica_all_info(self.meta.clone(), tenant, replica_id).await?;
        let replica = all_info.replica_set.clone();

        let raft_node = self.get_node_or_build(tenant, db_name, &replica).await?;
        self.assert_leader_node(raft_node.clone()).await?;

        let new_vnode_id = self.meta.retain_id(1).await.context(MetaSnafu)?;
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
            .await
            .context(ReplicationSnafu)?;

        let mut members = BTreeSet::new();
        members.insert(new_vnode_id as RaftNodeId);
        for vnode in replica.vnodes.iter() {
            members.insert(vnode.id as RaftNodeId);
        }
        raft_node
            .raft_change_membership(members, false)
            .await
            .context(ReplicationSnafu)?;

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
        if let Some(vnode) = replica.vnode(vnode_id) {
            let raft_node = self.get_node_or_build(tenant, db_name, &replica).await?;
            self.assert_leader_node(raft_node.clone()).await?;

            let mut members = BTreeSet::new();
            for vnode in replica.vnodes.iter() {
                if vnode.id != vnode_id {
                    members.insert(vnode.id as RaftNodeId);
                }
            }
            raft_node
                .raft_change_membership(members, false)
                .await
                .context(ReplicationSnafu)?;

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
        let storage = NodeStorage::open(
            raft_id,
            info.clone(),
            self.raft_state.clone(),
            engine.clone(),
            raft_logs,
        )
        .await
        .context(ReplicationSnafu)?;
        let storage = Arc::new(storage);

        let repl_config = self.replication_config();
        let node = RaftNode::new(raft_id, info, storage, repl_config)
            .await
            .context(ReplicationSnafu)?;

        let summary = RaftNodeSummary {
            raft_id,
            group_id,
            tenant: tenant.to_string(),
            db_name: db_name.to_string(),
        };

        self.raft_state
            .set_node_summary(group_id, &summary)
            .context(ReplicationSnafu)?;

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
        let storage = kv_inst.ok_or_else(|| CoordinatorError::KvInstanceNotFound {
            node_id: self.node_id(),
        })?;
        let mut vnode_store = storage
            .open_tsfamily(tenant, db_name, vnode_id)
            .await
            .context(TskvSnafu)?;

        // 2. open raft logs storage
        let owner = make_owner(tenant, db_name);
        let wal_option = tskv::kv_option::WalOptions::from(&self.config);
        let wal = wal::VnodeWal::new(Arc::new(wal_option), Arc::new(owner), vnode_id)
            .await
            .context(TskvSnafu)?;
        let mut raft_logs = RaftEntryStorage::new(wal);

        // 3. recover data...
        let apply_id = self
            .raft_state
            .get_last_applied_log(group_id)
            .context(ReplicationSnafu)?;
        info!(
            "open vnode({}-{}) last applied id: {:?}",
            group_id, vnode_id, apply_id
        );
        raft_logs
            .recover(apply_id, &mut vnode_store)
            .await
            .context(TskvSnafu)?;

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
            // raft_logs_to_keep: 100,
            // snapshot_policy: SnapshotPolicy::LogsSinceLast(100),
            snapshot_policy: SnapshotPolicy::Never,
            raft_logs_to_keep: self.config.cluster.raft_logs_to_keep,
            cluster_name: self.config.global.cluster_name.clone(),
            lmdb_max_map_size: self.config.cluster.lmdb_max_map_size,
            grpc_enable_gzip: self.config.service.grpc_enable_gzip,
            heartbeat_interval: self.config.cluster.heartbeat_interval.as_millis() as u64,
            send_append_entries_timeout: self.config.cluster.send_append_entries_timeout.as_millis()
                as u64,
            install_snapshot_timeout: self.config.cluster.install_snapshot_timeout.as_millis()
                as u64,
        }
    }

    async fn drop_remote_raft_node(
        &self,
        tenant: &str,
        db_name: &str,
        vnode: &VnodeInfo,
        replica_id: ReplicationSetId,
    ) -> CoordinatorResult<()> {
        let request = AdminCommand {
            tenant: tenant.to_string(),
            command: Some(admin_command::Command::DropRaftNode(DropRaftNodeRequest {
                replica_id,
                vnode_id: vnode.id,
                db_name: db_name.to_string(),
                tenant: tenant.to_string(),
            })),
        };

        let caller = TskvAdminRequest {
            request,
            meta: self.meta.clone(),
            timeout: Duration::from_secs(60),
            enable_gzip: self.config.service.grpc_enable_gzip,
        };

        caller.do_request(vnode.node_id).await?;

        Ok(())
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

        let request = AdminCommand {
            tenant: tenant.to_string(),
            command: Some(admin_command::Command::OpenRaftNode(OpenRaftNodeRequest {
                replica_id,
                vnode_id: vnode.id,
                db_name: db_name.to_string(),
                tenant: tenant.to_string(),
            })),
        };

        let caller = TskvAdminRequest {
            request,
            meta: self.meta.clone(),
            timeout: Duration::from_secs(60),
            enable_gzip: self.config.service.grpc_enable_gzip,
        };

        caller.do_request(vnode.node_id).await?;

        Ok(())
    }

    pub async fn sync_wal_writer(&self) {
        let raft_nodes = self.raft_nodes.write().await;
        let _ = raft_nodes.sync_wal_writer().await;
    }
}
