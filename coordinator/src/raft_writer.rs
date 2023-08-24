use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use meta::model::MetaRef;
use models::meta_data::*;
use models::schema::Precision;
use protos::kv_service::tskv_service_client::TskvServiceClient;
use protos::kv_service::{Meta, WritePointsRequest, WriteVnodeRequest};
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
use trace::{debug, info, SpanContext, SpanRecorder};
use tskv::EngineRef;

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

    pub async fn raft_node_by_id(&self, id: ReplicationSetId) -> Option<Arc<RaftNode>> {
        self.raft_nodes.read().await.get_node(id).map(|v| v.clone())
    }

    pub async fn build_replica_group(&self, replica: &ReplicationSet) -> CoordinatorResult<()> {
        if replica.leader_node_id() != self.node_id {
            return Err(CoordinatorError::LeaderIsWrong {
                msg: format!(
                    "build replica group node_id: {}, replica:{:?}",
                    self.node_id, replica
                ),
            });
        }

        let mut nodes = self.raft_nodes.write().await;

        if nodes.exist(replica.id) {
            return Ok(());
        }

        let is_init = self.raft_state.is_already_init(replica.id)?;
        let leader_vid = replica.leader_vnode_id();
        let raft_node = self.open_raft_node(leader_vid, replica.id).await?;
        let raft_node = Arc::new(raft_node);
        if is_init {
            nodes.add_node(raft_node.clone());
            return Ok(());
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

            raft_node.raft_add_learner(raft_id, info).await?;
        }

        if followers.len() > 1 {
            raft_node.raft_change_membership(followers).await?;
        }

        nodes.add_node(raft_node.clone());

        Ok(())
    }

    pub async fn open_raft_node(
        &self,
        id: VnodeId,
        group_id: ReplicationSetId,
    ) -> CoordinatorResult<RaftNode> {
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

        let node = RaftNode::new(id, info, config, storage, engine)
            .await
            .unwrap();

        Ok(node)
    }
}

pub struct RaftWriter {
    node_id: u64,
    timeout_ms: u64,
    kv_inst: Option<EngineRef>,
    meta: MetaRef,
    raft_manager: Arc<RaftNodesManager>,
}

impl RaftWriter {
    pub async fn new(
        node_id: u64,
        timeout_ms: u64,
        kv_inst: Option<EngineRef>,
        meta: MetaRef,
    ) -> Self {
        let grpc_addr = meta.node_info_by_id(node_id).await.unwrap().grpc_addr;
        let raft_manager = RaftNodesManager::new(node_id, grpc_addr, meta.clone());
        Self {
            meta,
            node_id,
            kv_inst,
            timeout_ms,
            raft_manager: Arc::new(raft_manager),
        }
    }

    pub async fn write_to_replica(
        &self,
        tenant: &str,
        data: Arc<Vec<u8>>,
        precision: Precision,
        replica: &ReplicationSet,
        span_recorder: SpanRecorder,
    ) -> CoordinatorResult<()> {
        let leader_id = replica.leader_node_id();
        if leader_id == self.node_id && self.kv_inst.is_some() {
            let span_recorder = span_recorder.child("write to local node or forward");
            let result = self
                .write_to_local_or_forward(
                    data.clone(),
                    tenant,
                    precision,
                    replica.id,
                    span_recorder.span_ctx(),
                )
                .await;

            info!("write to local {} {:?} {:?}", self.node_id, replica, result);

            result
        } else {
            let span_recorder = span_recorder.child("write to remote node");
            let result = self
                .write_to_remote(
                    replica.id,
                    leader_id,
                    tenant,
                    data.clone(),
                    precision,
                    span_recorder.span_ctx(),
                )
                .await;

            info!("write to remote {} {:?} {:?}", leader_id, replica, result);

            result
        }
    }

    pub async fn write_to_local_or_forward(
        &self,
        data: Arc<Vec<u8>>,
        tenant: &str,
        precision: Precision,
        replica_id: ReplicationSetId,
        span_ctx: Option<&SpanContext>,
    ) -> CoordinatorResult<()> {
        let raft = self.raft_manager.raft_node_by_id(replica_id).await.ok_or(
            CoordinatorError::LeaderIsWrong {
                msg: format!("not found raft node for write: {}", replica_id),
            },
        )?;

        let request = WritePointsRequest {
            version: 1,
            meta: Some(Meta {
                tenant: tenant.to_string(),
                user: None,
                password: None,
            }),
            points: Arc::unwrap_or_clone(data.clone()),
        };

        let raft_data = protos::models_helper::to_prost_bytes(request);
        let result = self.write_to_raft(raft, raft_data).await;
        if let Err(CoordinatorError::ForwardToLeader {
            leader_id,
            replica_id,
        }) = result
        {
            self.write_to_remote(replica_id, leader_id, tenant, data, precision, span_ctx)
                .await
        } else {
            result
        }
    }

    async fn write_to_remote(
        &self,
        replica_id: u32,
        leader_id: u64,
        tenant: &str,
        data: Arc<Vec<u8>>,
        precision: Precision,
        span_ctx: Option<&SpanContext>,
    ) -> CoordinatorResult<()> {
        let channel = self.meta.get_node_conn(leader_id).await?;
        let timeout_channel = Timeout::new(channel, Duration::from_millis(self.timeout_ms));
        let mut client = TskvServiceClient::<Timeout<transport::Channel>>::new(timeout_channel);

        let mut cmd = tonic::Request::new(WriteVnodeRequest {
            id: replica_id,
            data: Arc::unwrap_or_clone(data),
            precision: precision as u32,
            tenant: tenant.to_string(),
        });

        trace_http::ctx::append_trace_context(span_ctx, cmd.metadata_mut()).map_err(|_| {
            CoordinatorError::CommonError {
                msg: "Parse trace_id, this maybe a bug".to_string(),
            }
        })?;

        let begin_time = models::utils::now_timestamp_millis();
        let response = client
            .write_vnode_points(cmd)
            .await
            .map_err(|err| CoordinatorError::TskvError { source: err.into() })?
            .into_inner();

        let use_time = models::utils::now_timestamp_millis() - begin_time;
        if use_time > 200 {
            debug!(
                "write points to node:{}, use time too long {}",
                leader_id, use_time
            )
        }

        crate::status_response_to_result(&response)
    }

    async fn write_to_raft(&self, raft: Arc<RaftNode>, data: Vec<u8>) -> CoordinatorResult<()> {
        if let Err(err) = raft.raw_raft().client_write(data).await {
            if let Some(openraft::error::ForwardToLeader {
                leader_id: Some(leader_id),
                leader_node: Some(leader_node),
            }) = err.forward_to_leader()
            {
                Err(CoordinatorError::ForwardToLeader {
                    leader_id: *leader_id,
                    replica_id: leader_node.group_id,
                })
            } else {
                Err(CoordinatorError::RaftWriteError {
                    msg: err.to_string(),
                })
            }
        } else {
            Ok(())
        }
    }
}
