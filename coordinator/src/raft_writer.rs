use std::sync::Arc;
use std::time::Duration;

use meta::model::MetaRef;
use models::meta_data::*;
use models::schema::Precision;
use protos::kv_service::tskv_service_client::TskvServiceClient;
use protos::kv_service::{Meta, WritePointsRequest, WriteVnodeRequest};
use replication::raft_node::RaftNode;
use tonic::transport;
use tower::timeout::Timeout;
use trace::{debug, info, SpanContext, SpanRecorder};
use tskv::EngineRef;

use crate::errors::*;
use crate::raft_manager::RaftNodesManager;

pub struct RaftWriter {
    meta: MetaRef,
    node_id: u64,
    timeout_ms: u64,
    kv_inst: Option<EngineRef>,
    raft_manager: Arc<RaftNodesManager>,
}

impl RaftWriter {
    pub fn new(
        node_id: u64,
        meta: MetaRef,
        timeout_ms: u64,
        kv_inst: Option<EngineRef>,
        raft_manager: Arc<RaftNodesManager>,
    ) -> Self {
        Self {
            meta,
            node_id,
            kv_inst,
            timeout_ms,
            raft_manager,
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

    async fn write_to_local_or_forward(
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
