use std::sync::Arc;
use std::time::Duration;

use meta::model::MetaRef;
use models::meta_data::*;
use models::schema::Precision;
use protos::kv_service::tskv_service_client::TskvServiceClient;
use protos::kv_service::WriteReplicaRequest;
use replication::raft_node::RaftNode;
use tonic::transport;
use tower::timeout::Timeout;
use trace::{debug, info, SpanContext, SpanRecorder};
use tskv::EngineRef;

use super::manager::RaftNodesManager;
use crate::errors::*;

pub struct RaftWriter {
    meta: MetaRef,
    config: config::Config,
    kv_inst: Option<EngineRef>,
    raft_manager: Arc<RaftNodesManager>,
}

impl RaftWriter {
    pub fn new(
        meta: MetaRef,
        config: config::Config,
        kv_inst: Option<EngineRef>,
        raft_manager: Arc<RaftNodesManager>,
    ) -> Self {
        Self {
            meta,
            config,
            kv_inst,
            raft_manager,
        }
    }

    pub async fn write_to_replica(
        &self,
        tenant: &str,
        db_name: &str,
        data: Arc<Vec<u8>>,
        precision: Precision,
        replica: &ReplicationSet,
        span_recorder: SpanRecorder,
    ) -> CoordinatorResult<()> {
        let node_id = self.config.node_basic.node_id;
        let leader_id = replica.leader_node_id();
        if leader_id == node_id && self.kv_inst.is_some() {
            let span_recorder = span_recorder.child("write to local node or forward");
            let result = self
                .write_to_local_or_forward(
                    data.clone(),
                    tenant,
                    db_name,
                    precision,
                    replica,
                    span_recorder.span_ctx(),
                )
                .await;

            info!("write to local {} {:?} {:?}", node_id, replica, result);

            result
        } else {
            let span_recorder = span_recorder.child("write to remote node");
            let request = WriteReplicaRequest {
                replica_id: replica.id,
                db_name: db_name.to_string(),
                data: Arc::unwrap_or_clone(data),
                precision: precision as u32,
                tenant: tenant.to_string(),
            };
            let result = self
                .write_to_remote(leader_id, request, span_recorder.span_ctx())
                .await;

            info!("write to remote {} {:?} {:?}", leader_id, replica, result);

            result
        }
    }

    async fn write_to_local_or_forward(
        &self,
        data: Arc<Vec<u8>>,
        tenant: &str,
        db_name: &str,
        precision: Precision,
        replica: &ReplicationSet,
        span_ctx: Option<&SpanContext>,
    ) -> CoordinatorResult<()> {
        let raft = self.raft_manager.get_node_or_build(tenant, replica).await?;

        let task = tskv::wal::writer::Task::new_write(
            tenant.to_string(),
            db_name.to_string(),
            777777777,
            precision,
            Arc::unwrap_or_clone(data.clone()),
        );

        let raft_data = bincode::serialize(&task).map_err(|err| CoordinatorError::CommonError {
            msg: err.to_string(),
        })?;
        let result = self.write_to_raft(raft, raft_data).await;
        println!("------ debugxxxx write_to_local_or_forward: {:?}", result);

        if let Err(CoordinatorError::ForwardToLeader {
            replica_id: _,
            leader_vnode_id,
        }) = result
        {
            let request = WriteReplicaRequest {
                replica_id: replica.id,
                db_name: db_name.to_string(),
                data: Arc::unwrap_or_clone(data),
                precision: precision as u32,
                tenant: tenant.to_string(),
            };

            self.process_leader_change(tenant, leader_vnode_id, request, span_ctx)
                .await
        } else {
            result
        }
    }

    async fn process_leader_change(
        &self,
        tenant: &str,
        leader_vnode_id: VnodeId,
        request: WriteReplicaRequest,
        span_ctx: Option<&SpanContext>,
    ) -> CoordinatorResult<()> {
        let vnode_id = leader_vnode_id;
        let all_info = crate::get_vnode_all_info(self.meta.clone(), tenant, vnode_id).await?;

        let rsp = self
            .meta
            .tenant_meta(tenant)
            .await
            .ok_or(CoordinatorError::TenantNotFound {
                name: tenant.to_owned(),
            })?
            .change_repl_set_leader(
                &all_info.db_name,
                all_info.bucket_id,
                request.replica_id,
                all_info.node_id,
                vnode_id,
            )
            .await;

        info!(
            "change replica({}) set leader to: {}; {:?}",
            request.replica_id, vnode_id, rsp
        );

        self.write_to_remote(all_info.node_id, request, span_ctx)
            .await
    }

    async fn write_to_remote(
        &self,
        leader_id: u64,
        request: WriteReplicaRequest,
        span_ctx: Option<&SpanContext>,
    ) -> CoordinatorResult<()> {
        let channel = self.meta.get_node_conn(leader_id).await?;
        let timeout = self.config.query.write_timeout_ms;
        let timeout_channel = Timeout::new(channel, Duration::from_millis(timeout));
        let mut client = TskvServiceClient::<Timeout<transport::Channel>>::new(timeout_channel);

        let mut cmd = tonic::Request::new(request);
        trace_http::ctx::append_trace_context(span_ctx, cmd.metadata_mut()).map_err(|_| {
            CoordinatorError::CommonError {
                msg: "Parse trace_id, this maybe a bug".to_string(),
            }
        })?;

        let begin_time = models::utils::now_timestamp_millis();
        let response = client
            .write_replica_points(cmd)
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
                    leader_vnode_id: (*leader_id) as u32,
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
