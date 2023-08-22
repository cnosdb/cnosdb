use std::time::Duration;

use meta::error::MetaError;
use meta::model::MetaRef;
use models::meta_data::*;
use models::schema::Precision;
use models::utils::now_timestamp_millis;
use protos::kv_service::tskv_service_client::TskvServiceClient;
use protos::kv_service::{Meta, WritePointsRequest, WriteVnodeRequest};
use protos::models as fb_models;
use snafu::ResultExt;
use tonic::transport::Channel;
use tonic::Code;
use tower::timeout::Timeout;
use trace::{debug, info, SpanContext, SpanExt, SpanRecorder};
use trace_http::ctx::append_trace_context;
use tskv::EngineRef;

use crate::errors::*;
use crate::writer::VnodeMapping;
use crate::{status_response_to_result, WriteRequest};

#[derive(Debug)]
pub struct RaftWriter {
    node_id: u64,
    timeout_ms: u64,
    kv_inst: Option<EngineRef>,
    meta_manager: MetaRef,
}

impl RaftWriter {
    pub fn new(
        node_id: u64,
        timeout_ms: u64,
        kv_inst: Option<EngineRef>,
        meta_manager: MetaRef,
    ) -> Self {
        Self {
            node_id,
            kv_inst,
            timeout_ms,
            meta_manager,
        }
    }

    pub async fn write_points(
        &self,
        req: &WriteRequest,
        span_ctx: Option<&SpanContext>,
    ) -> CoordinatorResult<()> {
        let meta_client = self.meta_manager.tenant_meta(&req.tenant).await.ok_or(
            CoordinatorError::TenantNotFound {
                name: req.tenant.clone(),
            },
        )?;

        let mut mapping = VnodeMapping::new();
        {
            let _ = SpanRecorder::new(span_ctx.child_span("map point"));
            let fb_points = flatbuffers::root::<fb_models::Points>(&req.request.points)
                .context(InvalidFlatbufferSnafu)?;
            let db_name = fb_points.db_ext()?.to_string();
            for table in fb_points.tables_iter_ext()? {
                let schema = table.schema_ext()?;
                let tab_name = table.tab_ext()?.to_string();
                for item in table.points_iter_ext()? {
                    mapping
                        .map_point(
                            meta_client.clone(),
                            &db_name,
                            &tab_name,
                            req.precision,
                            schema,
                            item,
                        )
                        .await?;
                }
            }
        }

        let now = tokio::time::Instant::now();
        let mut requests = vec![];
        {
            let _ = SpanRecorder::new(span_ctx.child_span("build requests"));
            for (_id, points) in mapping.points.iter_mut() {
                points.finish()?;
                if points.repl_set.vnodes.is_empty() {
                    return Err(CoordinatorError::CommonError {
                        msg: "no available vnode in replication set".to_string(),
                    });
                }

                let span_recorder = SpanRecorder::new(
                    span_ctx.child_span(format!("write to raplica {}", points.repl_set.id)),
                );
                let request = self.write_to_replica(
                    points.data.clone(),
                    &req.tenant,
                    req.precision,
                    &points.repl_set,
                    span_recorder,
                );
                requests.push(request);
            }
        }

        for res in futures::future::join_all(requests).await {
            debug!(
                "Parallel write points on vnode over, start at: {:?}, elapsed: {} millis, result: {:?}",
                now,
                now.elapsed().as_millis(),
                res
            );
            res?
        }

        Ok(())
    }

    async fn write_to_replica(
        &self,
        data: Vec<u8>,
        tenant: &str,
        precision: Precision,
        replica: &ReplicationSet,
        span_recorder: SpanRecorder,
    ) -> CoordinatorResult<()> {
        let vnode_id = 0; // todo!()
        let leader_id = replica.leader_node_id();
        if leader_id == self.node_id && self.kv_inst.is_some() {
            let span_recorder = span_recorder.child("write to local node");

            let result = self
                .write_to_local_node(span_recorder.span_ctx(), vnode_id, tenant, precision, data)
                .await;
            debug!("write to local {}({}) {:?}", self.node_id, vnode_id, result);

            return result;
        }

        let mut span_recorder = span_recorder.child("write to remote node");

        let result = self
            .write_to_remote_node(
                vnode_id,
                leader_id,
                tenant,
                precision,
                data.clone(),
                span_recorder.span_ctx(),
            )
            .await;

        if let Err(ref err) = result {
            let meta_retry = MetaError::Retry {
                msg: "default".to_string(),
            };
            let tskv_memory = tskv::Error::MemoryExhausted;
            if matches!(*err, CoordinatorError::FailoverNode { .. })
                || err.error_code().to_string() == meta_retry.error_code().to_string()
                || err.error_code().to_string() == tskv_memory.error_code().to_string()
            {
                info!(
                    "write data to remote {}({}) failed {}; write to hh!",
                    leader_id, vnode_id, err
                );

                span_recorder.error(err.to_string());
            }
        }

        debug!(
            "write data to remote {}({})  {:?}!",
            leader_id, vnode_id, result
        );

        result
    }

    pub async fn write_to_remote_node(
        &self,
        vnode_id: u32,
        leader_id: u64,
        tenant: &str,
        precision: Precision,
        data: Vec<u8>,
        span_ctx: Option<&SpanContext>,
    ) -> CoordinatorResult<()> {
        let channel = self
            .meta_manager
            .get_node_conn(leader_id)
            .await
            .map_err(|error| CoordinatorError::FailoverNode {
                id: leader_id,
                error: error.to_string(),
            })?;
        let timeout_channel = Timeout::new(channel, Duration::from_millis(self.timeout_ms));
        let mut client = TskvServiceClient::<Timeout<Channel>>::new(timeout_channel);

        let mut cmd = tonic::Request::new(WriteVnodeRequest {
            vnode_id,
            precision: precision as u32,
            tenant: tenant.to_string(),
            data,
        });

        // 将当前的trace span信息写入到请求的metadata中
        append_trace_context(span_ctx, cmd.metadata_mut()).map_err(|_| {
            CoordinatorError::CommonError {
                msg: "Parse trace_id, this maybe a bug".to_string(),
            }
        })?;

        let begin_time = now_timestamp_millis();
        let response = client
            .write_vnode_points(cmd)
            .await
            .map_err(|err| match err.code() {
                Code::Internal => CoordinatorError::TskvError { source: err.into() },
                _ => CoordinatorError::FailoverNode {
                    id: leader_id,
                    error: format!("{err:?}"),
                },
            })?
            .into_inner();

        let use_time = now_timestamp_millis() - begin_time;
        if use_time > 200 {
            debug!(
                "write points to node:{}, use time too long {}",
                leader_id, use_time
            )
        }
        status_response_to_result(&response)
    }

    async fn write_to_local_node(
        &self,
        span_ctx: Option<&SpanContext>,
        vnode_id: u32,
        tenant: &str,
        precision: Precision,
        data: Vec<u8>,
    ) -> CoordinatorResult<()> {
        let req = WritePointsRequest {
            version: 1,
            meta: Some(Meta {
                tenant: tenant.to_string(),
                user: None,
                password: None,
            }),
            points: data.clone(),
        };

        if let Some(kv_inst) = self.kv_inst.clone() {
            let _ = kv_inst.write(span_ctx, vnode_id, precision, req).await?;
            Ok(())
        } else {
            Err(CoordinatorError::KvInstanceNotFound { node_id: 0 })
        }
    }
}
