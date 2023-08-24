use std::sync::Arc;
use std::time::Duration;

use meta::error::MetaError;
use meta::model::MetaRef;
use models::schema::Precision;
use models::utils::{now_timestamp_millis, now_timestamp_nanos};
use protos::kv_service::tskv_service_client::TskvServiceClient;
use protos::kv_service::{Meta, WritePointsRequest, WriteVnodeRequest};
use tokio::sync::mpsc::Sender;
use tokio::sync::oneshot;
use tonic::transport::Channel;
use tonic::Code;
use tower::timeout::Timeout;
use trace::{debug, info, SpanContext, SpanRecorder};
use trace_http::ctx::append_trace_context;
use tskv::EngineRef;

use crate::errors::*;
use crate::hh_queue::{HintedOffBlock, HintedOffWriteReq};
use crate::status_response_to_result;

#[derive(Debug)]
pub struct PointWriter {
    node_id: u64,
    timeout_ms: u64,
    kv_inst: Option<EngineRef>,
    meta_manager: MetaRef,
    hh_sender: Sender<HintedOffWriteReq>,
}

impl PointWriter {
    pub fn new(
        node_id: u64,
        timeout_ms: u64,
        kv_inst: Option<EngineRef>,
        meta_manager: MetaRef,
        hh_sender: Sender<HintedOffWriteReq>,
    ) -> Self {
        Self {
            node_id,
            kv_inst,
            timeout_ms,
            meta_manager,
            hh_sender,
        }
    }

    pub(crate) async fn write_to_node(
        &self,
        vnode_id: u32,
        tenant: &str,
        node_id: u64,
        precision: Precision,
        data: Arc<Vec<u8>>,
        span_recorder: SpanRecorder,
    ) -> CoordinatorResult<()> {
        if node_id == self.node_id && self.kv_inst.is_some() {
            let span_recorder = span_recorder.child("write to local node");

            let result = self
                .write_to_local_node(span_recorder.span_ctx(), vnode_id, tenant, precision, data)
                .await;
            debug!("write data to local {}({}) {:?}", node_id, vnode_id, result);

            return result;
        }

        let mut span_recorder = span_recorder.child("write to remote node");

        let result = self
            .write_to_remote_node(
                vnode_id,
                node_id,
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
                    node_id, vnode_id, err
                );

                span_recorder.error(err.to_string());

                return self
                    .write_to_handoff(
                        vnode_id,
                        node_id,
                        tenant,
                        precision,
                        Arc::unwrap_or_clone(data),
                    )
                    .await;
            }
        }

        debug!(
            "write data to remote {}({}) , inst exist: {}, {:?}!",
            node_id,
            vnode_id,
            self.kv_inst.is_some(),
            result
        );

        result
    }

    async fn write_to_handoff(
        &self,
        vnode_id: u32,
        node_id: u64,
        tenant: &str,
        precision: Precision,
        data: Vec<u8>,
    ) -> CoordinatorResult<()> {
        let (sender, receiver) = oneshot::channel();
        let block = HintedOffBlock::new(
            now_timestamp_nanos(),
            vnode_id,
            tenant.to_string(),
            precision,
            data,
        );
        let request = HintedOffWriteReq {
            node_id,
            sender,
            block,
        };

        self.hh_sender.send(request).await?;

        receiver.await?
    }

    pub async fn write_to_remote_node(
        &self,
        vnode_id: u32,
        node_id: u64,
        tenant: &str,
        precision: Precision,
        data: Arc<Vec<u8>>,
        span_ctx: Option<&SpanContext>,
    ) -> CoordinatorResult<()> {
        let channel = self
            .meta_manager
            .get_node_conn(node_id)
            .await
            .map_err(|error| CoordinatorError::FailoverNode {
                id: node_id,
                error: error.to_string(),
            })?;
        let timeout_channel = Timeout::new(channel, Duration::from_millis(self.timeout_ms));
        let mut client = TskvServiceClient::<Timeout<Channel>>::new(timeout_channel);

        let mut cmd = tonic::Request::new(WriteVnodeRequest {
            id: vnode_id,
            precision: precision as u32,
            tenant: tenant.to_string(),
            data: Arc::unwrap_or_clone(data),
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
                    id: node_id,
                    error: format!("{err:?}"),
                },
            })?
            .into_inner();

        let use_time = now_timestamp_millis() - begin_time;
        if use_time > 200 {
            debug!(
                "write points to node:{}, use time too long {}",
                node_id, use_time
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
        data: Arc<Vec<u8>>,
    ) -> CoordinatorResult<()> {
        let req = WritePointsRequest {
            version: 1,
            meta: Some(Meta {
                tenant: tenant.to_string(),
                user: None,
                password: None,
            }),
            points: Arc::unwrap_or_clone(data),
        };

        if let Some(kv_inst) = self.kv_inst.clone() {
            let _ = kv_inst.write(span_ctx, vnode_id, precision, req).await?;
            Ok(())
        } else {
            Err(CoordinatorError::KvInstanceNotFound { node_id: 0 })
        }
    }
}
