use std::sync::Arc;
use std::time::Duration;

use config::QueryConfig;
use futures::TryStreamExt;
use meta::model::MetaRef;
use models::meta_data::VnodeInfo;
use protos::kv_service::tskv_service_client::TskvServiceClient;
use tokio::runtime::Runtime;
use tonic::transport::Channel;
use tower::timeout::Timeout;
use trace::{SpanContext, SpanExt, SpanRecorder};
use trace_http::ctx::append_trace_context;
use tskv::reader::status_listener::VnodeStatusListener;
use tskv::reader::table_scan::LocalTskvTableScanStream;
use tskv::reader::QueryOption;
use tskv::EngineRef;

use crate::errors::{CoordinatorError, CoordinatorResult};
use crate::reader::deserialize::TonicRecordBatchDecoder;
use crate::reader::{VnodeOpenFuture, VnodeOpener};
use crate::SendableCoordinatorRecordBatchStream;

/// for connect a vnode and reading to a stream of [`RecordBatch`]
pub struct TemporaryTableScanOpener {
    config: QueryConfig,
    kv_inst: Option<EngineRef>,
    runtime: Arc<Runtime>,
    meta: MetaRef,
    span_ctx: Option<SpanContext>,
}

impl TemporaryTableScanOpener {
    pub fn new(
        config: QueryConfig,
        kv_inst: Option<EngineRef>,
        runtime: Arc<Runtime>,
        meta: MetaRef,
        span_ctx: Option<&SpanContext>,
    ) -> Self {
        Self {
            config,
            kv_inst,
            runtime,
            meta,
            span_ctx: span_ctx.cloned(),
        }
    }
}

impl VnodeOpener for TemporaryTableScanOpener {
    fn open(&self, vnode: &VnodeInfo, option: &QueryOption) -> CoordinatorResult<VnodeOpenFuture> {
        let node_id = vnode.node_id;
        let vnode_id = vnode.id;
        let curren_nodet_id = self.meta.node_id();
        let kv_inst = self.kv_inst.clone();
        let runtime = self.runtime.clone();
        let option = option.clone();
        let meta = self.meta.clone();
        let config = self.config.clone();
        let span_ctx = self.span_ctx.clone();

        let future = async move {
            // TODO 请求路由的过程应该由通信框架决定，客户端只关心业务逻辑（请求目标和请求内容）
            if node_id == curren_nodet_id {
                // 路由到进程内的引擎
                let tenant = option.table_schema.tenant.clone();
                let kv_inst = kv_inst.ok_or(CoordinatorError::KvInstanceNotFound { node_id })?;
                let input = Box::pin(LocalTskvTableScanStream::new(
                    vnode_id,
                    option,
                    kv_inst,
                    runtime,
                    SpanRecorder::new(
                        span_ctx.child_span(format!("LocalTskvTableScanStream ({vnode_id})")),
                    ),
                ));

                let stream =
                    VnodeStatusListener::new(tenant, meta, vnode_id, input).map_err(Into::into);

                Ok(Box::pin(stream) as SendableCoordinatorRecordBatchStream)
            } else {
                // 路由到远程的引擎
                let mut request = {
                    let vnode_ids = vec![vnode_id];
                    let req = option
                        .to_query_record_batch_request(vnode_ids)
                        .map_err(CoordinatorError::from)?;
                    tonic::Request::new(req)
                };

                append_trace_context(span_ctx, request.metadata_mut()).map_err(|_| {
                    CoordinatorError::CommonError {
                        msg: "Parse trace_id, this maybe a bug".to_string(),
                    }
                })?;

                let resp_stream = {
                    let channel = meta
                        .get_node_conn(node_id)
                        .await
                        .map_err(|_| CoordinatorError::FailoverNode { id: node_id })?;
                    let timeout_channel =
                        Timeout::new(channel, Duration::from_millis(config.read_timeout_ms));
                    let mut client = TskvServiceClient::<Timeout<Channel>>::new(timeout_channel);
                    client
                        .query_record_batch(request)
                        .await
                        .map_err(|_| CoordinatorError::FailoverNode { id: node_id })?
                        .into_inner()
                };

                Ok(Box::pin(TonicRecordBatchDecoder::new(resp_stream))
                    as SendableCoordinatorRecordBatchStream)
            }
        };

        Ok(Box::pin(future))
    }
}
