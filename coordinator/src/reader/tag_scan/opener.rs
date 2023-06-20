use std::time::Duration;

use config::QueryConfig;
use futures::TryStreamExt;
use meta::model::MetaRef;
use models::meta_data::VnodeInfo;
use protos::kv_service::tskv_service_client::TskvServiceClient;
use tonic::transport::Channel;
use tower::timeout::Timeout;
use trace::{SpanContext, SpanExt, SpanRecorder};
use trace_http::ctx::append_trace_context;
use tskv::reader::tag_scan::LocalTskvTagScanStream;
use tskv::reader::QueryOption;
use tskv::EngineRef;

use crate::errors::{CoordinatorError, CoordinatorResult};
use crate::reader::deserialize::TonicRecordBatchDecoder;
use crate::reader::{VnodeOpenFuture, VnodeOpener};
use crate::SendableCoordinatorRecordBatchStream;

pub struct TemporaryTagScanOpener {
    config: QueryConfig,
    kv_inst: Option<EngineRef>,
    meta: MetaRef,
    span_ctx: Option<SpanContext>,
}

impl TemporaryTagScanOpener {
    pub fn new(
        config: QueryConfig,
        kv_inst: Option<EngineRef>,
        meta: MetaRef,
        span_ctx: Option<&SpanContext>,
    ) -> Self {
        Self {
            config,
            kv_inst,
            meta,
            span_ctx: span_ctx.cloned(),
        }
    }
}

impl VnodeOpener for TemporaryTagScanOpener {
    fn open(&self, vnode: &VnodeInfo, option: &QueryOption) -> CoordinatorResult<VnodeOpenFuture> {
        let node_id = vnode.node_id;
        let vnode_id = vnode.id;
        let curren_nodet_id = self.meta.node_id();
        let kv_inst = self.kv_inst.clone();
        let option = option.clone();
        let admin_meta = self.meta.clone();
        let config = self.config.clone();
        let span_ctx = self.span_ctx.clone();

        let future = async move {
            // TODO 请求路由的过程应该由通信框架决定，客户端只关心业务逻辑（请求目标和请求内容）
            if node_id == curren_nodet_id {
                // 路由到进程内的引擎
                let kv_inst = kv_inst.ok_or(CoordinatorError::KvInstanceNotFound { node_id })?;
                // TODO U64Counter
                let stream = LocalTskvTagScanStream::new(
                    vnode_id,
                    option,
                    kv_inst,
                    SpanRecorder::new(
                        span_ctx.child_span(format!("LocalTskvTagScanStream ({vnode_id})")),
                    ),
                )
                .map_err(CoordinatorError::from);
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
                    let channel = admin_meta
                        .get_node_conn(node_id)
                        .await
                        .map_err(|_| CoordinatorError::FailoverNode { id: node_id })?;
                    let timeout_channel =
                        Timeout::new(channel, Duration::from_millis(config.read_timeout_ms));
                    let mut client = TskvServiceClient::<Timeout<Channel>>::new(timeout_channel);
                    client
                        .tag_scan(request)
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
