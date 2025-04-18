use config::tskv::QueryConfig;
use futures::TryStreamExt;
use meta::model::MetaRef;
use models::meta_data::VnodeInfo;
use protos::{tskv_service_time_out_client, DEFAULT_GRPC_SERVER_MESSAGE_LEN};
use snafu::{IntoError, ResultExt};
use trace::http::http_ctx::grpc_append_trace_context;
use trace::span_ext::SpanExt;
use trace::{Span, SpanContext};
use tskv::reader::tag_scan::LocalTskvTagScanStream;
use tskv::reader::QueryOption;
use tskv::EngineRef;

use crate::errors::{CommonSnafu, CoordinatorError, CoordinatorResult, ModelSnafu, TskvSnafu};
use crate::reader::deserialize::TonicRecordBatchDecoder;
use crate::reader::{VnodeOpenFuture, VnodeOpener};
use crate::SendableCoordinatorRecordBatchStream;

pub struct TemporaryTagScanOpener {
    config: QueryConfig,
    kv_inst: Option<EngineRef>,
    meta: MetaRef,
    span_ctx: Option<SpanContext>,
    grpc_enable_gzip: bool,
}

impl TemporaryTagScanOpener {
    pub fn new(
        config: QueryConfig,
        kv_inst: Option<EngineRef>,
        meta: MetaRef,
        span_ctx: Option<&SpanContext>,
        grpc_enable_gzip: bool,
    ) -> Self {
        Self {
            config,
            kv_inst,
            meta,
            span_ctx: span_ctx.cloned(),
            grpc_enable_gzip,
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
        let span_ctx = self.span_ctx;
        let grpc_enable_gzip = self.grpc_enable_gzip;

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
                    Span::from_context(
                        format!("LocalTskvTagScanStream ({vnode_id})"),
                        span_ctx.as_ref(),
                    ),
                )
                .map_err(|e| TskvSnafu.into_error(e));
                Ok(Box::pin(stream) as SendableCoordinatorRecordBatchStream)
            } else {
                // 路由到远程的引擎
                let mut request = {
                    let vnode_ids = vec![vnode_id];
                    let req = option
                        .to_query_record_batch_request(vnode_ids)
                        .context(ModelSnafu)?;
                    tonic::Request::new(req)
                };

                grpc_append_trace_context(span_ctx.as_ref(), request.metadata_mut()).map_err(
                    |_| {
                        CommonSnafu {
                            msg: "Parse trace_id, this maybe a bug".to_string(),
                        }
                        .build()
                    },
                )?;

                let resp_stream = {
                    let channel = admin_meta.get_node_conn(node_id).await.map_err(|error| {
                        CoordinatorError::PreExecution {
                            error: error.to_string(),
                        }
                    })?;
                    let mut client = tskv_service_time_out_client(
                        channel,
                        config.read_timeout,
                        DEFAULT_GRPC_SERVER_MESSAGE_LEN,
                        grpc_enable_gzip,
                    );
                    client.tag_scan(request).await?.into_inner()
                };

                Ok(Box::pin(TonicRecordBatchDecoder::new(resp_stream))
                    as SendableCoordinatorRecordBatchStream)
            }
        };

        Ok(Box::pin(future))
    }
}
