use std::sync::Arc;

use config::QueryConfig;
use meta::model::MetaRef;
use models::meta_data::VnodeInfo;
use tskv::query_iterator::{QueryOption, TskvSourceMetrics};
use tskv::EngineRef;

use super::local::LocalTskvTagScanStream;
use super::remote::TonicTskvTagScanStream;
use crate::errors::{CoordinatorError, CoordinatorResult};
use crate::reader::{VnodeOpenFuture, VnodeOpener};
use crate::service::CoordServiceMetrics;
use crate::SendableCoordinatorRecordBatchStream;

pub struct TemporaryTagScanOpener {
    config: QueryConfig,
    kv_inst: Option<EngineRef>,
    meta: MetaRef,
    metrics: TskvSourceMetrics,
    coord_metrics: Arc<CoordServiceMetrics>,
}

impl TemporaryTagScanOpener {
    pub fn new(
        config: QueryConfig,
        kv_inst: Option<EngineRef>,
        meta: MetaRef,
        metrics: TskvSourceMetrics,
        coord_metrics: Arc<CoordServiceMetrics>,
    ) -> Self {
        Self {
            config,
            kv_inst,
            meta,
            metrics,
            coord_metrics,
        }
    }
}

impl VnodeOpener for TemporaryTagScanOpener {
    fn open(&self, vnode: &VnodeInfo, option: &QueryOption) -> CoordinatorResult<VnodeOpenFuture> {
        let node_id = vnode.node_id;
        let vnode_id = vnode.id;
        let curren_nodet_id = self.meta.node_id();
        let kv_inst = self.kv_inst.clone();
        let metrics = self.metrics.clone();
        let coord_metrics = self.coord_metrics.clone();
        let option = option.clone();
        let admin_meta = self.meta.admin_meta();
        let config = self.config.clone();

        let future = async move {
            // TODO 请求路由的过程应该由通信框架决定，客户端只关心业务逻辑（请求目标和请求内容）
            if node_id == curren_nodet_id {
                let data_out = coord_metrics.data_out(
                    option.table_schema.tenant.as_str(),
                    option.table_schema.db.as_str(),
                );

                // 路由到进程内的引擎
                let kv_inst = kv_inst.ok_or(CoordinatorError::KvInstanceNotFound { node_id })?;
                // TODO U64Counter
                let stream = LocalTskvTagScanStream::new(vnode_id, option, kv_inst, data_out);
                Ok(Box::pin(stream) as SendableCoordinatorRecordBatchStream)
            } else {
                // 路由到远程的引擎
                let request = {
                    let vnode_ids = vec![vnode_id];
                    let req = option
                        .to_query_record_batch_request(vnode_ids)
                        .map_err(CoordinatorError::from)?;
                    tonic::Request::new(req)
                };

                Ok(Box::pin(TonicTskvTagScanStream::new(
                    config, node_id, request, admin_meta, metrics,
                )) as SendableCoordinatorRecordBatchStream)
            }
        };

        Ok(Box::pin(future))
    }
}
