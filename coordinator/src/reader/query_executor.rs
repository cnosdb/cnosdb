use std::sync::Arc;

use meta::model::MetaRef;
use metrics::count::U64Counter;
use models::meta_data::{NodeId, VnodeInfo};
use tokio::runtime::Runtime;
use tskv::query_iterator::QueryOption;
use tskv::EngineRef;

use super::status_listener::VnodeStatusListener;
use super::table_scan::local::LocalTskvTableScanStream;
use super::tag_scan::local::LocalTskvTagScanStream;
use super::ParallelMergeStream;
use crate::errors::{CoordinatorError, CoordinatorResult};
use crate::service::CoordServiceMetrics;
use crate::SendableCoordinatorRecordBatchStream;

pub struct QueryExecutor {
    option: QueryOption,
    meta: MetaRef,
    runtime: Arc<Runtime>,
    kv_inst: Option<EngineRef>,

    data_out: U64Counter,
}

impl QueryExecutor {
    pub fn new(
        option: QueryOption,
        runtime: Arc<Runtime>,
        meta: MetaRef,
        kv_inst: Option<EngineRef>,
        metrics: Arc<CoordServiceMetrics>,
    ) -> Self {
        let data_out = metrics.data_out(
            option.table_schema.tenant.as_str(),
            option.table_schema.db.as_str(),
        );
        Self {
            option,
            runtime,
            meta,
            kv_inst,
            data_out,
        }
    }

    pub fn local_node_executor(
        &self,
        node_id: NodeId,
        vnodes: Vec<VnodeInfo>,
    ) -> CoordinatorResult<SendableCoordinatorRecordBatchStream> {
        let kv = self
            .kv_inst
            .as_ref()
            .ok_or(CoordinatorError::KvInstanceNotFound { node_id })?
            .clone();

        let mut streams = Vec::with_capacity(vnodes.len());

        vnodes.into_iter().for_each(|vnode| {
            let input = Box::pin(LocalTskvTableScanStream::new(
                vnode.id,
                self.option.clone(),
                kv.clone(),
                self.runtime.clone(),
                self.data_out.clone(),
            ));
            let stream = VnodeStatusListener::new(
                &self.option.table_schema.tenant,
                self.meta.clone(),
                vnode.id,
                input,
            );
            streams.push(Box::pin(stream) as SendableCoordinatorRecordBatchStream);
        });

        let parallel_merge_stream = ParallelMergeStream::new(Some(self.runtime.clone()), streams);

        Ok(Box::pin(parallel_merge_stream))
    }

    pub fn local_node_tag_scan(
        &self,
        node_id: NodeId,
        vnodes: Vec<VnodeInfo>,
    ) -> CoordinatorResult<SendableCoordinatorRecordBatchStream> {
        let kv = self
            .kv_inst
            .as_ref()
            .ok_or(CoordinatorError::KvInstanceNotFound { node_id })?
            .clone();

        let mut streams = Vec::with_capacity(vnodes.len());
        vnodes.into_iter().for_each(|vnode| {
            let stream = LocalTskvTagScanStream::new(
                vnode.id,
                self.option.clone(),
                kv.clone(),
                self.data_out.clone(),
            );
            streams.push(Box::pin(stream) as SendableCoordinatorRecordBatchStream);
        });

        let parallel_merge_stream = ParallelMergeStream::new(Some(self.runtime.clone()), streams);

        Ok(Box::pin(parallel_merge_stream))
    }
}
