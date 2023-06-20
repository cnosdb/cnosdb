use std::sync::Arc;

use datafusion::arrow::record_batch::RecordBatch;
use meta::model::MetaRef;
use models::arrow::stream::{BoxStream, ParallelMergeStream};
use models::meta_data::VnodeInfo;
use tokio::runtime::Runtime;
use trace::{SpanContext, SpanExt, SpanRecorder};

use super::status_listener::VnodeStatusListener;
use super::table_scan::LocalTskvTableScanStream;
use super::tag_scan::LocalTskvTagScanStream;
use crate::error::Result;
use crate::reader::{QueryOption, SendableTskvRecordBatchStream};
use crate::EngineRef;

pub struct QueryExecutor {
    option: QueryOption,
    meta: MetaRef,
    runtime: Arc<Runtime>,
    kv_inst: EngineRef,
}

impl QueryExecutor {
    pub fn new(
        option: QueryOption,
        runtime: Arc<Runtime>,
        meta: MetaRef,
        kv_inst: EngineRef,
    ) -> Self {
        Self {
            option,
            runtime,
            meta,
            kv_inst,
        }
    }

    pub fn local_node_executor(
        &self,
        vnodes: Vec<VnodeInfo>,
        span_ctx: Option<&SpanContext>,
    ) -> Result<SendableTskvRecordBatchStream> {
        let mut streams: Vec<BoxStream<Result<RecordBatch>>> = Vec::with_capacity(vnodes.len());

        vnodes.into_iter().for_each(|vnode| {
            let input = Box::pin(LocalTskvTableScanStream::new(
                vnode.id,
                self.option.clone(),
                self.kv_inst.clone(),
                self.runtime.clone(),
                SpanRecorder::new(
                    span_ctx.child_span(format!("LocalTskvTableScanStream ({})", vnode.id)),
                ),
            ));
            let stream = VnodeStatusListener::new(
                &self.option.table_schema.tenant,
                self.meta.clone(),
                vnode.id,
                input,
            );
            streams.push(Box::pin(stream));
        });

        let parallel_merge_stream = ParallelMergeStream::new(Some(self.runtime.clone()), streams);

        Ok(Box::pin(parallel_merge_stream))
    }

    pub fn local_node_tag_scan(
        &self,
        vnodes: Vec<VnodeInfo>,
        span_ctx: Option<&SpanContext>,
    ) -> Result<SendableTskvRecordBatchStream> {
        let mut streams = Vec::with_capacity(vnodes.len());
        vnodes.into_iter().for_each(|vnode| {
            let stream = LocalTskvTagScanStream::new(
                vnode.id,
                self.option.clone(),
                self.kv_inst.clone(),
                SpanRecorder::new(
                    span_ctx.child_span(format!("LocalTskvTagScanStream ({})", vnode.id)),
                ),
            );
            streams.push(Box::pin(stream) as SendableTskvRecordBatchStream);
        });

        let parallel_merge_stream = ParallelMergeStream::new(Some(self.runtime.clone()), streams);

        Ok(Box::pin(parallel_merge_stream))
    }
}
