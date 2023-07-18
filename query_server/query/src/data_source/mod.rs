use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::Result as DFResult;
use datafusion::execution::context::{SessionState, TaskContext};
use datafusion::logical_expr::TableSource;
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion::physical_plan::{ExecutionPlan, SendableRecordBatchStream};
use futures::StreamExt;
use spi::{QueryError, Result};

use self::table_source::TableSourceAdapter;
use crate::extension::physical::plan_node::table_writer::TableWriterExec;
use crate::extension::DropEmptyRecordBatchStream;

pub mod batch;
pub mod sink;
pub mod split;
pub mod stream;
pub mod table_source;
pub mod write_exec_ext;

#[async_trait]
pub trait WriteExecExt: Send + Sync {
    async fn write(
        &self,
        state: &SessionState,
        input: Arc<dyn ExecutionPlan>,
    ) -> DFResult<Arc<TableWriterExec>>;
}

#[async_trait]
pub trait RecordBatchSink: Send + Sync {
    async fn append(&self, record_batch: RecordBatch) -> Result<SinkMetadata>;

    async fn stream_write(&self, stream: SendableRecordBatchStream) -> Result<SinkMetadata> {
        let mut meta = SinkMetadata::default();
        let mut stream = DropEmptyRecordBatchStream::new(stream);

        while let Some(batch) = stream.next().await {
            let batch: RecordBatch = batch?;
            meta.merge(self.append(batch).await?);
        }

        Ok(meta)
    }
}

pub trait RecordBatchSinkProvider: Send + Sync {
    fn create_batch_sink(
        &self,
        context: Arc<TaskContext>,
        metrics: &ExecutionPlanMetricsSet,
        partition: usize,
    ) -> Box<dyn RecordBatchSink>;
}

#[derive(Default)]
pub struct SinkMetadata {
    rows_writed: usize,
    bytes_writed: usize,
}

impl SinkMetadata {
    pub fn new(rows_writed: usize, bytes_writed: usize) -> Self {
        Self {
            rows_writed,
            bytes_writed,
        }
    }

    pub fn merge(&mut self, other: Self) {
        self.rows_writed += other.rows_writed;
        self.bytes_writed += other.bytes_writed;
    }

    pub fn record_rows_writed(&mut self, rows_writed: usize) {
        self.rows_writed += rows_writed;
    }

    pub fn record_bytes_writed(&mut self, bytes_writed: usize) {
        self.bytes_writed += bytes_writed;
    }

    pub fn rows_writed(&self) -> usize {
        self.rows_writed
    }

    pub fn bytes_writed(&self) -> usize {
        self.bytes_writed
    }
}

/// Attempt to downcast a TableSource to DefaultTableSource and access the
/// TableProvider. \
/// Then attempt to downcast a TableProvider to TableProviderAdapter and access the
/// TableProviderAdapter.
pub fn source_downcast_adapter(source: &Arc<dyn TableSource>) -> Result<&TableSourceAdapter> {
    match source.as_any().downcast_ref::<TableSourceAdapter>() {
        Some(adapter) => Ok(adapter),
        _ => Err(QueryError::Internal {
            reason: "TableProvider was not TableProviderAdapter".to_string(),
        }),
    }
}
