use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::Result as DFResult;
use datafusion::datasource::listing::ListingTable;
use datafusion::datasource::TableProvider;
use datafusion::error::DataFusionError;
use datafusion::execution::context::SessionState;
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion::physical_plan::{ExecutionPlan, SendableRecordBatchStream};
use futures::StreamExt;
use spi::Result;
use trace::warn;

use self::table_provider::tskv::ClusterTable;
use crate::extension::physical::plan_node::table_writer::TableWriterExec;

pub mod sink;
pub mod table_provider;
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
impl WriteExecExt for dyn TableProvider {
    async fn write(
        &self,
        state: &SessionState,
        input: Arc<dyn ExecutionPlan>,
    ) -> DFResult<Arc<TableWriterExec>> {
        let table_write: &dyn WriteExecExt =
            if let Some(table) = self.as_any().downcast_ref::<ClusterTable>() {
                table as _
            } else if let Some(table) = self.as_any().downcast_ref::<ListingTable>() {
                table as _
            } else {
                warn!("Table not support write.");
                return Err(DataFusionError::Plan(
                    "Table not support write.".to_string(),
                ));
            };

        let result = table_write.write(state, input).await?;

        Ok(result)
    }
}

#[async_trait]
pub trait RecordBatchSink: Send + Sync {
    async fn append(&self, record_batch: RecordBatch) -> Result<SinkMetadata>;

    async fn stream_write(&self, mut stream: SendableRecordBatchStream) -> Result<SinkMetadata> {
        let mut meta = SinkMetadata::default();

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
