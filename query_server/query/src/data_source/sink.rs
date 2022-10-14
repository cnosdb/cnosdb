use async_trait::async_trait;
use datafusion::{
    arrow::record_batch::RecordBatch, physical_plan::metrics::ExecutionPlanMetricsSet,
};

use super::Result;

#[async_trait]
pub trait RecordBatchSink: Send + Sync {
    async fn append(&self, record_batch: RecordBatch) -> Result<()>;
}

pub trait RecordBatchSinkProvider: Send + Sync {
    fn create_batch_sink(
        &self,
        metrics: &ExecutionPlanMetricsSet,
        partition: usize,
    ) -> Box<dyn RecordBatchSink>;
}
