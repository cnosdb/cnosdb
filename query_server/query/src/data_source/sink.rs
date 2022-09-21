use async_trait::async_trait;
use datafusion::arrow::record_batch::RecordBatch;

use super::Result;

#[async_trait]
pub trait RecordBatchSink: Send + Sync {
    async fn append(&self, record_batch: RecordBatch) -> Result<()>;
}

pub trait RecordBatchSinkPrivider: Send + Sync {
    fn create_batch_sink(&self, partition: usize) -> Box<dyn RecordBatchSink>;
}
