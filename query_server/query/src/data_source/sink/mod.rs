use async_trait::async_trait;
use bytes::Bytes;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::physical_plan::SendableRecordBatchStream;
use spi::query::datasource::WriteContext;
use spi::Result;

pub mod obj_store;
pub mod tskv;

pub type DynRecordBatchSerializer = dyn RecordBatchSerializer + Send + Sync;

#[async_trait]
pub trait RecordBatchSerializer {
    /// Serialize [`SendableRecordBatchStream`] into a bytes array.
    ///
    /// Return the number of data rows and bytes array.
    async fn stream_to_bytes(
        &self,
        ctx: &WriteContext,
        stream: SendableRecordBatchStream,
    ) -> Result<(usize, Bytes)>;
    /// Serialize multi [`RecordBatch`] into a bytes array.
    ///
    /// Return the number of data rows and bytes array.
    async fn batches_to_bytes(
        &self,
        ctx: &WriteContext,
        schema: SchemaRef,
        batches: &[RecordBatch],
    ) -> Result<(usize, Bytes)>;
}
