pub mod serializer;

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::execution::context::TaskContext;
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion::physical_plan::{RecordBatchStream, SendableRecordBatchStream};
use futures::{pin_mut, TryStreamExt};
use object_store::path::Path;
use object_store::DynObjectStore;
use spi::query::datasource::WriteContext;
use spi::{QueryError, Result};
use trace::debug;

use super::DynRecordBatchSerializer;
use crate::data_source::{RecordBatchSink, RecordBatchSinkProvider, SinkMetadata};
use crate::extension::DropEmptyRecordBatchStream;

pub struct ObjectStoreSink {
    ctx: WriteContext,
    s: Arc<DynRecordBatchSerializer>,
    object_store: Arc<DynObjectStore>,
}

// 128MB
const MAX_FILE_SIZXE: usize = 128 * 1024 * 1024;

#[async_trait]
impl RecordBatchSink for ObjectStoreSink {
    async fn append(&self, _: RecordBatch) -> Result<SinkMetadata> {
        Err(QueryError::Unimplement {
            msg: "ObjectStoreRecordBatchSink::append".to_string(),
        })
    }

    async fn stream_write(&self, stream: SendableRecordBatchStream) -> Result<SinkMetadata> {
        debug!("Process ObjectStoreRecordBatchSink::stream_write");

        let stream = DropEmptyRecordBatchStream::new(stream);

        pin_mut!(stream);

        let mut writer = BufferedWriter::new(
            &self.ctx,
            &self.s,
            &self.object_store,
            stream.schema(),
            MAX_FILE_SIZXE,
        );

        while let Some(batch) = stream.try_next().await? {
            writer.write(batch).await?;
        }

        writer.close().await
    }
}

pub struct ObjectStoreSinkProvider {
    location: Path,
    object_store: Arc<DynObjectStore>,
    serializer: Arc<DynRecordBatchSerializer>,
    file_extension: String,
}

impl ObjectStoreSinkProvider {
    pub fn new(
        location: Path,
        object_store: Arc<DynObjectStore>,
        serializer: Arc<DynRecordBatchSerializer>,
        file_extension: String,
    ) -> Self {
        Self {
            location,
            object_store,
            serializer,
            file_extension,
        }
    }
}

impl RecordBatchSinkProvider for ObjectStoreSinkProvider {
    fn create_batch_sink(
        &self,
        context: Arc<TaskContext>,
        _metrics: &ExecutionPlanMetricsSet,
        partition: usize,
    ) -> Box<dyn RecordBatchSink> {
        let ctx = WriteContext::new(
            self.location.clone(),
            context.task_id(),
            partition,
            self.file_extension.clone(),
        );

        Box::new(ObjectStoreSink {
            ctx,
            s: self.serializer.clone(),
            object_store: self.object_store.clone(),
        })
    }
}

#[non_exhaustive]
struct BufferedWriter<'a> {
    ctx: &'a WriteContext,
    s: &'a Arc<DynRecordBatchSerializer>,
    object_store: &'a Arc<DynObjectStore>,
    schema: SchemaRef,

    /// For each column, maintain an ordered queue of arrays to write
    buffer: Vec<RecordBatch>,
    /// The total number of bytes of memory of currently buffered
    buffered_size: usize,

    // max buffer size
    max_file_size: usize,

    // statistics
    rows_writed: usize,
    bytes_writed: usize,

    // flushed file seq num
    file_number: AtomicU64,
}

impl<'a> BufferedWriter<'a> {
    pub fn new(
        ctx: &'a WriteContext,
        s: &'a Arc<DynRecordBatchSerializer>,
        object_store: &'a Arc<DynObjectStore>,
        schema: SchemaRef,
        max_file_sizxe: usize,
    ) -> Self {
        Self {
            ctx,
            s,
            object_store,
            schema,
            max_file_size: max_file_sizxe,
            buffer: Default::default(),
            buffered_size: Default::default(),
            rows_writed: Default::default(),
            bytes_writed: Default::default(),
            file_number: Default::default(),
        }
    }

    pub async fn write(&mut self, batch: RecordBatch) -> Result<()> {
        self.buffered_size += batch.get_array_memory_size();
        self.buffer.push(batch);
        self.try_flush().await?;
        Ok(())
    }

    pub async fn try_flush(&mut self) -> Result<()> {
        if self.buffered_size >= self.max_file_size {
            self.flush().await?;
        }

        Ok(())
    }

    async fn flush(&mut self) -> Result<()> {
        if self.buffered_size == 0 {
            return Ok(());
        }

        debug!(
            "Export partition {} data of task[{}], flushing, buffered_size: {}, max_file_size: {}",
            self.ctx.task_id(),
            self.ctx.partition(),
            self.buffered_size,
            self.max_file_size
        );

        let (rows_writed, data) = self
            .s
            .batches_to_bytes(self.ctx, self.schema.clone(), &self.buffer)
            .await?;
        self.buffer.clear();
        self.buffered_size = 0;
        let bytes_writed = data.len();

        if bytes_writed > 0 {
            let path = self.ctx.location().child(format!(
                "part-{}-{}-{}{}",
                self.ctx.task_id(),
                self.ctx.partition(),
                self.file_number.fetch_add(1, Ordering::Relaxed),
                self.ctx.file_extension()
            ));

            self.object_store.put(&path, data).await?;

            debug!("Generated file: {}, size: {} bytes.", path, bytes_writed);

            self.rows_writed += rows_writed;
            self.bytes_writed += bytes_writed;
        }

        Ok(())
    }

    pub async fn close(mut self) -> Result<SinkMetadata> {
        self.flush().await?;

        Ok(SinkMetadata::new(self.rows_writed, self.bytes_writed))
    }
}
