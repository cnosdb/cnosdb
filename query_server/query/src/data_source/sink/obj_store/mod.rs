pub mod serializer;

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::execution::context::TaskContext;
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion::physical_plan::SendableRecordBatchStream;
use futures::{pin_mut, TryStreamExt};
use object_store::path::Path;
use object_store::DynObjectStore;
use spi::query::datasource::WriteContext;
use spi::query::session::SqlExecInfo;
use spi::{ObjectStoreSnafu, QueryError, QueryResult};
use trace::debug;

use super::DynRecordBatchSerializer;
use crate::data_source::{RecordBatchSink, RecordBatchSinkProvider, SinkMetadata};

pub struct ObjectStoreSink {
    ctx: WriteContext,
    s: Arc<DynRecordBatchSerializer>,
    object_store: Arc<DynObjectStore>,
}

#[async_trait]
impl RecordBatchSink for ObjectStoreSink {
    async fn append(&self, _: RecordBatch) -> QueryResult<SinkMetadata> {
        Err(QueryError::Unimplemented {
            msg: "ObjectStoreRecordBatchSink::append".to_string(),
        })
    }

    async fn stream_write(&self, stream: SendableRecordBatchStream) -> QueryResult<SinkMetadata> {
        debug!("Process ObjectStoreRecordBatchSink::stream_write");

        pin_mut!(stream);

        let mut writer =
            BufferedWriter::new(&self.ctx, &self.s, &self.object_store, stream.schema());

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
    schema: SchemaRef,
}

impl ObjectStoreSinkProvider {
    pub fn new(
        location: Path,
        object_store: Arc<DynObjectStore>,
        serializer: Arc<DynRecordBatchSerializer>,
        file_extension: String,
        schema: SchemaRef,
    ) -> Self {
        Self {
            location,
            object_store,
            serializer,
            file_extension,
            schema,
        }
    }
}

impl RecordBatchSinkProvider for ObjectStoreSinkProvider {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn create_batch_sink(
        &self,
        context: Arc<TaskContext>,
        _metrics: &ExecutionPlanMetricsSet,
        partition: usize,
    ) -> Box<dyn RecordBatchSink> {
        let ctx = WriteContext::new(
            context.session_id(),
            self.location.clone(),
            context.task_id(),
            partition,
            self.file_extension.clone(),
            context
                .session_config()
                .options()
                .extensions
                .get::<SqlExecInfo>()
                .unwrap()
                .clone(),
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
    rows_wrote: usize,
    bytes_wrote: usize,

    // flushed file seq num
    file_number: AtomicU64,
}

impl<'a> BufferedWriter<'a> {
    pub fn new(
        ctx: &'a WriteContext,
        s: &'a Arc<DynRecordBatchSerializer>,
        object_store: &'a Arc<DynObjectStore>,
        schema: SchemaRef,
    ) -> Self {
        Self {
            ctx,
            s,
            object_store,
            schema,
            max_file_size: ctx.sql_exec_info().copyinto_trigger_flush_size as usize,
            buffer: Default::default(),
            buffered_size: Default::default(),
            rows_wrote: Default::default(),
            bytes_wrote: Default::default(),
            file_number: Default::default(),
        }
    }

    pub async fn write(&mut self, batch: RecordBatch) -> QueryResult<()> {
        self.buffered_size += batch.get_array_memory_size();
        self.buffer.push(batch);
        self.try_flush().await?;
        Ok(())
    }

    pub async fn try_flush(&mut self) -> QueryResult<()> {
        if self.buffered_size >= self.max_file_size {
            self.flush().await?;
        }

        Ok(())
    }

    async fn flush(&mut self) -> QueryResult<()> {
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

        let (rows_wrote, data) = self
            .s
            .batches_to_bytes(self.ctx, self.schema.clone(), &self.buffer)
            .await?;
        self.buffer.clear();
        self.buffered_size = 0;
        let bytes_wrote = data.len();

        if bytes_wrote > 0 {
            let path = self.ctx.location().child(format!(
                "queryId-{}-part-{}-{}-{}{}",
                self.ctx.query_id(),
                self.ctx.task_id(),
                self.ctx.partition(),
                self.file_number.fetch_add(1, Ordering::Relaxed),
                self.ctx.file_extension()
            ));

            self.object_store
                .put(&path, data.into())
                .await
                .map_err(|e| ObjectStoreSnafu { msg: e.to_string() }.build())?;

            debug!("Generated file: {}, size: {} bytes.", path, bytes_wrote);

            self.rows_wrote += rows_wrote;
            self.bytes_wrote += bytes_wrote;
        }

        Ok(())
    }

    pub async fn close(mut self) -> QueryResult<SinkMetadata> {
        self.flush().await?;

        Ok(SinkMetadata::new(self.rows_wrote, self.bytes_wrote))
    }
}
