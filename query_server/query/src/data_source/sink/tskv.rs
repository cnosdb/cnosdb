use std::sync::Arc;

use async_trait::async_trait;
use coordinator::service::CoordinatorRef;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::execution::context::TaskContext;
use datafusion::physical_plan::metrics::{self, Count, ExecutionPlanMetricsSet, MetricBuilder};
use models::schema::TskvTableSchemaRef;
use spi::Result;
use trace::{SpanContext, SpanExt, SpanRecorder};

use crate::data_source::{RecordBatchSink, RecordBatchSinkProvider, SinkMetadata};

pub struct TskvRecordBatchSink {
    coord: CoordinatorRef,
    partition: usize,
    schema: TskvTableSchemaRef,

    metrics: TskvSinkMetrics,
    span_recorder: SpanRecorder,
}

#[async_trait]
impl RecordBatchSink for TskvRecordBatchSink {
    async fn append(&self, record_batch: RecordBatch) -> Result<SinkMetadata> {
        trace::trace!(
            "Partition: {}, \nTskvTableSchema: {:?}, \nTskvRecordBatchSink::append: {:?}",
            self.partition,
            self.schema,
            record_batch,
        );

        let mut span_recorder = self
            .span_recorder
            .child(format!("Batch ({})", self.metrics.output_batches()));

        let rows_writed = record_batch.num_rows();

        let timer = self.metrics.elapsed_record_batch_write().timer();
        let record_batch_size = record_batch.get_array_memory_size() as u64;

        let tenant = self.schema.tenant.as_str();
        let db_name = self.schema.db.as_str();
        let meta_client = self.coord.tenant_meta(tenant).await.ok_or(
            coordinator::errors::CoordinatorError::TenantNotFound {
                name: tenant.to_string(),
            },
        )?;
        let db_schema = meta_client.get_db_schema(db_name)?.ok_or_else(|| {
            meta::error::MetaError::DatabaseNotFound {
                database: db_name.to_string(),
            }
        })?;
        if db_schema.options().get_db_is_hidden() {
            return Err(spi::QueryError::Meta {
                source: meta::error::MetaError::DatabaseNotFound {
                    database: db_name.to_string(),
                },
            });
        }

        let db_precision = db_schema.config.precision_or_default();
        let write_bytes = self
            .coord
            .write_record_batch(
                self.schema.clone(),
                record_batch,
                *db_precision,
                span_recorder.span_ctx(),
            )
            .await
            .map(|write_bytes| {
                span_recorder.set_metadata("output_rows", rows_writed);
                write_bytes
            })
            .map_err(|err| {
                span_recorder.error(err.to_string());
                err
            })?;
        self.coord
            .metrics()
            .sql_data_in(self.schema.tenant.as_str(), self.schema.db.as_str())
            .inc(record_batch_size);

        timer.done();

        // Record the number of `RecordBatch` that has been written
        self.metrics.record_output_batches(1);

        Ok(SinkMetadata::new(rows_writed, write_bytes))
    }
}

pub struct TskvRecordBatchSinkProvider {
    coord: CoordinatorRef,
    schema: TskvTableSchemaRef,
}

impl TskvRecordBatchSinkProvider {
    pub fn new(coord: CoordinatorRef, schema: TskvTableSchemaRef) -> Self {
        Self { coord, schema }
    }
}

impl RecordBatchSinkProvider for TskvRecordBatchSinkProvider {
    fn schema(&self) -> SchemaRef {
        self.schema.to_arrow_schema()
    }

    fn create_batch_sink(
        &self,
        context: Arc<TaskContext>,
        metrics: &ExecutionPlanMetricsSet,
        partition: usize,
    ) -> Box<dyn RecordBatchSink> {
        let parent_span_ctx = context.session_config().get_extension::<SpanContext>();
        let span_recorder = SpanRecorder::new(
            parent_span_ctx.child_span(format!("TskvRecordBatchSink ({partition})")),
        );

        Box::new(TskvRecordBatchSink {
            coord: self.coord.clone(),
            partition,
            schema: self.schema.clone(),
            metrics: TskvSinkMetrics::new(metrics, partition),
            span_recorder,
        })
    }
}

/// Stores metrics about the tskv sink execution.
#[derive(Debug)]
pub struct TskvSinkMetrics {
    elapsed_record_batch_write: metrics::Time,
    output_batches: Count,
}

impl TskvSinkMetrics {
    /// Create new metrics
    pub fn new(metrics: &ExecutionPlanMetricsSet, partition: usize) -> Self {
        let elapsed_record_batch_write =
            MetricBuilder::new(metrics).subset_time("elapsed_record_batch_write", partition);

        let output_batches = MetricBuilder::new(metrics).counter("output_batches", partition);

        Self {
            elapsed_record_batch_write,
            output_batches,
        }
    }

    pub fn elapsed_record_batch_write(&self) -> &metrics::Time {
        &self.elapsed_record_batch_write
    }

    pub fn record_output_batches(&self, num: usize) {
        self.output_batches.add(num);
    }

    pub fn output_batches(&self) -> usize {
        self.output_batches.value()
    }
}
