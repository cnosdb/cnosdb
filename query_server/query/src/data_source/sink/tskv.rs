use std::sync::Arc;

use async_trait::async_trait;
use coordinator::service::CoordinatorRef;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::execution::context::TaskContext;
use datafusion::physical_plan::metrics::{self, Count, ExecutionPlanMetricsSet, MetricBuilder};
use models::schema::tskv_table_schema::TskvTableSchemaRef;
use snafu::ResultExt;
use spi::{CoordinatorSnafu, MetaSnafu, QueryResult};
use trace::span_ext::SpanExt;
use trace::{Span, SpanContext};

use crate::data_source::{RecordBatchSink, RecordBatchSinkProvider, SinkMetadata};

pub struct TskvRecordBatchSink {
    coord: CoordinatorRef,
    partition: usize,
    schema: TskvTableSchemaRef,

    metrics: TskvSinkMetrics,
    span: Span,
}

#[async_trait]
impl RecordBatchSink for TskvRecordBatchSink {
    async fn append(&self, record_batch: RecordBatch) -> QueryResult<SinkMetadata> {
        trace::trace!(
            "Partition: {}, \nTskvTableSchema: {:?}, \nTskvRecordBatchSink::append: {:?}",
            self.partition,
            self.schema,
            record_batch,
        );

        let mut span = Span::enter_with_parent(
            format!("Batch ({})", self.metrics.output_batches()),
            &self.span,
        );

        let rows_writed = record_batch.num_rows();

        let timer = self.metrics.elapsed_record_batch_write().timer();
        let record_batch_size = record_batch.get_array_memory_size() as u64;

        let tenant = self.schema.tenant.as_str();
        let db_name = self.schema.db.as_str();
        let meta_client = self
            .coord
            .tenant_meta(tenant)
            .await
            .ok_or_else(|| coordinator::errors::CoordinatorError::TenantNotFound {
                name: tenant.to_string(),
            })
            .context(CoordinatorSnafu)?;
        let db_schema = meta_client
            .get_db_schema(db_name)
            .context(MetaSnafu)?
            .ok_or_else(|| meta::error::MetaError::DatabaseNotFound {
                database: db_name.to_string(),
            })
            .context(MetaSnafu)?;
        if db_schema.is_hidden() {
            return Err(spi::QueryError::Meta {
                source: meta::error::MetaError::DatabaseNotFound {
                    database: db_name.to_string(),
                },
            });
        }

        let db_precision = db_schema.config.precision();
        let write_bytes = self
            .coord
            .write_record_batch(
                self.schema.clone(),
                record_batch,
                *db_precision,
                span.context().as_ref(),
            )
            .await
            .inspect(|_write_bytes| {
                span.add_property(|| ("output_rows", rows_writed.to_string()));
            })
            .inspect_err(|err| {
                span.error(err.to_string());
            })
            .context(CoordinatorSnafu)?;
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
        self.schema.build_arrow_schema()
    }

    fn create_batch_sink(
        &self,
        context: Arc<TaskContext>,
        metrics: &ExecutionPlanMetricsSet,
        partition: usize,
    ) -> Box<dyn RecordBatchSink> {
        let parent_span_ctx = context.session_config().get_extension::<SpanContext>();
        let span = Span::from_context(
            format!("TskvRecordBatchSink ({partition})"),
            parent_span_ctx.as_deref(),
        );

        Box::new(TskvRecordBatchSink {
            coord: self.coord.clone(),
            partition,
            schema: self.schema.clone(),
            metrics: TskvSinkMetrics::new(metrics, partition),
            span,
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
