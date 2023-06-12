use std::any::Any;
use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::UInt64Array;
use datafusion::arrow::datatypes::{Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::context::TaskContext;
use datafusion::physical_expr::PhysicalSortExpr;
use datafusion::physical_plan::memory::MemoryStream;
use datafusion::physical_plan::metrics::{
    self, ExecutionPlanMetricsSet, MetricBuilder, MetricsSet,
};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayFormatType, Distribution, ExecutionPlan, Partitioning, SendableRecordBatchStream,
    Statistics,
};
use futures::TryStreamExt;
use spi::query::AFFECTED_ROWS;
use trace::debug;

use crate::data_source::{RecordBatchSink, RecordBatchSinkProvider, SinkMetadata};

pub struct TableWriterExec {
    input: Arc<dyn ExecutionPlan>,
    table: String,
    /// Execution metrics
    metrics: ExecutionPlanMetricsSet,

    schema: SchemaRef,

    record_batch_sink_provider: Arc<dyn RecordBatchSinkProvider>,
}

impl TableWriterExec {
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        table: String,
        record_batch_sink_provider: Arc<dyn RecordBatchSinkProvider>,
    ) -> Self {
        let schema = Arc::new(Schema::new(vec![Field::new(
            AFFECTED_ROWS.0,
            AFFECTED_ROWS.1,
            false,
        )]));

        Self {
            input,
            table,
            metrics: ExecutionPlanMetricsSet::new(),
            record_batch_sink_provider,
            schema,
        }
    }
}

impl Debug for TableWriterExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.fmt_as(DisplayFormatType::Default, f)
    }
}

#[async_trait]
impl ExecutionPlan for TableWriterExec {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn output_partitioning(&self) -> Partitioning {
        self.input.output_partitioning()
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        self.input.output_ordering()
    }

    fn benefits_from_input_partitioning(&self) -> bool {
        false
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        vec![Distribution::UnspecifiedDistribution]
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![self.input.clone()]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(TableWriterExec {
            input: children[0].clone(),
            table: self.table.clone(),
            metrics: self.metrics.clone(),
            schema: self.schema.clone(),
            record_batch_sink_provider: self.record_batch_sink_provider.clone(),
        }))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        debug!(
            "Start TableWriterExec::execute for partition {} of context session_id {} and task_id {:?}",
            partition,
            context.session_id(),
            context.task_id()
        );

        let input = self.input.execute(partition, context.clone())?;
        let record_batch_sink =
            self.record_batch_sink_provider
                .create_batch_sink(context, &self.metrics, partition);

        let metrics = TableWriterMetrics::new(&self.metrics, partition);

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            futures::stream::once(do_write(
                self.schema.clone(),
                input,
                record_batch_sink,
                metrics,
            ))
            .try_flatten(),
        )))
    }

    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default => {
                write!(f, "TableWriterExec: [{}]", self.table,)
            }
        }
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn statistics(&self) -> Statistics {
        self.input.statistics()
    }
}

async fn do_write(
    schema: SchemaRef,
    input: SendableRecordBatchStream,
    record_batch_sink: Box<dyn RecordBatchSink>,
    metrics: TableWriterMetrics,
) -> Result<SendableRecordBatchStream> {
    let timer = metrics.elapsed_write().timer();
    let sink_metadata = record_batch_sink
        .stream_write(input)
        .await
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    timer.done();

    metrics.record_rows_writed(sink_metadata.rows_writed());
    metrics.record_bytes_writed(sink_metadata.bytes_writed());
    metrics.done();

    aggregate_statistiction(schema, sink_metadata)
}

fn aggregate_statistiction(
    schema: SchemaRef,
    metrics: SinkMetadata,
) -> Result<SendableRecordBatchStream> {
    let rows_writed = metrics.rows_writed();

    let output_rows_col = Arc::new(UInt64Array::from(vec![rows_writed as u64]));

    let batch = RecordBatch::try_new(schema.clone(), vec![output_rows_col])?;

    Ok(Box::pin(MemoryStream::try_new(vec![batch], schema, None)?))
}

/// Stores metrics about the table writer execution.
#[derive(Debug, Clone)]
pub struct TableWriterMetrics {
    /// amount of time the operator was actively trying to use the CPU
    elapsed_write: metrics::Time,
    /// end_time is set when `ExecutionMetrics::done()` is called
    end_time: metrics::Timestamp,
    /// Total number of rows writed
    rows_writed: metrics::Count,
    /// Total number of bytes writed
    bytes_writed: metrics::Count,
}

impl TableWriterMetrics {
    /// Create new metrics
    pub fn new(metrics: &ExecutionPlanMetricsSet, partition: usize) -> Self {
        let start_time = MetricBuilder::new(metrics).start_timestamp(partition);
        start_time.record();

        let elapsed_write = MetricBuilder::new(metrics).subset_time("elapsed_write", partition);

        let end_time = MetricBuilder::new(metrics).end_timestamp(partition);

        let rows_writed = MetricBuilder::new(metrics).counter("rows_writed", partition);

        let bytes_writed = MetricBuilder::new(metrics).counter("bytes_writed", partition);

        Self {
            elapsed_write,
            end_time,
            rows_writed,
            bytes_writed,
        }
    }

    /// return the metric for cpu time spend in this operator
    pub fn elapsed_write(&self) -> &metrics::Time {
        &self.elapsed_write
    }

    pub fn rows_writed(&self) -> &metrics::Count {
        &self.rows_writed
    }

    pub fn bytes_writed(&self) -> &metrics::Count {
        &self.bytes_writed
    }

    /// Record that some number of rows have been writed
    pub fn record_rows_writed(&self, num_rows: usize) {
        self.rows_writed.add(num_rows);
    }

    /// Record that bytes size of rows have been writed
    pub fn record_bytes_writed(&self, size: usize) {
        self.bytes_writed.add(size);
    }

    /// Records the fact that this operator's execution is complete
    /// (recording the `end_time` metric).
    pub fn done(&self) {
        self.end_time.record()
    }
}
