use async_trait::async_trait;
use datafusion::{
    arrow::{
        array::UInt64Array,
        datatypes::{DataType, Field, Schema, SchemaRef},
        record_batch::RecordBatch,
    },
    error::DataFusionError,
    execution::context::TaskContext,
    physical_expr::PhysicalSortExpr,
    physical_plan::{
        common::{batch_byte_size, SizedRecordBatchStream},
        metrics::{self, ExecutionPlanMetricsSet, MemTrackingMetrics, MetricBuilder, MetricsSet},
        stream::RecordBatchStreamAdapter,
        DisplayFormatType, Distribution, ExecutionPlan, Partitioning, SendableRecordBatchStream,
        Statistics,
    },
};
use futures::TryStreamExt;
use std::{any::Any, fmt::Debug, sync::Arc};

use datafusion::error::Result;
use futures::StreamExt;
use models::schema::TableSchema;
use trace::debug;

use crate::data_source::sink::{RecordBatchSink, RecordBatchSinkProvider};

pub struct TableWriterExec {
    input: Arc<dyn ExecutionPlan>,
    schema: TableSchema,
    /// Execution metrics
    metrics: ExecutionPlanMetricsSet,

    record_batch_sink_privider: Arc<dyn RecordBatchSinkProvider>,
}

impl TableWriterExec {
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        schema: TableSchema,
        record_batch_sink_privider: Arc<dyn RecordBatchSinkProvider>,
    ) -> Self {
        Self {
            input,
            schema,
            metrics: ExecutionPlanMetricsSet::new(),
            record_batch_sink_privider,
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
        self.input.schema()
    }

    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(1)
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        None
    }

    fn relies_on_input_order(&self) -> bool {
        false
    }

    fn benefits_from_input_partitioning(&self) -> bool {
        false
    }

    fn required_child_distribution(&self) -> Distribution {
        Distribution::UnspecifiedDistribution
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
            schema: self.schema.clone(),
            metrics: self.metrics.clone(),
            record_batch_sink_privider: self.record_batch_sink_privider.clone(),
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

        let input = self.input.execute(partition, context)?;
        let record_batch_sink = self
            .record_batch_sink_privider
            .create_batch_sink(&self.metrics, partition);

        let metrics = TableWriterMetrics::new(&self.metrics, partition);

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            futures::stream::once(do_write(input, record_batch_sink, metrics)).try_flatten(),
        )))
    }

    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default => {
                let schemas: Vec<String> = self
                    .schema
                    .fields
                    .iter()
                    .map(|(k, v)| format!("{} := {}", k, v.column_type))
                    .collect();

                write!(f, "TableWriterExec: schema=[{}]", schemas.join(", "),)?;

                Ok(())
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
    mut input: SendableRecordBatchStream,
    record_batch_sink: Box<dyn RecordBatchSink>,
    metrics: TableWriterMetrics,
) -> Result<SendableRecordBatchStream> {
    while let Some(batch) = input.next().await {
        let batch: RecordBatch = batch?;
        let num_rows = batch.num_rows();
        let size = batch_byte_size(&batch);

        let timer = metrics.elapsed_compute().timer();
        record_batch_sink
            .append(batch)
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        timer.done();

        metrics.record_rows_writed(num_rows);
        metrics.record_bytes_writed(size);
    }

    metrics.done();

    aggregate_statistiction(metrics)
}

fn aggregate_statistiction(metrics: TableWriterMetrics) -> Result<SendableRecordBatchStream> {
    let rows_writed = metrics.rows_writed().value();

    let output_rows_col = Arc::new(UInt64Array::from(vec![rows_writed as u64]));
    let schema = Arc::new(Schema::new(vec![Field::new(
        "rows",
        DataType::UInt64,
        false,
    )]));

    let batch = Arc::new(RecordBatch::try_new(schema.clone(), vec![output_rows_col])?);

    let metrics = MemTrackingMetrics::new(&ExecutionPlanMetricsSet::new(), 0);

    Ok(Box::pin(SizedRecordBatchStream::new(
        schema,
        vec![batch],
        metrics,
    )))
}

/// Stores metrics about the table writer execution.
#[derive(Debug, Clone)]
pub struct TableWriterMetrics {
    /// amount of time the operator was actively trying to use the CPU
    elapsed_compute: metrics::Time,
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

        let elapsed_compute = MetricBuilder::new(metrics).elapsed_compute(partition);
        let end_time = MetricBuilder::new(metrics).end_timestamp(partition);

        let rows_writed = MetricBuilder::new(metrics).counter("rows_writed", partition);

        let bytes_writed = MetricBuilder::new(metrics).counter("bytes_writed", partition);

        Self {
            elapsed_compute,
            end_time,
            rows_writed,
            bytes_writed,
        }
    }

    /// return the metric for cpu time spend in this operator
    pub fn elapsed_compute(&self) -> &metrics::Time {
        &self.elapsed_compute
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
