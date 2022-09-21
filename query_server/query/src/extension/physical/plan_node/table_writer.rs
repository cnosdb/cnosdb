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
        common::SizedRecordBatchStream,
        metrics::{ExecutionPlanMetricsSet, MemTrackingMetrics, MetricsSet},
        stream::RecordBatchStreamAdapter,
        DisplayFormatType, Distribution, ExecutionPlan, Partitioning, PhysicalExpr,
        SendableRecordBatchStream, Statistics,
    },
};
use futures::TryStreamExt;
use std::{any::Any, fmt::Debug, sync::Arc};

use datafusion::error::Result;
use futures::StreamExt;
use trace::debug;

use crate::{
    data_source::sink::{RecordBatchSink, RecordBatchSinkPrivider},
    schema::TableSchema,
};

pub struct TableWriterExec {
    input: Arc<dyn ExecutionPlan>,
    schema: TableSchema,
    output_physical_exprs: Vec<Arc<dyn PhysicalExpr>>,
    /// Execution metrics
    metrics: ExecutionPlanMetricsSet,

    record_batch_sink_privider: Arc<dyn RecordBatchSinkPrivider>,
}

impl TableWriterExec {
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        schema: TableSchema,
        output_physical_exprs: Vec<Arc<dyn PhysicalExpr>>,
        record_batch_sink_privider: Arc<dyn RecordBatchSinkPrivider>,
    ) -> Self {
        Self {
            input,
            schema,
            output_physical_exprs,
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
            output_physical_exprs: self.output_physical_exprs.clone(),
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
        let record_batch_sink = self.record_batch_sink_privider.create_batch_sink(partition);
        let metrics = MemTrackingMetrics::new(&self.metrics, partition);

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            futures::stream::once(do_write(input, record_batch_sink, metrics)).try_flatten(),
        )))
    }

    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default => {
                let out_exprs: Vec<String> = self
                    .output_physical_exprs
                    .iter()
                    .map(|e| e.to_string())
                    .collect();
                write!(
                    f,
                    "TableWriterExec: {}",
                    out_exprs.join(",") // self.target_table_name(),
                )?;
                for (name, field) in &self.schema.fields {
                    write!(f, "\n    {} := {}", name, field.column_type,)?;
                }
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
    metrics: MemTrackingMetrics,
) -> Result<SendableRecordBatchStream> {
    while let Some(batch) = input.next().await {
        let batch: RecordBatch = batch?;
        let num_rows = batch.num_rows();

        let timer = metrics.elapsed_compute().timer();
        record_batch_sink
            .append(batch)
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        timer.done();

        metrics.record_output(num_rows);
    }

    metrics.done();

    aggrega_statistiction(metrics)
}

fn aggrega_statistiction(metrics: MemTrackingMetrics) -> Result<SendableRecordBatchStream> {
    let output_rows = metrics.output_rows().value();

    let output_rows_col = Arc::new(UInt64Array::from(vec![output_rows as u64]));
    let schema = Arc::new(Schema::new(vec![Field::new(
        "rows",
        DataType::UInt64,
        false,
    )]));

    let batch = Arc::new(RecordBatch::try_new(schema.clone(), vec![output_rows_col])?);

    Ok(Box::pin(SizedRecordBatchStream::new(
        schema,
        vec![batch],
        metrics,
    )))
}
