use std::{
    any::Any,
    fmt::Debug,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use async_trait::async_trait;
use datafusion::{
    arrow::{
        self,
        array::UInt64Array,
        datatypes::{DataType, Field, Schema, SchemaRef},
        record_batch::RecordBatch,
    },
    execution::context::TaskContext,
    physical_expr::PhysicalSortExpr,
    physical_plan::{
        metrics::{
            BaselineMetrics, ExecutionPlanMetricsSet,
            MetricsSet,
        },
        DisplayFormatType, Distribution, ExecutionPlan, Partitioning, PhysicalExpr,
        RecordBatchStream, SendableRecordBatchStream, Statistics,
    },
};

use arrow::error::Result as ArrowResult;
use datafusion::error::Result;
use futures::{Stream, StreamExt};
use trace::debug;
use tskv::engine::EngineRef;

use crate::schema::TableSchema;

pub struct TskvWriterExec {
    input: Arc<dyn ExecutionPlan>,
    schema: TableSchema,
    output_physical_exprs: Vec<Arc<dyn PhysicalExpr>>,
    /// Execution metrics
    metrics: ExecutionPlanMetricsSet,

    engine: EngineRef,
}

impl TskvWriterExec {
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        schema: TableSchema,
        output_physical_exprs: Vec<Arc<dyn PhysicalExpr>>,
        engine: EngineRef,
    ) -> Self {
        Self {
            input,
            schema,
            output_physical_exprs,
            metrics: ExecutionPlanMetricsSet::new(),
            engine,
        }
    }
}

impl Debug for TskvWriterExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "TskvWriteExec")
    }
}

#[async_trait]
impl ExecutionPlan for TskvWriterExec {
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
        Ok(Arc::new(TskvWriterExec {
            input: children[0].clone(),
            schema: self.schema.clone(),
            output_physical_exprs: self.output_physical_exprs.clone(),
            metrics: self.metrics.clone(),
            engine: self.engine.clone(),
        }))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        debug!(
            "Start TskvWriteExec::execute for partition {} of context session_id {} and task_id {:?}",
            partition,
            context.session_id(),
            context.task_id()
        );

        let baseline_metrics = BaselineMetrics::new(&self.metrics, partition);
        Ok(Box::pin(TskvWriterExecStream {
            schema: self.input.schema(),
            input: self.input.execute(partition, context)?,
            baseline_metrics,
        }))
    }

    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default => {
                write!(f, "TskvWriteExec: ")
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

struct TskvWriterExecStream {
    /// Output schema, which is the same as the out put exprs
    schema: SchemaRef,
    /// The input partition to filter.
    input: SendableRecordBatchStream,
    /// runtime metrics recording
    baseline_metrics: BaselineMetrics,
}

impl Stream for TskvWriterExecStream {
    type Item = ArrowResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let poll = self.input.poll_next_unpin(cx).map(|x| match x {
            Some(Ok(batch)) => {
                let timer = self.baseline_metrics.elapsed_compute().timer();
                let statistics = batch_insert(&batch);
                timer.done();
                Some(statistics)
            }
            other => other,
        });
        self.baseline_metrics.record_poll(poll)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        // same number of record batches
        self.input.size_hint()
    }
}

impl RecordBatchStream for TskvWriterExecStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

fn batch_insert(_batch: &RecordBatch) -> ArrowResult<RecordBatch> {
    debug!("处理record batch");
    wrap_affected_row_result(10)
}

fn wrap_affected_row_result(count: u64) -> ArrowResult<RecordBatch> {
    let column = UInt64Array::from(vec![count]);
    let schema = Arc::new(Schema::new(vec![Field::new(
        "count",
        DataType::UInt64,
        false,
    )]));

    let batch = RecordBatch::try_new(schema, vec![Arc::new(column)])?;

    Ok(batch)
}
