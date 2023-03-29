use std::any::Any;
use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::{Schema, SchemaRef};
use datafusion::error::Result;
use datafusion::execution::context::TaskContext;
use datafusion::physical_expr::PhysicalSortExpr;
use datafusion::physical_plan::aggregates::AggregateMode;
use datafusion::physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::{
    create_schema, AggregateExpr, AggregateStream, DisplayFormatType, Distribution, ExecutionPlan,
    Partitioning, SendableRecordBatchStream, Statistics,
};
use trace::debug;

pub struct TableWriterMergeExec {
    input: Arc<dyn ExecutionPlan>,
    /// Execution metrics
    metrics: ExecutionPlanMetricsSet,
    aggr_expr: Vec<Arc<dyn AggregateExpr>>,
    schema: SchemaRef,
}

impl TableWriterMergeExec {
    pub fn try_new(
        input: Arc<dyn ExecutionPlan>,
        aggr_expr: Vec<Arc<dyn AggregateExpr>>,
    ) -> Result<Self> {
        let fields = aggr_expr
            .iter()
            .map(|expr| expr.field())
            .collect::<Result<Vec<_>>>()?;

        let schema = Arc::new(Schema::new_with_metadata(
            fields,
            input.schema().metadata().clone(),
        ));

        Ok(Self {
            input,
            metrics: ExecutionPlanMetricsSet::new(),
            aggr_expr,
            schema,
        })
    }
}

impl Debug for TableWriterMergeExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.fmt_as(DisplayFormatType::Default, f)
    }
}

#[async_trait]
impl ExecutionPlan for TableWriterMergeExec {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(1)
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        None
    }

    fn benefits_from_input_partitioning(&self) -> bool {
        false
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        vec![Distribution::SinglePartition]
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![self.input.clone()]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(TableWriterMergeExec {
            input: children[0].clone(),
            aggr_expr: self.aggr_expr.clone(),
            metrics: self.metrics.clone(),
            schema: self.schema.clone(),
        }))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        debug!(
            "Start TableWriterMergeExec::execute for partition {} of context session_id {} and task_id {:?}",
            partition,
            context.session_id(),
            context.task_id()
        );

        let partial_input = self.input.execute(partition, Arc::clone(&context))?;
        let partial_stream = create_aggregate_stream(
            partition,
            context.clone(),
            BaselineMetrics::new(&self.metrics, partition),
            partial_input,
            AggregateMode::Partial,
            &self.aggr_expr,
        )?;

        let final_stream = create_aggregate_stream(
            partition,
            context,
            BaselineMetrics::new(&self.metrics, partition),
            Box::pin(partial_stream),
            AggregateMode::Final,
            &self.aggr_expr,
        )?;

        Ok(Box::pin(final_stream))
    }

    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default => {
                write!(f, "TableWriterMergeExec",)
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

fn create_aggregate_stream(
    partition: usize,
    context: Arc<TaskContext>,
    baseline_metrics: BaselineMetrics,
    stream: SendableRecordBatchStream,
    mode: AggregateMode,
    aggr_expr: &[Arc<dyn AggregateExpr>],
) -> Result<AggregateStream> {
    let input_schema = Arc::new(create_schema(
        stream.schema().as_ref(),
        &[],
        aggr_expr,
        false,
        mode,
    )?);
    AggregateStream::new(
        mode,
        input_schema,
        aggr_expr.to_vec(),
        stream,
        baseline_metrics,
        context,
        partition,
    )
}
