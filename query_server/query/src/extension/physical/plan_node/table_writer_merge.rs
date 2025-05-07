use std::any::Any;
use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::error::Result;
use datafusion::execution::context::TaskContext;
use datafusion::physical_expr::PhysicalSortExpr;
use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode, PhysicalGroupBy};
use datafusion::physical_plan::metrics::MetricsSet;
use datafusion::physical_plan::{
    AggregateExpr, AggregateStream, DisplayAs, DisplayFormatType, Distribution, ExecutionPlan,
    Partitioning, PlanProperties, SendableRecordBatchStream, Statistics,
};
use trace::debug;

pub struct TableWriterMergeExec {
    // input: Arc<dyn ExecutionPlan>,
    // /// Execution metrics
    // metrics: ExecutionPlanMetricsSet,
    aggr_expr: Vec<Arc<dyn AggregateExpr>>,
    // schema: SchemaRef,
    agg_exec: Arc<AggregateExec>,
}

impl TableWriterMergeExec {
    pub fn try_new(
        input: Arc<dyn ExecutionPlan>,
        aggr_expr: Vec<Arc<dyn AggregateExpr>>,
    ) -> Result<Self> {
        let agg_exec = create_aggregate_exec(input, AggregateMode::Single, &aggr_expr)?;

        Ok(Self {
            aggr_expr,
            agg_exec: Arc::new(agg_exec),
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
    fn name(&self) -> &str {
        "TableWriterMergeExec"
    }

    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.agg_exec.schema()
    }

    fn properties(&self) -> &PlanProperties {
        &self.agg_exec.properties
    }

    fn benefits_from_input_partitioning(&self) -> bool {
        self.agg_exec.benefits_from_input_partitioning()
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        vec![Distribution::SinglePartition]
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        self.agg_exec.children()
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Self::try_new(children[0].clone(), self.aggr_expr.clone()).map(Arc::new)?)
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

        Ok(Box::pin(AggregateStream::new(
            self.agg_exec.as_ref(),
            context,
            partition,
        )?))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        self.agg_exec.metrics()
    }

    fn statistics(&self) -> Statistics {
        self.agg_exec.statistics()
    }
}

impl DisplayAs for TableWriterMergeExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                let exprs: Vec<String> = self
                    .agg_exec
                    .aggr_expr()
                    .iter()
                    .map(|agg| agg.name().to_string())
                    .collect();
                write!(f, "TableWriterMergeExec: expr=[{}]", exprs.join(","))
            }
            DisplayFormatType::TreeRender => {
                // TODO(zipper): implement this.
                write!(f, "")
            }
        }
    }
}

fn create_aggregate_exec(
    input: Arc<dyn ExecutionPlan>,
    mode: AggregateMode,
    aggr_expr: &[Arc<dyn AggregateExpr>],
) -> Result<AggregateExec> {
    let filter_expr = vec![None; aggr_expr.len()];
    AggregateExec::try_new(
        mode,
        PhysicalGroupBy::default(),
        aggr_expr.to_vec(),
        filter_expr,
        input.clone(),
        input.schema(),
    )
}
