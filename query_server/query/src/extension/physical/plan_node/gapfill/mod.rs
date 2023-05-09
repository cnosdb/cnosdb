//! This module contains code that implements
//! a gap-filling extension to DataFusion

mod algo;
mod buffered_input;
mod params;
mod stream;

use std::fmt::{self, Debug};
use std::ops::{Bound, Range};
use std::sync::Arc;

use datafusion::arrow::compute::SortOptions;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::context::TaskContext;
use datafusion::execution::memory_pool::MemoryConsumer;
use datafusion::physical_expr::{PhysicalSortExpr, PhysicalSortRequirement};
use datafusion::physical_plan::expressions::Column;
use datafusion::physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet};
use datafusion::physical_plan::{
    DisplayFormatType, Distribution, ExecutionPlan, Partitioning, PhysicalExpr,
    SendableRecordBatchStream, Statistics,
};

use self::stream::GapFillStream;
use crate::extension::logical::plan_node::gapfill::FillStrategy;
use crate::extension::utils::{try_map_bound, try_map_range};

/// A physical node for the gap-fill operation.
pub struct GapFillExec {
    input: Arc<dyn ExecutionPlan>,
    // The group by expressions from the original aggregation node.
    group_expr: Vec<Arc<dyn PhysicalExpr>>,
    // The aggregate expressions from the original aggregation node.
    aggr_expr: Vec<Arc<dyn PhysicalExpr>>,
    // The sort expressions for the required sort order of the input:
    // all of the group exressions, with the time column being last.
    sort_expr: Vec<PhysicalSortExpr>,
    // Parameters (besides streaming data) to gap filling
    params: GapFillExecParams,
    /// Metrics reporting behavior during execution.
    metrics: ExecutionPlanMetricsSet,
}

#[derive(Clone, Debug)]
pub struct GapFillExecParams {
    /// The uniform interval of incoming timestamps
    stride: Arc<dyn PhysicalExpr>,
    /// The sliding interval of incoming timestamps
    sliding: Arc<dyn PhysicalExpr>,
    /// The timestamp column produced by date_bin
    time_column: Column,
    /// The origin argument from the all to TIME_WINDOW_GAPFILL
    origin: Option<Arc<dyn PhysicalExpr>>,
    /// The time range of source input to TIME_WINDOW_GAPFILL.
    /// Inferred from predicates in the overall query.
    time_range: Range<Bound<Arc<dyn PhysicalExpr>>>,
    /// What to do when filling aggregate columns.
    /// The 0th element in each tuple is the aggregate column.
    fill_strategy: Vec<(Arc<dyn PhysicalExpr>, FillStrategy)>,
}

impl GapFillExecParams {
    pub fn new(
        stride: Arc<dyn PhysicalExpr>,
        sliding: Arc<dyn PhysicalExpr>,
        time_column: Column,
        origin: Option<Arc<dyn PhysicalExpr>>,
        time_range: Range<Bound<Arc<dyn PhysicalExpr>>>,
        fill_strategy: Vec<(Arc<dyn PhysicalExpr>, FillStrategy)>,
    ) -> Self {
        Self {
            stride,
            sliding,
            time_column,
            origin,
            time_range,
            fill_strategy,
        }
    }
}

impl GapFillExec {
    pub fn try_new(
        input: Arc<dyn ExecutionPlan>,
        group_expr: Vec<Arc<dyn PhysicalExpr>>,
        aggr_expr: Vec<Arc<dyn PhysicalExpr>>,
        params: GapFillExecParams,
    ) -> Result<Self> {
        let sort_expr = {
            let mut sort_expr: Vec<_> = group_expr
                .iter()
                .map(|expr| PhysicalSortExpr {
                    expr: Arc::clone(expr),
                    options: SortOptions::default(),
                })
                .collect();

            // Ensure that the time column is the last component in the sort
            // expressions.
            let time_idx = group_expr
                .iter()
                .enumerate()
                .find(|(_i, e)| {
                    e.as_any()
                        .downcast_ref::<Column>()
                        .map_or(false, |c| c.index() == params.time_column.index())
                })
                .map(|(i, _)| i);

            if let Some(time_idx) = time_idx {
                let last_elem = sort_expr.len() - 1;
                sort_expr.swap(time_idx, last_elem);
            } else {
                return Err(DataFusionError::Internal(
                    "could not find time column for GapFillExec".to_string(),
                ));
            }

            sort_expr
        };

        Ok(Self {
            input,
            group_expr,
            aggr_expr,
            sort_expr,
            params,
            metrics: ExecutionPlanMetricsSet::new(),
        })
    }
}

impl Debug for GapFillExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "GapFillExec")
    }
}

impl ExecutionPlan for GapFillExec {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.input.schema()
    }

    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(1)
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        // It seems like it could be possible to partition on all the
        // group keys except for the time expression. For now, keep it simple.
        vec![Distribution::SinglePartition]
    }

    fn output_ordering(&self) -> Option<&[datafusion::physical_expr::PhysicalSortExpr]> {
        self.input.output_ordering()
    }

    fn required_input_ordering(&self) -> Vec<Option<Vec<PhysicalSortRequirement>>> {
        vec![Some(PhysicalSortRequirement::from_sort_exprs(
            &self.sort_expr,
        ))]
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![Arc::clone(&self.input)]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        match children.len() {
            1 => Ok(Arc::new(Self::try_new(
                Arc::clone(&children[0]),
                self.group_expr.clone(),
                self.aggr_expr.clone(),
                self.params.clone(),
            )?)),
            _ => Err(DataFusionError::Internal(
                "GapFillExec wrong number of children".to_string(),
            )),
        }
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Internal(format!(
                "GapFillExec invalid partition {partition}, there can be only one partition"
            )));
        }

        let baseline_metrics = BaselineMetrics::new(&self.metrics, partition);
        let output_batch_size = context.session_config().batch_size();
        let reservation = MemoryConsumer::new(format!("GapFillExec[{partition}]"))
            .register(context.memory_pool());
        let input_stream = self.input.execute(partition, context)?;
        Ok(Box::pin(GapFillStream::try_new(
            self,
            output_batch_size,
            input_stream,
            reservation,
            baseline_metrics,
        )?))
    }

    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match t {
            DisplayFormatType::Default => {
                let group_expr: Vec<_> = self.group_expr.iter().map(|e| e.to_string()).collect();
                let aggr_expr: Vec<_> = self
                    .params
                    .fill_strategy
                    .iter()
                    .map(|(e, fs)| match fs {
                        FillStrategy::PrevNullAsIntentional => {
                            format!("LOCF(null-as-intentional, {})", e)
                        }
                        FillStrategy::PrevNullAsMissing => format!("LOCF({})", e),
                        FillStrategy::LinearInterpolate => format!("INTERPOLATE({})", e),
                        FillStrategy::Null => e.to_string(),
                    })
                    .collect();
                let time_range = try_map_range(&self.params.time_range, |b| {
                    try_map_bound(b.as_ref(), |e| Ok(e.to_string()))
                })
                .map_err(|_| fmt::Error {})?;
                write!(
                    f,
                    "GapFillExec: group_expr=[{}], aggr_expr=[{}], stride={}, time_range={:?}",
                    group_expr.join(", "),
                    aggr_expr.join(", "),
                    self.params.stride,
                    time_range
                )
            }
        }
    }

    fn statistics(&self) -> Statistics {
        Statistics::default()
    }
}
