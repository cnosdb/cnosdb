use std::sync::Arc;

use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion::common::{Result as DFResult, ToDFSchema};
use datafusion::config::ConfigOptions;
use datafusion::execution::context::ExecutionProps;
use datafusion::logical_expr::col;
use datafusion::logical_expr::expr::Sort;
use datafusion::physical_expr::LexOrdering;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::aggregates::AggregateExec;
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::create_physical_sort_expr;

use crate::extension::physical::plan_node::tskv_exec::TskvExec;
use crate::extension::utils::downcast_execution_plan;

#[derive(Debug)]
#[non_exhaustive]
pub struct AddSortExec {}

impl AddSortExec {
    pub fn new() -> Self {
        Self {}
    }

    fn optimize_inner(&self, plan: Arc<dyn ExecutionPlan>) -> DFResult<Arc<dyn ExecutionPlan>> {
        plan.transform_up(|plan| {
            if let Some(tskv_exec) = downcast_execution_plan::<TskvExec>(plan.as_ref()) {
                let physical_sort_expr = create_physical_sort_expr(
                    &Sort::new(col("time"), true, false),
                    &tskv_exec.schema().to_dfschema()?,
                    &ExecutionProps::new(),
                )?;
                let sort_plan = Arc::new(
                    SortExec::new(
                        LexOrdering::new(vec![physical_sort_expr.clone()]),
                        plan.clone(),
                    )
                    .with_preserve_partitioning(true),
                );
                let sort_merge_plan = Arc::new(SortPreservingMergeExec::new(
                    LexOrdering::new(vec![physical_sort_expr]),
                    sort_plan,
                ));

                return Ok(Transformed::yes(sort_merge_plan));
            }

            Ok(Transformed::no(plan))
        })
        .data()
    }
}

impl Default for AddSortExec {
    fn default() -> Self {
        Self::new()
    }
}

impl PhysicalOptimizerRule for AddSortExec {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        plan.transform_up(|plan| {
            if let Some(agg_exec) = downcast_execution_plan::<AggregateExec>(plan.as_ref()) {
                let mut is_need_sort = false;
                agg_exec.aggr_expr().iter().for_each(|expr| {
                    let fun = expr.fun();
                    match fun.name() {
                        "stats_agg" | "gauge_agg" | "corr" | "covar_pop" | "stddev"
                        | "stddev_pop" | "var" | "var_pop" => {
                            is_need_sort = true;
                        }
                        _ => {}
                    }
                });

                if is_need_sort {
                    return Ok(Transformed::yes(self.optimize_inner(plan.clone())?));
                }
            }

            Ok(Transformed::no(plan))
        })
        .data()
    }

    fn name(&self) -> &str {
        "add_sort_exec"
    }

    fn schema_check(&self) -> bool {
        true
    }
}
