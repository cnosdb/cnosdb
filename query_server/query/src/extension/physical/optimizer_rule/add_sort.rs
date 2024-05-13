use std::sync::Arc;

use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{Result as DFResult, ToDFSchema};
use datafusion::config::ConfigOptions;
use datafusion::execution::context::ExecutionProps;
use datafusion::logical_expr::expr::Sort;
use datafusion::logical_expr::{col, Expr};
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::aggregates::AggregateExec;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::create_physical_sort_expr;

use crate::extension::utils::downcast_execution_plan;

#[non_exhaustive]
pub struct AddSortExec {}

impl AddSortExec {
    pub fn new() -> Self {
        Self {}
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
        plan.transform_up(&|plan| {
            if let Some(agg_exec) = downcast_execution_plan::<AggregateExec>(plan.as_ref()) {
                let mut is_need_sort = false;
                agg_exec.aggr_expr().iter().for_each(|expr| {
                    if !expr.support_concurrency() {
                        is_need_sort = true;
                    }
                });
                if is_need_sort {
                    let physical_sort_expr = create_physical_sort_expr(
                        &Expr::Sort(Sort::new(Box::new(col("time")), true, false)),
                        &agg_exec.input().schema().to_dfschema()?,
                        &agg_exec.input().schema(),
                        &ExecutionProps::new(),
                    )?;
                    let input = Arc::new(datafusion::physical_plan::sorts::sort::SortExec::new(
                        vec![physical_sort_expr],
                        agg_exec.input().clone(),
                    ));
                    let plan = Arc::new(AggregateExec::try_new(
                        *agg_exec.mode(),
                        agg_exec.group_expr().clone(),
                        agg_exec.aggr_expr().to_vec(),
                        agg_exec.filter_expr().to_vec(),
                        agg_exec.order_by_expr().to_vec(),
                        input.clone(),
                        input.schema(),
                    )?);
                    return Ok(Transformed::Yes(plan));
                }
            }

            Ok(Transformed::No(plan))
        })
    }

    fn name(&self) -> &str {
        "add_sort_exec"
    }

    fn schema_check(&self) -> bool {
        true
    }
}
