use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::LogicalPlan;
use datafusion::optimizer::{OptimizerConfig, OptimizerRule};

#[derive(Debug)]
pub struct RejectCrossJoin {}

impl OptimizerRule for RejectCrossJoin {
    fn rewrite(
        &self,
        plan: LogicalPlan,
        _optimizer_config: &dyn OptimizerConfig,
    ) -> DFResult<Transformed<LogicalPlan>> {
        plan.map_children(|child| self.rewrite(child, _optimizer_config))
    }

    fn name(&self) -> &str {
        "reject_cross_join"
    }
}
