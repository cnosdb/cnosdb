use datafusion::{
    error::DataFusionError,
    logical_expr::LogicalPlan,
    optimizer::{OptimizerConfig, OptimizerRule},
};

use datafusion::error::Result;

pub struct RejectCrossJoin {}

impl OptimizerRule for RejectCrossJoin {
    fn optimize(
        &self,
        plan: &LogicalPlan,
        optimizer_config: &mut OptimizerConfig,
    ) -> Result<LogicalPlan> {
        if let LogicalPlan::CrossJoin(_) = plan {
            return Err(DataFusionError::NotImplemented("cross join".to_string()));
        }

        datafusion::optimizer::utils::optimize_children(self, plan, optimizer_config)
    }

    fn name(&self) -> &str {
        "reject_cross_join"
    }
}
