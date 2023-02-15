use std::sync::Arc;

use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::ExecutionPlan;
use spi::query::session::IsiphoSessionCtx;
use spi::Result;

pub trait PhysicalOptimizer {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        session: &IsiphoSessionCtx,
    ) -> Result<Arc<dyn ExecutionPlan>>;

    fn inject_optimizer_rule(
        &mut self,
        optimizer_rule: Arc<dyn PhysicalOptimizerRule + Send + Sync>,
    );
}
