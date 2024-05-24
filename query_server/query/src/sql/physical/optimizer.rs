use std::sync::Arc;

use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::ExecutionPlan;
use spi::query::session::SessionCtx;
use spi::QueryResult;

pub trait PhysicalOptimizer {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        session: &SessionCtx,
    ) -> QueryResult<Arc<dyn ExecutionPlan>>;

    fn inject_optimizer_rule(
        &mut self,
        optimizer_rule: Arc<dyn PhysicalOptimizerRule + Send + Sync>,
    );
}
