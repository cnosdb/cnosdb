use std::sync::Arc;

use async_trait::async_trait;
use datafusion::logical_expr::LogicalPlan;
use datafusion::physical_plan::planner::ExtensionPlanner;
use datafusion::physical_plan::ExecutionPlan;

use super::session::SessionCtx;
use crate::Result;

#[async_trait]
pub trait PhysicalPlanner {
    /// Given a `LogicalPlan`, create an `ExecutionPlan` suitable for execution
    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        session_state: &SessionCtx,
    ) -> Result<Arc<dyn ExecutionPlan>>;

    fn inject_physical_transform_rule(&mut self, rule: Arc<dyn ExtensionPlanner + Send + Sync>);
}
