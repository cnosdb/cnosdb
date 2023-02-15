use std::sync::Arc;

use async_trait::async_trait;
use datafusion::logical_expr::LogicalPlan;
use datafusion::physical_plan::ExecutionPlan;

use super::session::IsiphoSessionCtx;
use crate::Result;

#[async_trait]
pub trait Optimizer {
    async fn optimize(
        &self,
        plan: &LogicalPlan,
        session: &IsiphoSessionCtx,
    ) -> Result<Arc<dyn ExecutionPlan>>;
}
