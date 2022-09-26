use std::sync::Arc;

use async_trait::async_trait;
use datafusion::{logical_plan::LogicalPlan, physical_plan::ExecutionPlan};

use super::{session::IsiphoSessionCtx, Result};

#[async_trait]
pub trait Optimizer {
    async fn optimize(
        &self,
        plan: &LogicalPlan,
        session: &IsiphoSessionCtx,
    ) -> Result<Arc<dyn ExecutionPlan>>;
}
