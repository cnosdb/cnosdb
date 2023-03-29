use std::sync::Arc;

use async_trait::async_trait;
use datafusion::error::DataFusionError;
use datafusion::execution::context::TaskContext;
use datafusion::physical_plan::{execute_stream, ExecutionPlan};
use spi::query::scheduler::{ExecutionResults, Scheduler};

pub struct LocalScheduler {}

#[async_trait]
impl Scheduler for LocalScheduler {
    async fn schedule(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        context: Arc<TaskContext>,
    ) -> Result<ExecutionResults, DataFusionError> {
        let stream = execute_stream(plan, context)?;

        Ok(ExecutionResults::new(stream))
    }
}
