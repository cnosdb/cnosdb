use std::sync::Arc;

use async_trait::async_trait;
use datafusion::error::DataFusionError;
use datafusion::execution::context::TaskContext;
use datafusion::physical_plan::ExecutionPlan;
use spi::query::scheduler::{ExecutionResults, Scheduler};
use trace::debug;

pub struct LocalScheduler {}

#[async_trait]
impl Scheduler for LocalScheduler {
    async fn schedule(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        context: Arc<TaskContext>,
    ) -> Result<ExecutionResults, DataFusionError> {
        debug!("Init local executor of query engine.");
        let stream = plan.execute(0, context)?;
        Ok(ExecutionResults::new(stream))
    }
}
