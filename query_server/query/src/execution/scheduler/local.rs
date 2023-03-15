use std::sync::Arc;

use async_trait::async_trait;
use datafusion::error::DataFusionError;
use datafusion::execution::context::TaskContext;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::ExecutionPlan;
use spi::query::scheduler::{ExecutionResults, Scheduler};
use trace::info;

pub struct LocalScheduler {}

#[async_trait]
impl Scheduler for LocalScheduler {
    async fn schedule(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        context: Arc<TaskContext>,
    ) -> Result<ExecutionResults, DataFusionError> {
        info!("Init local executor of query engine.");

        let partition_count = plan.output_partitioning().partition_count();

        let merged_plan = if partition_count > 1 {
            Arc::new(CoalescePartitionsExec::new(plan))
        } else {
            plan
        };

        debug_assert_eq!(1, merged_plan.output_partitioning().partition_count());

        let stream = merged_plan.execute(0, context)?;

        Ok(ExecutionResults::new(stream))
    }
}
