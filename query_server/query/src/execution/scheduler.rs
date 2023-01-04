use std::sync::Arc;

use datafusion::{
    common::Result,
    execution::context::TaskContext,
    physical_plan::{coalesce_partitions::CoalescePartitionsExec, ExecutionPlan},
};
use spi::query::scheduler::{ExecutionResults, Scheduler};

pub struct LocalScheduler {}

impl Scheduler for LocalScheduler {
    fn schedule(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        context: Arc<TaskContext>,
    ) -> Result<ExecutionResults> {
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
