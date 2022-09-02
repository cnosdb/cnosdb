use std::sync::Arc;

use async_trait::async_trait;
use datafusion::{physical_plan::SendableRecordBatchStream, scheduler::Scheduler};
use snafu::ResultExt;
use spi::query::execution::Output;
use spi::query::{
    execution::{QueryExecution, QueryStateMachineRef},
    logical_planner::QueryPlan,
    optimizer::Optimizer,
    ScheduleSnafu,
};

use spi::query::Result;
pub struct SqlQueryExecution {
    query_state_machine: QueryStateMachineRef,
    plan: QueryPlan,
    optimizer: Arc<dyn Optimizer + Send + Sync>,
    scheduler: Arc<Scheduler>,
}

impl SqlQueryExecution {
    pub fn new(
        query_state_machine: QueryStateMachineRef,
        plan: QueryPlan,
        optimizer: Arc<dyn Optimizer + Send + Sync>,
        scheduler: Arc<Scheduler>,
    ) -> Self {
        Self {
            query_state_machine,
            plan,
            optimizer,
            scheduler,
        }
    }
}

#[async_trait]
impl QueryExecution for SqlQueryExecution {
    async fn start(&self) -> Result<Output> {
        // begin optimize
        self.query_state_machine.begin_optimize();
        let optimized_physical_plan = self
            .optimizer
            .optimize(&self.plan.df_plan, &self.query_state_machine.session)
            .await?;
        self.query_state_machine.end_optimize();

        // begin schedule
        self.query_state_machine.begin_schedule();
        let execution_result = self
            .scheduler
            .schedule(
                optimized_physical_plan,
                self.query_state_machine.session.inner().task_ctx(),
            )
            .context(ScheduleSnafu)?
            .stream();
        self.query_state_machine.end_schedule();

        Ok(Output::StreamData(execution_result))
    }
}
