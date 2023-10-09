mod delete_from_table;

use async_trait::async_trait;
use spi::query::dispatcher::{QueryInfo, QueryStatus};
use spi::query::execution::{Output, QueryExecution, QueryStateMachineRef};
use spi::query::logical_planner::DMLPlan;
use spi::Result;

use self::delete_from_table::DeleteFromTableTask;

/// Traits that DML tasks should implement
#[async_trait]
trait DMLDefinitionTask: Send + Sync {
    async fn execute(&self, query_state_machine: QueryStateMachineRef) -> Result<Output>;
}

pub struct DMLExecution {
    task_factory: DMLDefinitionTaskFactory,
    query_state_machine: QueryStateMachineRef,
}

impl DMLExecution {
    pub fn new(query_state_machine: QueryStateMachineRef, plan: DMLPlan) -> Self {
        Self {
            task_factory: DMLDefinitionTaskFactory { plan },
            query_state_machine,
        }
    }
}

#[async_trait]
impl QueryExecution for DMLExecution {
    // execute DML task
    // This logic usually does not change
    async fn start(&self) -> Result<Output> {
        let query_state_machine = &self.query_state_machine;

        query_state_machine.begin_schedule();

        let _span_recorder = self
            .query_state_machine
            .session
            .get_child_span_recorder("execute DML");

        let result = self
            .task_factory
            .create_task()
            .execute(query_state_machine.clone())
            .await;

        query_state_machine.end_schedule();

        result
    }

    fn cancel(&self) -> Result<()> {
        // DML ignore
        Ok(())
    }

    fn info(&self) -> QueryInfo {
        let qsm = &self.query_state_machine;
        QueryInfo::new(
            qsm.query_id,
            qsm.query.content().to_string(),
            *qsm.session.tenant_id(),
            qsm.session.tenant().to_string(),
            qsm.session.user().desc().clone(),
        )
    }

    fn status(&self) -> QueryStatus {
        QueryStatus::new(
            self.query_state_machine.state().clone(),
            self.query_state_machine.duration(),
        )
    }
}

struct DMLDefinitionTaskFactory {
    plan: DMLPlan,
}

impl DMLDefinitionTaskFactory {
    // According to different statement types, construct the corresponding task
    // If you add DML operations, you usually need to modify here
    fn create_task(&self) -> Box<dyn DMLDefinitionTask> {
        match &self.plan {
            DMLPlan::DeleteFromTable(sub_plan) => {
                Box::new(DeleteFromTableTask::new(sub_plan.clone()))
            }
        }
    }
}
