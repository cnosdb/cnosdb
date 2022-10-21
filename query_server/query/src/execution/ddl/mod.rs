use async_trait::async_trait;

use spi::query::execution::{Output, QueryExecution, QueryStateMachineRef};
use spi::query::logical_planner::DDLPlan;
use spi::query::{self, QueryError};

use spi::query::execution::ExecutionError;

use self::create_table::CreateTableTask;
use crate::execution::ddl::create_database::CreateDatabaseTask;
use crate::execution::ddl::describe_database::DescribeDatabaseTask;
use crate::execution::ddl::describe_table::DescribeTableTask;
use crate::execution::ddl::show_database::ShowDatabaseTask;
use crate::execution::ddl::show_table::ShowTableTask;
use snafu::ResultExt;

use self::create_external_table::CreateExternalTableTask;
use self::drop_object::DropObjectTask;

mod create_database;
mod create_external_table;
mod create_table;
mod describe_database;
mod describe_table;
mod drop_object;
mod show_database;
mod show_table;

/// Traits that DDL tasks should implement
#[async_trait]
trait DDLDefinitionTask: Send + Sync {
    async fn execute(
        &self,
        query_state_machine: QueryStateMachineRef,
    ) -> Result<Output, ExecutionError>;
}

pub struct DDLExecution {
    task_factory: DDLDefinitionTaskFactory,
    query_state_machine: QueryStateMachineRef,
}

impl DDLExecution {
    pub fn new(query_state_machine: QueryStateMachineRef, plan: DDLPlan) -> Self {
        Self {
            task_factory: DDLDefinitionTaskFactory { plan },
            query_state_machine,
        }
    }
}

#[async_trait]
impl QueryExecution for DDLExecution {
    // execute ddl task
    // This logic usually does not change
    async fn start(&self) -> Result<Output, QueryError> {
        let query_state_machine = self.query_state_machine.clone();

        self.task_factory
            .create_task()
            .execute(query_state_machine)
            .await
            .context(query::ExecutionSnafu)
    }
}

struct DDLDefinitionTaskFactory {
    plan: DDLPlan,
}

impl DDLDefinitionTaskFactory {
    // According to different statement types, construct the corresponding task
    // If you add ddl operations, you usually need to modify here
    fn create_task(&self) -> Box<dyn DDLDefinitionTask> {
        match &self.plan {
            DDLPlan::CreateExternalTable(stmt) => {
                Box::new(CreateExternalTableTask::new(stmt.clone()))
            }
            DDLPlan::Drop(stmt) => Box::new(DropObjectTask::new(stmt.clone())),
            DDLPlan::CreateTable(stmt) => Box::new(CreateTableTask::new(stmt.clone())),
            DDLPlan::CreateDatabase(stmt) => Box::new(CreateDatabaseTask::new(stmt.clone())),
            DDLPlan::DescribeDatabase(stmt) => Box::new(DescribeDatabaseTask::new(stmt.clone())),
            DDLPlan::DescribeTable(stmt) => Box::new(DescribeTableTask::new(stmt.clone())),
            DDLPlan::ShowTable(stmt) => Box::new(ShowTableTask::new(stmt.to_string())),
            DDLPlan::ShowDatabase() => Box::new(ShowDatabaseTask::new()),
        }
    }
}
