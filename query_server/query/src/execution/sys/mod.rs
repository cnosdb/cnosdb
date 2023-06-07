mod kill_query;
mod show_queries;

use std::sync::Arc;

use async_trait::async_trait;
use spi::query::dispatcher::{QueryInfo, QueryStatus};
use spi::query::execution::{Output, QueryExecution, QueryStateMachineRef};
use spi::query::logical_planner::SYSPlan;
use spi::Result;

use self::kill_query::KillQueryTask;
use self::show_queries::ShowQueriesTask;
use crate::dispatcher::query_tracker::QueryTracker;

pub struct SystemExecution {
    task_factory: SystemTaskFactory,
    state_machine: QueryStateMachineRef,
}

impl SystemExecution {
    pub fn new(
        state_machine: QueryStateMachineRef,
        plan: SYSPlan,
        query_tracker: Arc<QueryTracker>,
    ) -> Self {
        Self {
            task_factory: SystemTaskFactory {
                plan,
                query_tracker,
            },
            state_machine,
        }
    }
}

#[async_trait]
impl QueryExecution for SystemExecution {
    // start
    async fn start(&self) -> Result<Output> {
        let query_state_machine = self.state_machine.clone();

        query_state_machine.begin_schedule();

        let result = self
            .task_factory
            .create_task()
            .execute(query_state_machine.clone())
            .await;

        query_state_machine.end_schedule();

        result
    }
    // 停止
    fn cancel(&self) -> Result<()> {
        Ok(())
    }
    // 静态信息
    fn info(&self) -> QueryInfo {
        let qsm = &self.state_machine;
        QueryInfo::new(
            qsm.query_id,
            qsm.query.content().to_string(),
            *qsm.session.tenant_id(),
            qsm.session.tenant().to_string(),
            qsm.session.user().desc().clone(),
        )
    }
    // 运行时信息
    fn status(&self) -> QueryStatus {
        QueryStatus::new(
            self.state_machine.state().clone(),
            self.state_machine.duration(),
        )
    }
}

/// Traits that system tasks should implement
#[async_trait]
trait SystemTask: Send + Sync {
    async fn execute(&self, query_state_machine: QueryStateMachineRef) -> Result<Output>;
}

struct SystemTaskFactory {
    plan: SYSPlan,
    query_tracker: Arc<QueryTracker>,
}

impl SystemTaskFactory {
    fn create_task(&self) -> Box<dyn SystemTask> {
        match &self.plan {
            SYSPlan::ShowQueries => Box::new(ShowQueriesTask::new(self.query_tracker.clone())),
            SYSPlan::KillQuery(query_id) => {
                Box::new(KillQueryTask::new(self.query_tracker.clone(), *query_id))
            }
        }
    }
}
