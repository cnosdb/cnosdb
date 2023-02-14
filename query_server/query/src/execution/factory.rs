use std::sync::Arc;

use spi::query::execution::{QueryExecution, QueryExecutionFactory, QueryStateMachineRef};
use spi::query::logical_planner::Plan;
use spi::query::optimizer::Optimizer;
use spi::query::scheduler::SchedulerRef;

use super::query::SqlQueryExecution;
use super::sys::SystemExecution;
use crate::dispatcher::query_tracker::QueryTracker;
use crate::execution::ddl::DDLExecution;

pub struct SqlQueryExecutionFactory {
    optimizer: Arc<dyn Optimizer + Send + Sync>,
    scheduler: SchedulerRef,
    query_tracker: Arc<QueryTracker>,
}

impl SqlQueryExecutionFactory {
    #[inline(always)]
    pub fn new(
        optimizer: Arc<dyn Optimizer + Send + Sync>,
        scheduler: SchedulerRef,
        query_tracker: Arc<QueryTracker>,
    ) -> Self {
        Self {
            optimizer,
            scheduler,
            query_tracker,
        }
    }
}

impl QueryExecutionFactory for SqlQueryExecutionFactory {
    fn create_query_execution(
        &self,
        plan: Plan,
        state_machine: QueryStateMachineRef,
    ) -> Arc<dyn QueryExecution> {
        match plan {
            Plan::Query(query_plan) => Arc::new(SqlQueryExecution::new(
                state_machine,
                query_plan,
                self.optimizer.clone(),
                self.scheduler.clone(),
            )),
            Plan::DDL(ddl_plan) => Arc::new(DDLExecution::new(state_machine, ddl_plan)),
            Plan::SYSTEM(sys_plan) => Arc::new(SystemExecution::new(
                state_machine,
                sys_plan,
                self.query_tracker.clone(),
            )),
        }
    }
}
