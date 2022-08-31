use std::sync::Arc;

use datafusion::scheduler::Scheduler;
use spi::{
    catalog::factory::CatalogManager,
    query::{
        execution::{QueryExecution, QueryExecutionFactory, QueryStateMachineRef},
        logical_planner::Plan,
        optimizer::Optimizer,
    },
};

use super::query::SqlQueryExecution;

pub struct SqlQueryExecutionFactory {
    catalog_manager: Arc<dyn CatalogManager + Send + Sync>,
    // TODO access control
    // cnosdb optimizer
    optimizer: Arc<dyn Optimizer + Send + Sync>,
    // TODO 需要封装 scheduler
    scheduler: Arc<Scheduler>,
}

impl SqlQueryExecutionFactory {
    #[inline(always)]
    pub fn new(
        optimizer: Arc<dyn Optimizer + Send + Sync>,
        scheduler: Arc<Scheduler>,
        catalog_manager: Arc<dyn CatalogManager + Send + Sync>,
    ) -> Self {
        Self {
            optimizer,
            scheduler,
            catalog_manager,
        }
    }
}

impl QueryExecutionFactory for SqlQueryExecutionFactory {
    fn create_query_execution(
        &self,
        plan: Plan,
        state_machine: QueryStateMachineRef,
    ) -> Box<dyn QueryExecution> {
        match plan {
            Plan::Query(query_plan) => Box::new(SqlQueryExecution::new(
                state_machine,
                query_plan,
                self.optimizer.clone(),
                self.scheduler.clone(),
            )),
            Plan::Drop(_) => todo!(),
        }
    }
}
