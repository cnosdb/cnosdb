use std::sync::Arc;

use datafusion::logical_expr::{Extension, LogicalPlan};
use models::runtime::executor::DedicatedExecutor;
use spi::query::config::StreamTriggerInterval;
use spi::query::datasource::stream::checker::StreamCheckerManagerRef;
use spi::query::execution::{QueryExecutionFactory, QueryExecutionRef, QueryStateMachineRef};
use spi::query::logical_planner::{Plan, QueryPlan};
use spi::query::optimizer::Optimizer;
use spi::query::scheduler::SchedulerRef;

use super::query::SqlQueryExecution;
use super::stream::trigger::executor::{TriggerExecutorFactory, TriggerExecutorFactoryRef};
use super::stream::MicroBatchStreamExecution;
use super::sys::SystemExecution;
use crate::dispatcher::query_tracker::QueryTracker;
use crate::execution::ddl::DDLExecution;
use crate::extension::logical::plan_node::table_writer_merge::TableWriterMergePlanNode;
use crate::extension::logical::utils::extract_stream_providers;
use crate::extension::utils::downcast_plan_node;

pub struct SqlQueryExecutionFactory {
    optimizer: Arc<dyn Optimizer + Send + Sync>,
    scheduler: SchedulerRef,
    query_tracker: Arc<QueryTracker>,
    trigger_executor_factory: TriggerExecutorFactoryRef,
    runtime: Arc<DedicatedExecutor>,
    stream_checker_manager: StreamCheckerManagerRef,
}

impl SqlQueryExecutionFactory {
    #[inline(always)]
    pub fn new(
        optimizer: Arc<dyn Optimizer + Send + Sync>,
        scheduler: SchedulerRef,
        query_tracker: Arc<QueryTracker>,
        stream_checker_manager: StreamCheckerManagerRef,
    ) -> Self {
        // TODO configurable
        // Only do periodic scheduling, no need for many threads
        let trigger_executor_runtime = DedicatedExecutor::new("stream-trigger", 4);
        let trigger_executor_factory = Arc::new(TriggerExecutorFactory::new(Arc::new(
            trigger_executor_runtime,
        )));

        // TODO configurable
        // perform stream-related preparations, not actual operator execution
        let runtime = Arc::new(DedicatedExecutor::new("stream-executor", num_cpus::get()));

        Self {
            optimizer,
            scheduler,
            query_tracker,
            trigger_executor_factory,
            runtime,
            stream_checker_manager,
        }
    }
}

impl QueryExecutionFactory for SqlQueryExecutionFactory {
    fn create_query_execution(
        &self,
        plan: Plan,
        state_machine: QueryStateMachineRef,
    ) -> QueryExecutionRef {
        match plan {
            Plan::Query(query_plan) => {
                // 获取执行计划中所有涉及到的stream source
                let stream_providers = extract_stream_providers(&query_plan);

                // 纯批操作
                // 1. 没有stream source
                // 2. explain
                // 3. 非dml
                if stream_providers.is_empty() || query_plan.is_explain() || !is_dml(&query_plan) {
                    return Arc::new(SqlQueryExecution::new(
                        state_machine,
                        query_plan,
                        self.optimizer.clone(),
                        self.scheduler.clone(),
                    ));
                }

                // 流操作
                let stream_trigger_interval = state_machine
                    .session
                    .inner()
                    .state()
                    .config()
                    .get_extension::<StreamTriggerInterval>()
                    .unwrap_or_else(|| Arc::new(StreamTriggerInterval::Once));
                Arc::new(MicroBatchStreamExecution::new(
                    state_machine,
                    Arc::new(query_plan),
                    stream_providers,
                    self.scheduler.clone(),
                    self.trigger_executor_factory
                        .create(stream_trigger_interval.as_ref()),
                    self.runtime.clone(),
                ))
            }
            Plan::DDL(ddl_plan) => Arc::new(DDLExecution::new(
                state_machine,
                self.stream_checker_manager.clone(),
                ddl_plan,
            )),
            Plan::SYSTEM(sys_plan) => Arc::new(SystemExecution::new(
                state_machine,
                sys_plan,
                self.query_tracker.clone(),
            )),
        }
    }
}

fn is_dml(query_plan: &QueryPlan) -> bool {
    match &query_plan.df_plan {
        LogicalPlan::Dml(_) => true,
        LogicalPlan::Extension(Extension { node }) => {
            downcast_plan_node::<TableWriterMergePlanNode>(node.as_ref()).is_some()
        }
        _ => false,
    }
}

impl Drop for SqlQueryExecutionFactory {
    fn drop(&mut self) {
        self.runtime.shutdown();
    }
}
