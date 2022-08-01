use std::result;
use std::sync::Arc;
use datafusion::common::DataFusionError;
use datafusion::execution::context::TaskContext;

use datafusion::execution::runtime_env::{RuntimeConfig, RuntimeEnv};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::scheduler::{ExecutionResults, Scheduler};

use crate::{
    context::{IsiphoSessionCtx, IsiphoSessionCfg},
};
pub type Result<T> = result::Result<T, DataFusionError>;


#[derive(Debug, Clone)]
pub struct ExecutorConfig {
    pub num_threads: usize,
    pub query_partitions: usize,
}


pub struct Executor {
    query_exec: Arc<Scheduler>,
    config: ExecutorConfig,
    runtime: Arc<RuntimeEnv>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum ExecutorType {
    Query,
}

impl Executor {
    pub fn new(num_threads: usize) -> Self {
        Self::new_with_config(ExecutorConfig { num_threads, query_partitions: num_threads })
    }

    pub fn new_with_config(config: ExecutorConfig) -> Self {
        let query_exec = Arc::new(Scheduler::new(config.num_threads));
        let runtime_config = RuntimeConfig::new();
        let runtime = Arc::new(RuntimeEnv::new(runtime_config).expect("creating runtime"));

        Self { query_exec, config, runtime }
    }

    pub fn new_execution_config(&self, executor_type: ExecutorType) -> IsiphoSessionCfg {
        let exec = self.executor(executor_type).clone();
        IsiphoSessionCfg::new(exec, Arc::clone(&self.runtime))
            .with_target_partitions(self.config.query_partitions)
    }

    pub fn new_context(&self, executor_type: ExecutorType) -> IsiphoSessionCtx {
        self.new_execution_config(executor_type).build()
    }

    fn executor(&self, executor_type: ExecutorType) -> Arc<Scheduler> {
        match executor_type {
            ExecutorType::Query => self.query_exec.clone(),
        }
    }
    pub fn run(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        context: Arc<TaskContext>,
    ) -> Result<ExecutionResults> {
        self.query_exec.schedule(plan, context)
    }
}
