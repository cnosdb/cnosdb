use std::{result, sync::Arc};

use datafusion::{
    common::DataFusionError,
    execution::{
        context::TaskContext,
        runtime_env::{RuntimeConfig, RuntimeEnv},
    },
    physical_plan::ExecutionPlan,
    scheduler::{ExecutionResults, Scheduler},
};

use crate::context::IsiphoSessionCfg;
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
        Self::new_with_config(ExecutorConfig {
            num_threads,
            query_partitions: num_threads,
        })
    }

    pub fn new_with_config(config: ExecutorConfig) -> Self {
        let query_exec = Arc::new(Scheduler::new(config.num_threads));
        let runtime_config = RuntimeConfig::new();
        let runtime = Arc::new(RuntimeEnv::new(runtime_config).expect("creating runtime"));

        Self {
            query_exec,
            config,
            runtime,
        }
    }

    pub fn new_execution_config(&self) -> IsiphoSessionCfg {
        IsiphoSessionCfg::new(Arc::clone(&self.runtime))
            .with_target_partitions(self.config.query_partitions)
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
