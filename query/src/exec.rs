use std::sync::Arc;

use datafusion::execution::runtime_env::{RuntimeConfig, RuntimeEnv};

use crate::{
    context::{IsiophoSessionCtx, IsiphoSessionCfg},
    executor::DedicatedExecutor,
};

#[derive(Debug, Clone)]
pub struct ExecutorConfig {
    pub num_threads: usize,
    pub query_partitions: usize,
}

#[derive(Debug)]
pub struct Executor {
    query_exec: DedicatedExecutor,
    config: ExecutorConfig,
    runtime: Arc<RuntimeEnv>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecutorType {
    Query,
}

impl Executor {
    pub fn new(num_threads: usize) -> Self {
        Self::new_with_config(ExecutorConfig { num_threads, query_partitions: num_threads })
    }

    pub fn new_with_config(config: ExecutorConfig) -> Self {
        let query_exec = DedicatedExecutor::new("Query Executor Thread", config.num_threads);
        let runtime_config = RuntimeConfig::new();
        let runtime = Arc::new(RuntimeEnv::new(runtime_config).expect("creating runtime"));

        Self { query_exec, config, runtime }
    }

    pub fn new_execution_config(&self, executor_type: ExecutorType) -> IsiphoSessionCfg {
        let exec = self.executor(executor_type).clone();
        IsiphoSessionCfg::new(exec, Arc::clone(&self.runtime))
            .with_target_partitions(self.config.query_partitions)
    }

    pub fn new_context(&self, executor_type: ExecutorType) -> IsiophoSessionCtx {
        self.new_execution_config(executor_type).build()
    }

    fn executor(&self, executor_type: ExecutorType) -> &DedicatedExecutor {
        match executor_type {
            ExecutorType::Query => &self.query_exec,
        }
    }

    pub fn shutdown(&self) {
        self.query_exec.shutdown();
    }

    pub async fn join(&self) {
        self.query_exec.join().await;
    }
}
