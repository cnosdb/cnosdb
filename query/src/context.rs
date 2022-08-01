use std::{fmt, result, sync::Arc};

use datafusion::{
    catalog::catalog::CatalogProvider,
    common::DataFusionError,
    execution::{context::SessionState, runtime_env::RuntimeEnv},
    prelude::{SessionConfig, SessionContext},
    scheduler::Scheduler,
};

pub type Result<T> = result::Result<T, DataFusionError>;

#[derive(Clone)]
pub struct IsiphoSessionCfg {
    exec: Arc<Scheduler>,
    session_config: SessionConfig,
    runtime: Arc<RuntimeEnv>,
    catalog: Option<Arc<dyn CatalogProvider>>,
}
const SIZE: usize = 1000;
pub const DEFAULT_CATALOG: &str = "cnosdb";
pub const DEFAULT_SCHEMA: &str = "public";

impl IsiphoSessionCfg {
    pub(super) fn new(exec: Arc<Scheduler>, runtime: Arc<RuntimeEnv>) -> Self {
        let session_config =
            SessionConfig::new().with_batch_size(SIZE).with_information_schema(true);

        Self { exec, session_config, runtime, catalog: None }
    }

    pub fn with_target_partitions(mut self, target_partitions: usize) -> Self {
        self.session_config = self.session_config.with_target_partitions(target_partitions);
        self
    }

    pub fn with_default_catalog(self, catalog: Arc<dyn CatalogProvider>) -> Self {
        Self { catalog: Some(catalog), ..self }
    }

    pub fn build(self) -> IsiphoSessionCtx {
        let state = SessionState::with_config_rt(self.session_config, self.runtime);

        let inner = SessionContext::with_state(state);

        if let Some(default_catalog) = self.catalog {
            inner.register_catalog(DEFAULT_CATALOG, default_catalog);
        }

        IsiphoSessionCtx { inner, exec: Some(self.exec) }
    }
}

#[derive(Default)]
pub struct IsiphoSessionCtx {
    inner: SessionContext,
    exec: Option<Arc<Scheduler>>,
}

impl fmt::Debug for IsiphoSessionCtx {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("IsiophoSessionCtx")
         .field("inner", &"<DataFusion ExecutionContext>")
         .finish()
    }
}

impl IsiphoSessionCtx {
    pub fn inner(&self) -> &SessionContext {
        &self.inner
    }

    // for test
    pub fn set_cxt(&mut self, ctx: SessionContext) {
        self.inner = ctx
    }
}
