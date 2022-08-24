use std::{fmt, sync::Arc};

use datafusion::{
    catalog::catalog::CatalogProvider,
    execution::{context::SessionState, runtime_env::RuntimeEnv},
    prelude::{SessionConfig, SessionContext},
    scheduler::Scheduler,
};

#[derive(Clone)]
pub struct IsiphoSessionCfg {
    exec: Arc<Scheduler>,
    session_config: SessionConfig,
    runtime: Arc<RuntimeEnv>,
    catalog: Option<Arc<dyn CatalogProvider>>,
}

const SIZE: usize = 1000;

#[allow(dead_code)]
pub const DEFAULT_SCHEMA: &str = "public";
pub const DEFAULT_CATALOG: &str = "cnosdb";

impl IsiphoSessionCfg {
    pub(super) fn new(exec: Arc<Scheduler>, runtime: Arc<RuntimeEnv>) -> Self {
        let session_config = SessionConfig::new()
            .with_batch_size(SIZE)
            .with_information_schema(true);

        Self {
            exec,
            session_config,
            runtime,
            catalog: None,
        }
    }

    pub fn with_target_partitions(mut self, target_partitions: usize) -> Self {
        self.session_config = self
            .session_config
            .with_target_partitions(target_partitions);
        self
    }

    pub fn with_default_catalog(self, catalog: Arc<dyn CatalogProvider>) -> Self {
        Self {
            catalog: Some(catalog),
            ..self
        }
    }

    pub fn build(self) -> IsiphoSessionCtx {
        let state = SessionState::with_config_rt(self.session_config, self.runtime);

        let inner = SessionContext::with_state(state);

        if let Some(default_catalog) = self.catalog {
            inner.register_catalog(DEFAULT_CATALOG, default_catalog);
        }

        IsiphoSessionCtx {
            inner,
            exec: Some(self.exec),
        }
    }
}

#[allow(dead_code)]
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
    #[allow(dead_code)]
    pub fn set_cxt(&mut self, ctx: SessionContext) {
        self.inner = ctx
    }
}
