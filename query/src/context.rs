use crate::catalog::CatalogRef;
use crate::extension::datafusion::expr::{self, func_manager::DFSessionContextFuncAdapter};
use datafusion::{
    execution::{context::SessionState, runtime_env::RuntimeEnv},
    prelude::{SessionConfig, SessionContext},
};
use std::{fmt, sync::Arc};
use trace::warn;

#[derive(Clone)]
pub struct IsiphoSessionCfg {
    session_config: SessionConfig,
    runtime: Arc<RuntimeEnv>,
}

const SIZE: usize = 1000;

#[allow(dead_code)]
pub const DEFAULT_SCHEMA: &str = "public";
pub const DEFAULT_CATALOG: &str = "cnosdb";

impl IsiphoSessionCfg {
    pub(super) fn new(runtime: Arc<RuntimeEnv>) -> Self {
        let session_config = SessionConfig::new()
            .with_batch_size(SIZE)
            .with_information_schema(true);

        Self {
            session_config,
            runtime,
        }
    }

    pub fn with_target_partitions(mut self, target_partitions: usize) -> Self {
        self.session_config = self
            .session_config
            .with_target_partitions(target_partitions);
        self
    }

    pub fn build(self, tenant: String, catalog: CatalogRef) -> IsiphoSessionCtx {
        let state = SessionState::with_config_rt(self.session_config, self.runtime);

        let mut inner = SessionContext::with_state(state);
        // temporary(database level): wrap SessionContext into function meta manager
        let mut func_manager = DFSessionContextFuncAdapter::new(&mut inner);
        // temporary(database level): register function to function meta manager
        if let Err(e) = expr::load_all_functions(&mut func_manager) {
            warn!("Failed to load consdb's built-in function. err: {}", e);
        };

        inner.register_catalog(tenant, catalog);

        IsiphoSessionCtx { inner }
    }
}

#[allow(dead_code)]
pub struct IsiphoSessionCtx {
    inner: SessionContext,
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
