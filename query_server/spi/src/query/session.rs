use datafusion::{
    config::OPT_OPTIMIZER_SKIP_FAILED_RULES,
    execution::context,
    prelude::{SessionConfig, SessionContext},
};

use crate::service::protocol::Context;

#[derive(Clone)]
pub struct IsiphoSessionCtx {
    // todo
    // ...
    catalog: String,
    database: String,
    inner: SessionContext,
}

impl IsiphoSessionCtx {
    pub fn inner(&self) -> &SessionContext {
        &self.inner
    }

    pub fn catalog(&self) -> &str {
        &self.catalog
    }

    pub fn database(&self) -> &str {
        &self.database
    }
}

#[derive(Default)]
pub struct IsiphoSessionCtxFactory {
    // TODO global config
}

impl IsiphoSessionCtxFactory {
    pub fn create_isipho_session_ctx(&self, context: Context) -> IsiphoSessionCtx {
        let isipho_ctx = context.session_config().to_owned();
        // TODO Use global configuration as the default configuration for session
        let df_session_state = context::default_session_builder(isipho_ctx.inner);
        let df_session_ctx = SessionContext::with_state(df_session_state);

        IsiphoSessionCtx {
            catalog: context.user_info().to_owned().user,
            database: context.database().to_owned(),
            inner: df_session_ctx,
        }
    }
}

#[derive(Clone)]
pub struct IsiphoSessionConfig {
    inner: SessionConfig,
}

impl Default for IsiphoSessionConfig {
    fn default() -> Self {
        let mut inner: SessionConfig = Default::default();

        inner
            .config_options
            .set_bool(OPT_OPTIMIZER_SKIP_FAILED_RULES, false);

        Self { inner }
    }
}

impl IsiphoSessionConfig {
    pub fn to_df_config(&self) -> &SessionConfig {
        &self.inner
    }

    /// Customize target_partitions
    /// partition count must be greater than zero
    pub fn with_target_partitions(mut self, n: usize) -> Self {
        self.inner = self.inner.with_target_partitions(n);
        self
    }
}
