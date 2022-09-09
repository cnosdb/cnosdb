use std::sync::Arc;

use datafusion::{
    catalog::catalog::CatalogProvider,
    config::OPT_OPTIMIZER_SKIP_FAILED_RULES,
    execution::context,
    prelude::{SessionConfig, SessionContext},
};

#[derive(Clone)]
pub struct IsiphoSessionCtx {
    inner: SessionContext,
}

impl IsiphoSessionCtx {
    pub fn inner(&self) -> &SessionContext {
        &self.inner
    }
}

pub struct IsiphoSessionCtxFactory {
    default_session_config: SessionConfig,
}

impl Default for IsiphoSessionCtxFactory {
    fn default() -> Self {
        let mut default_session_config: SessionConfig = Default::default();

        default_session_config
            .config_options
            .set_bool(OPT_OPTIMIZER_SKIP_FAILED_RULES, false);

        Self {
            default_session_config,
        }
    }
}

impl IsiphoSessionCtxFactory {
    pub fn new(default_session_config: SessionConfig) -> IsiphoSessionCtxFactory {
        Self {
            default_session_config,
        }
    }

    pub fn create_isipho_session_ctx_with_config(
        catalog_name: String,
        catalog: Arc<dyn CatalogProvider>,
        session_config: SessionConfig,
    ) -> IsiphoSessionCtx {
        let df_session_state = context::default_session_builder(session_config);
        let df_session_ctx = SessionContext::with_state(df_session_state);

        df_session_ctx.register_catalog(catalog_name, catalog);

        IsiphoSessionCtx {
            inner: df_session_ctx,
        }
    }

    pub fn create_isipho_session_ctx(
        &self,
        catalog_name: String,
        catalog: Arc<dyn CatalogProvider>,
    ) -> IsiphoSessionCtx {
        Self::create_isipho_session_ctx_with_config(
            catalog_name,
            catalog,
            self.default_session_config.clone(),
        )
    }

    pub fn default_isipho_session_ctx(&self) -> IsiphoSessionCtx {
        let df_session_state =
            context::default_session_builder(self.default_session_config.clone());
        let df_session_ctx = SessionContext::with_state(df_session_state);

        IsiphoSessionCtx {
            inner: df_session_ctx,
        }
    }
}
