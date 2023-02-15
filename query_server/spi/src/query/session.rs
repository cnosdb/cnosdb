use datafusion::execution::context;
use datafusion::prelude::{SessionConfig, SessionContext};
use models::auth::user::User;
use models::oid::Oid;

use crate::service::protocol::Context;

#[derive(Clone)]
pub struct IsiphoSessionCtx {
    // todo
    // ...
    user: User,

    tenant_id: Oid,
    tenant: String,
    default_database: String,

    inner: SessionContext,
}

impl IsiphoSessionCtx {
    pub fn inner(&self) -> &SessionContext {
        &self.inner
    }

    pub fn tenant_id(&self) -> &Oid {
        &self.tenant_id
    }

    pub fn tenant(&self) -> &str {
        &self.tenant
    }

    pub fn default_database(&self) -> &str {
        &self.default_database
    }

    pub fn user(&self) -> &User {
        &self.user
    }
}

#[derive(Default)]
pub struct IsiphoSessionCtxFactory {
    // TODO global config
}

impl IsiphoSessionCtxFactory {
    pub fn create_isipho_session_ctx(&self, context: Context, tenant_id: Oid) -> IsiphoSessionCtx {
        let isipho_ctx = context.session_config().to_owned();
        // TODO Use global configuration as the default configuration for session
        let df_session_state = context::default_session_builder(isipho_ctx.inner);
        let df_session_ctx = SessionContext::with_state(df_session_state);

        IsiphoSessionCtx {
            user: context.user_info().to_owned(),
            tenant_id,
            tenant: context.tenant().to_owned(),
            default_database: context.database().to_owned(),
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
        let inner =
            SessionConfig::default().set_bool("datafusion.optimizer.skip_failed_rules", false);

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
