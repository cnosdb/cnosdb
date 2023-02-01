use std::sync::Arc;

use datafusion::{
    config::OPT_OPTIMIZER_SKIP_FAILED_RULES,
    execution::context::SessionState,
    execution::runtime_env::{RuntimeConfig, RuntimeEnv},
    prelude::{SessionConfig, SessionContext},
};
use sysinfo::SystemExt;

use models::{auth::user::User, oid::Oid};

use crate::service::protocol::Context;
use crate::Result;

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
    pub fn create_isipho_session_ctx(
        &self,
        context: Context,
        tenant_id: Oid,
    ) -> Result<IsiphoSessionCtx> {
        let isipho_ctx = context.session_config().to_owned();
        // temp: fix POC OOM
        let mut sys = sysinfo::System::new();
        sys.refresh_memory();
        let memory_limit = sys.total_memory() / 4;
        let rt_config = RuntimeConfig::new().with_memory_limit(memory_limit as usize, 1_f64);
        let rt = RuntimeEnv::new(rt_config)?;
        let df_session_state = SessionState::with_config_rt(isipho_ctx.inner, Arc::new(rt));
        let df_session_ctx = SessionContext::with_state(df_session_state);

        Ok(IsiphoSessionCtx {
            user: context.user_info().to_owned(),
            tenant_id,
            tenant: context.tenant().to_owned(),
            default_database: context.database().to_owned(),
            inner: df_session_ctx,
        })
    }
}

#[derive(Clone)]
pub struct IsiphoSessionConfig {
    inner: SessionConfig,
}

impl Default for IsiphoSessionConfig {
    fn default() -> Self {
        let inner: SessionConfig = Default::default();

        inner
            .config_options
            .write()
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

#[cfg(test)]
mod tests {
    use sysinfo::SystemExt;

    #[test]
    fn test_sysinfo() {
        let mut sys = sysinfo::System::new();
        sys.refresh_memory();
        let memory = sys.total_memory();
        println!("total memory {}", memory)
    }
}
