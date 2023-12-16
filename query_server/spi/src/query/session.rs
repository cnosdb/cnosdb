use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use datafusion::execution::context::SessionState;
use datafusion::execution::memory_pool::MemoryPool;
use datafusion::execution::runtime_env::{RuntimeConfig, RuntimeEnv};
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion::variable::VarType;
use models::auth::user::User;
use models::oid::Oid;
use trace::{SpanContext, SpanExt, SpanRecorder};

use super::config::StreamTriggerInterval;
use super::variable::VarProviderRef;
use crate::service::protocol::Context;
use crate::Result;

#[derive(Clone)]
pub struct SessionCtx {
    desc: Arc<SessionCtxDesc>,
    inner: SessionState,
    span_ctx: Option<SpanContext>,
}

impl SessionCtx {
    pub fn inner(&self) -> &SessionState {
        &self.inner
    }

    pub fn tenant_id(&self) -> &Oid {
        &self.desc.tenant_id
    }

    pub fn tenant(&self) -> &str {
        &self.desc.tenant
    }

    pub fn default_database(&self) -> &str {
        &self.desc.default_database
    }

    pub fn user(&self) -> &User {
        &self.desc.user
    }

    pub fn dedicated_hidden_dir(&self) -> &Path {
        self.desc.query_dedicated_hidden_dir.as_path()
    }

    pub fn with_span_ctx(&self, span_ctx: Option<SpanContext>) -> Self {
        Self {
            desc: self.desc.clone(),
            inner: self.inner.clone(),
            span_ctx,
        }
    }

    pub fn get_span_ctx(&self) -> Option<&SpanContext> {
        self.span_ctx.as_ref()
        // self.inner().config().get_extension::<SpanContext>();
    }

    pub fn get_child_span_recorder(&self, name: &'static str) -> SpanRecorder {
        SpanRecorder::new(self.get_span_ctx().child_span(name))
    }
}

#[derive(Clone)]
pub struct SessionCtxDesc {
    // todo
    // ...
    user: User,

    tenant_id: Oid,
    tenant: String,
    default_database: String,

    query_dedicated_hidden_dir: PathBuf,
}

#[derive(Default)]
pub struct SessionCtxFactory {
    sys_var_provider: Option<VarProviderRef>,
    query_dedicated_hidden_dir: PathBuf,
}

impl SessionCtxFactory {
    pub fn new(
        sys_var_provider: Option<VarProviderRef>,
        query_dedicated_hidden_dir: PathBuf,
    ) -> Self {
        Self {
            sys_var_provider,
            query_dedicated_hidden_dir,
        }
    }

    pub fn create_session_ctx(
        &self,
        session_id: impl Into<String>,
        context: &Context,
        tenant_id: Oid,
        memory_pool: Arc<dyn MemoryPool>,
        span_ctx: Option<SpanContext>,
    ) -> Result<SessionCtx> {
        let df_session_ctx = self.build_df_session_context(
            session_id,
            context.session_config().to_df_config(),
            memory_pool,
            &span_ctx,
        )?;

        Ok(SessionCtx {
            desc: Arc::new(SessionCtxDesc {
                user: context.user_info().to_owned(),
                tenant_id,
                tenant: context.tenant().to_owned(),
                default_database: context.database().to_owned(),
                query_dedicated_hidden_dir: self.query_dedicated_hidden_dir.clone(),
            }),
            inner: df_session_ctx.state(),
            span_ctx,
        })
    }

    fn build_df_session_context(
        &self,
        session_id: impl Into<String>,
        config: &SessionConfig,
        memory_pool: Arc<dyn MemoryPool>,
        span_ctx: &Option<SpanContext>,
    ) -> Result<SessionContext> {
        let mut config = config.clone();
        if let Some(span_ctx) = span_ctx {
            // inject span context into datafusion session config, so that it can be used in execution
            config = config.with_extension(Arc::new(span_ctx.clone()))
        }

        let rt_config = RuntimeConfig::new().with_memory_pool(memory_pool);
        let rt = RuntimeEnv::new(rt_config)?;
        let df_session_state =
            SessionState::with_config_rt(config, Arc::new(rt)).with_session_id(session_id.into());
        let df_session_ctx = SessionContext::with_state(df_session_state);
        // register built-in system variables
        if let Some(p) = self.sys_var_provider.as_ref() {
            df_session_ctx.register_variable(VarType::System, p.clone());
        }

        Ok(df_session_ctx)
    }
}

#[derive(Clone)]
pub struct CnosSessionConfig {
    inner: SessionConfig,
}

impl Default for CnosSessionConfig {
    fn default() -> Self {
        let inner = SessionConfig::default()
            .set_bool("datafusion.optimizer.skip_failed_rules", false)
            // TODO read from config file
            // .with_extension(Arc::new(StreamTriggerInterval::Once));
            .with_extension(Arc::new(StreamTriggerInterval::Interval(
                Duration::from_secs(6),
            )));

        Self { inner }
    }
}

impl CnosSessionConfig {
    pub fn to_df_config(&self) -> &SessionConfig {
        &self.inner
    }

    /// Customize target_partitions
    /// partition count must be greater than zero
    pub fn with_target_partitions(mut self, n: usize) -> Self {
        self.inner = self.inner.with_target_partitions(n);
        self
    }

    /// TODO
    pub fn with_stream_trigger_interval(mut self, interval: StreamTriggerInterval) -> Self {
        self.inner = self.inner.with_extension(Arc::new(interval));
        self
    }
}
