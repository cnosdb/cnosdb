use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use datafusion::execution::context::SessionState;
use datafusion::execution::memory_pool::MemoryPool;
use datafusion::execution::runtime_env::{RuntimeConfig, RuntimeEnv};
use datafusion::prelude::{SessionConfig, SessionContext};
use models::auth::user::User;
use models::oid::Oid;
use trace::{SpanContext, SpanExt, SpanRecorder};

use super::config::StreamTriggerInterval;
use crate::service::protocol::Context;
use crate::Result;

#[derive(Clone)]
pub struct SessionCtx {
    desc: Arc<SessionCtxDesc>,
    inner: SessionContext,
    span_ctx: Option<SpanContext>,
}

impl SessionCtx {
    pub fn inner(&self) -> &SessionContext {
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

    fn get_span_ctx(&self) -> Option<&SpanContext> {
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
    query_dedicated_hidden_dir: PathBuf,
}

impl SessionCtxFactory {
    pub fn new(query_dedicated_hidden_dir: PathBuf) -> Self {
        Self {
            query_dedicated_hidden_dir,
        }
    }

    pub fn create_session_ctx(
        &self,
        session_id: impl Into<String>,
        context: Context,
        tenant_id: Oid,
        memory_pool: Arc<dyn MemoryPool>,
        span_ctx: Option<SpanContext>,
    ) -> Result<SessionCtx> {
        let mut ctx = context.session_config().to_owned();
        if let Some(span_ctx) = &span_ctx {
            // inject span context into datafusion session config, so that it can be used in execution
            ctx.inner = ctx.inner.with_extension(Arc::new(span_ctx.clone()));
        }

        let mut rt_config = RuntimeConfig::new();
        rt_config.memory_pool = Some(memory_pool);
        let rt = RuntimeEnv::new(rt_config)?;
        let df_session_state = SessionState::with_config_rt(ctx.inner, Arc::new(rt))
            .with_session_id(session_id.into());
        let df_session_ctx = SessionContext::with_state(df_session_state);

        Ok(SessionCtx {
            desc: Arc::new(SessionCtxDesc {
                user: context.user_info().to_owned(),
                tenant_id,
                tenant: context.tenant().to_owned(),
                default_database: context.database().to_owned(),
                query_dedicated_hidden_dir: self.query_dedicated_hidden_dir.clone(),
            }),
            inner: df_session_ctx,
            span_ctx,
        })
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
