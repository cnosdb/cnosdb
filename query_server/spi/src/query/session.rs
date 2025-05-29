use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use coordinator::Coordinator;
use datafusion::common::extensions_options;
use datafusion::config::ConfigExtension;
use datafusion::execution::context::SessionState;
use datafusion::execution::memory_pool::MemoryPool;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion::variable::VarType;
use models::auth::user::User;
use models::oid::Oid;
use trace::span_ext::SpanExt;
use trace::{Span, SpanContext};

use super::config::StreamTriggerInterval;
use super::variable::VarProviderRef;
use crate::service::protocol::Context;
use crate::QueryResult;

extensions_options! {
    pub struct SqlExecInfo {
        pub copyinto_trigger_flush_size: u64, default = 128 * 1024 * 1024 // 128MB
    }
}
impl ConfigExtension for SqlExecInfo {
    const PREFIX: &'static str = "sql_exec_info";
}

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

    pub fn get_child_span(&self, name: &'static str) -> Span {
        Span::from_context(name, self.get_span_ctx())
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
    session_function_register: Option<fn(df_session_ctx: &SessionContext, context: &Context)>,
}

impl SessionCtxFactory {
    pub fn new(
        sys_var_provider: Option<VarProviderRef>,
        query_dedicated_hidden_dir: PathBuf,
        session_function_register: Option<fn(df_session_ctx: &SessionContext, context: &Context)>,
    ) -> Self {
        Self {
            sys_var_provider,
            query_dedicated_hidden_dir,
            session_function_register,
        }
    }

    pub fn create_session_ctx(
        &self,
        session_id: impl Into<String>,
        context: &Context,
        tenant_id: Oid,
        memory_pool: Arc<dyn MemoryPool>,
        span_ctx: Option<SpanContext>,
        coord: Arc<dyn Coordinator>,
    ) -> QueryResult<SessionCtx> {
        let df_session_ctx =
            self.build_df_session_context(session_id, context, memory_pool, &span_ctx, coord)?;

        Ok(SessionCtx {
            desc: Arc::new(SessionCtxDesc {
                user: context.user().to_owned(),
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
        context: &Context,
        memory_pool: Arc<dyn MemoryPool>,
        span_ctx: &Option<SpanContext>,
        coord: Arc<dyn Coordinator>,
    ) -> QueryResult<SessionContext> {
        let mut df_session_cfg = context.session_config().to_df_config().clone();
        if let Some(span_ctx) = span_ctx {
            // inject span context into datafusion session config, so that it can be used in execution
            df_session_cfg = df_session_cfg.with_extension(Arc::new(*span_ctx))
        }
        // inject cnosdb_config into datafusion session_config
        df_session_cfg
            .options_mut()
            .extensions
            .insert(SqlExecInfo::default());
        df_session_cfg = df_session_cfg.set_u64(
            "sql_exec_info.copyinto_trigger_flush_size",
            coord.get_config().storage.copyinto_trigger_flush_size,
        );

        let rt = RuntimeEnvBuilder::new()
            .with_memory_pool(memory_pool)
            .build()?;
        let df_session_state = SessionStateBuilder::new()
            .with_runtime_env(Arc::new(rt))
            .with_session_id(session_id.into())
            .with_config(df_session_cfg)
            .build();
        let df_session_ctx = SessionContext::new_with_state(df_session_state);
        // register built-in system variables
        if let Some(p) = self.sys_var_provider.as_ref() {
            df_session_ctx.register_variable(VarType::System, p.clone());
        }
        if let Some(register) = self.session_function_register.as_ref() {
            register(&df_session_ctx, context);
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
