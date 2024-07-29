use models::auth::user::User;
use models::schema::query_info::QueryId;
use models::schema::{DEFAULT_CATALOG, DEFAULT_DATABASE, DEFAULT_PRECISION};

use crate::query::config::StreamTriggerInterval;
use crate::query::execution::Output;
use crate::query::session::CnosSessionConfig;

#[derive(Clone)]
pub struct Context {
    // todo
    // user info
    // security certification info
    // ...
    user: User,
    tenant: String,
    database: String,
    precision: String,
    chunked: bool,
    session_config: CnosSessionConfig,
    is_old: bool,
}

impl Context {
    pub fn tenant(&self) -> &str {
        &self.tenant
    }

    pub fn database(&self) -> &str {
        &self.database
    }

    pub fn precision(&self) -> &str {
        &self.precision
    }

    pub fn user(&self) -> &User {
        &self.user
    }

    pub fn session_config(&self) -> &CnosSessionConfig {
        &self.session_config
    }
    pub fn chunked(&self) -> bool {
        self.chunked
    }
    pub fn is_old(&self) -> bool {
        self.is_old
    }
}

pub struct ContextBuilder {
    user: User,
    tenant: String,
    database: String,
    precision: String,
    chunked: bool,
    session_config: CnosSessionConfig,
    is_old: bool,
}

impl ContextBuilder {
    pub fn new(user: User) -> Self {
        Self {
            user,
            precision: DEFAULT_PRECISION.to_string(),
            tenant: DEFAULT_CATALOG.to_string(),
            database: DEFAULT_DATABASE.to_string(),
            chunked: Default::default(),
            session_config: Default::default(),
            is_old: Default::default(),
        }
    }

    pub fn with_tenant(mut self, tenant: Option<String>) -> Self {
        if let Some(tenant) = tenant {
            self.tenant = tenant
        }
        self
    }

    pub fn with_database(mut self, database: Option<String>) -> Self {
        if let Some(db) = database {
            self.database = db
        }
        self
    }

    pub fn with_precision(mut self, precision: Option<String>) -> Self {
        if let Some(precision) = precision {
            self.precision = precision
        }
        self
    }

    pub fn with_target_partitions(mut self, target_partitions: Option<usize>) -> Self {
        if let Some(dbtarget_partitions) = target_partitions {
            self.session_config = self
                .session_config
                .with_target_partitions(dbtarget_partitions);
        }
        self
    }

    pub fn with_stream_trigger_interval(mut self, interval: Option<StreamTriggerInterval>) -> Self {
        if let Some(interval) = interval {
            self.session_config = self.session_config.with_stream_trigger_interval(interval);
        }
        self
    }

    pub fn with_chunked(mut self, chunked: Option<bool>) -> Self {
        if let Some(chunked) = chunked {
            self.chunked = chunked;
        }
        self
    }

    pub fn with_is_old(mut self, is_old: Option<bool>) -> Self {
        if let Some(is_old) = is_old {
            self.is_old = is_old;
        }
        self
    }

    pub fn build(self) -> Context {
        Context {
            user: self.user,
            tenant: self.tenant,
            database: self.database,
            precision: self.precision,
            chunked: self.chunked,
            session_config: self.session_config,
            is_old: self.is_old,
        }
    }
}

#[derive(Clone)]
pub struct Query {
    context: Context,
    content: String,
}

impl Query {
    #[inline(always)]
    pub fn new(context: Context, content: String) -> Self {
        Self { context, content }
    }

    pub fn context(&self) -> &Context {
        &self.context
    }

    pub fn content(&self) -> &str {
        self.content.as_str()
    }
}

// #[derive(Clone)]
pub struct QueryHandle {
    id: QueryId,
    query: Query,
    result: Output,
}

impl QueryHandle {
    pub fn new(id: QueryId, query: Query, result: Output) -> Self {
        Self { id, query, result }
    }

    pub fn id(&self) -> QueryId {
        self.id
    }

    pub fn query(&self) -> &Query {
        &self.query
    }

    pub fn result(self) -> Output {
        self.result
    }
}
