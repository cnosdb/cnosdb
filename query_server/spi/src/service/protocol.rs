use std::sync::atomic::{AtomicU64, Ordering};

use crate::catalog::DEFAULT_DATABASE;
use crate::query::execution::Output;
use crate::query::session::IsiphoSessionConfig;

#[derive(Debug, Clone, Copy)]
pub struct QueryId(u64);

impl QueryId {
    pub fn next_id() -> Self {
        static NEXT_ID: AtomicU64 = AtomicU64::new(1);
        let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
        Self(id)
    }
}

#[derive(Clone)]
pub struct UserInfo {
    pub user: String,
    pub password: String,
}

#[derive(Clone)]
pub struct Context {
    // todo
    // user info
    // security certification info
    // ...
    user_info: UserInfo,
    database: String,
    session_config: IsiphoSessionConfig,
}

impl Context {
    pub fn catalog(&self) -> &str {
        &self.user_info.user
    }

    pub fn database(&self) -> &str {
        &self.database
    }

    pub fn user_info(&self) -> &UserInfo {
        &self.user_info
    }

    pub fn session_config(&self) -> &IsiphoSessionConfig {
        &self.session_config
    }
}

pub struct ContextBuilder {
    user_info: UserInfo,
    database: String,
    session_config: IsiphoSessionConfig,
}

impl ContextBuilder {
    pub fn new(user_info: UserInfo) -> Self {
        Self {
            user_info,
            database: DEFAULT_DATABASE.to_string(),
            session_config: Default::default(),
        }
    }

    pub fn with_database(mut self, database: Option<String>) -> Self {
        if let Some(db) = database {
            self.database = db
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

    pub fn build(self) -> Context {
        Context {
            user_info: self.user_info,
            database: self.database,
            session_config: self.session_config,
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

pub struct QueryHandle {
    id: QueryId,
    query: Query,
    result: Vec<Output>,
}

impl QueryHandle {
    #[inline(always)]
    pub fn new(id: QueryId, query: Query, result: Vec<Output>) -> Self {
        Self { id, query, result }
    }

    pub fn id(&self) -> QueryId {
        self.id
    }

    pub fn query(&self) -> &Query {
        &self.query
    }

    pub fn cancel(&self) {
        // TODO
    }

    pub fn result(&mut self) -> &mut Vec<Output> {
        self.result.as_mut()
    }
}
