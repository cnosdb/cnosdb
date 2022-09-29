use std::sync::atomic::{AtomicU64, Ordering};

use crate::catalog::{DEFAULT_CATALOG, DEFAULT_SCHEMA};
use crate::query::execution::Output;

#[derive(Debug, Clone, Copy)]
pub struct QueryId(u64);

impl QueryId {
    pub fn next_id() -> Self {
        static NEXT_ID: AtomicU64 = AtomicU64::new(1);
        let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
        Self(id)
    }
}

pub struct UserInfo {
    pub user: String,
    pub password: String,
}

#[derive(Debug, Clone)]
pub struct Context {
    // todo
    // user info
    // security certification info
    // ...
    pub catalog: String,
    pub database: String,
}

impl Default for Context {
    fn default() -> Self {
        Self {
            catalog: DEFAULT_CATALOG.to_string(),
            database: DEFAULT_SCHEMA.to_string(),
        }
    }
}

pub struct ContextBuilder {
    user_info: UserInfo,
    database: String,
}

impl ContextBuilder {
    pub fn new(user_info: UserInfo) -> ContextBuilder {
        Self {
            user_info,
            database: DEFAULT_SCHEMA.to_string(),
        }
    }

    pub fn with_database(mut self, database: Option<String>) -> ContextBuilder {
        if let Some(db) = database {
            self.database = db
        }
        self
    }

    pub fn build(self) -> Context {
        Context {
            catalog: self.user_info.user,
            database: self.database,
        }
    }
}

#[derive(Debug, Clone)]
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
