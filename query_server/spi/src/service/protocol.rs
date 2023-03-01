use std::sync::atomic::{AtomicU64, Ordering};

use models::auth::user::User;
use models::schema::{DEFAULT_CATALOG, DEFAULT_DATABASE};

use crate::query::execution::Output;
use crate::query::session::CnosSessionConfig;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct QueryId(u64);

impl QueryId {
    pub fn next_id() -> Self {
        static NEXT_ID: AtomicU64 = AtomicU64::new(1);
        let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
        Self(id)
    }
}

impl From<u64> for QueryId {
    fn from(u: u64) -> Self {
        QueryId(u)
    }
}

impl TryFrom<Vec<u8>> for QueryId {
    type Error = String;

    fn try_from(bytes: Vec<u8>) -> Result<Self, Self::Error> {
        if bytes.len() != 8 {
            return Err(format!("Incorrect content: {:?}", &bytes));
        }

        let len_bytes: [u8; 8] = unsafe { bytes[0..8].try_into().unwrap_unchecked() };

        Ok(Self(u64::from_le_bytes(len_bytes)))
    }
}

impl From<QueryId> for Vec<u8> {
    fn from(val: QueryId) -> Self {
        val.0.to_le_bytes().into()
    }
}

impl ToString for QueryId {
    fn to_string(&self) -> String {
        self.0.to_string()
    }
}

#[derive(Clone)]
pub struct Context {
    // todo
    // user info
    // security certification info
    // ...
    user_info: User,
    tenant: String,
    database: String,
    session_config: CnosSessionConfig,
}

impl Context {
    pub fn tenant(&self) -> &str {
        &self.tenant
    }

    pub fn database(&self) -> &str {
        &self.database
    }

    pub fn user_info(&self) -> &User {
        &self.user_info
    }

    pub fn session_config(&self) -> &CnosSessionConfig {
        &self.session_config
    }
}

pub struct ContextBuilder {
    user_info: User,
    tenant: String,
    database: String,
    session_config: CnosSessionConfig,
}

impl ContextBuilder {
    pub fn new(user_info: User) -> Self {
        Self {
            user_info,
            tenant: DEFAULT_CATALOG.to_string(),
            database: DEFAULT_DATABASE.to_string(),
            session_config: Default::default(),
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
            tenant: self.tenant,
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

#[derive(Clone)]
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
