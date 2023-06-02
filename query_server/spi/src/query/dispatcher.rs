use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use models::auth::user::UserDesc;
use models::oid::{Identifier, Oid};
use serde::{Deserialize, Serialize};
use trace::SpanContext;

use super::execution::QueryState;
use crate::query::execution::{Output, QueryStateMachine};
use crate::query::logical_planner::Plan;
use crate::service::protocol::{Query, QueryId};
use crate::Result;

#[async_trait]
pub trait QueryDispatcher: Send + Sync {
    async fn start(&self) -> Result<()>;

    fn stop(&self);

    fn create_query_id(&self) -> QueryId;

    fn query_info(&self, id: &QueryId);

    async fn execute_query(
        &self,
        tenant_id: Oid,
        id: QueryId,
        query: &Query,
        span: Option<&SpanContext>,
    ) -> Result<Output>;

    async fn build_logical_plan(
        &self,
        query_state_machine: Arc<QueryStateMachine>,
    ) -> Result<Option<Plan>>;

    async fn execute_logical_plan(
        &self,
        logical_plan: Plan,
        query_state_machine: Arc<QueryStateMachine>,
    ) -> Result<Output>;

    async fn build_query_state_machine(
        &self,
        tenant_id: Oid,
        id: QueryId,
        query: Query,
        span: Option<&SpanContext>,
    ) -> Result<Arc<QueryStateMachine>>;

    fn running_query_infos(&self) -> Vec<QueryInfo>;

    fn running_query_status(&self) -> Vec<QueryStatus>;

    fn cancel_query(&self, id: &QueryId);
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueryInfo {
    query_id: QueryId,
    query: String,

    tenant_id: Oid,
    tenant_name: String,
    user: UserDesc,
}

impl QueryInfo {
    pub fn new(
        query_id: QueryId,
        query: String,
        tenant_id: Oid,
        tenant_name: String,
        user: UserDesc,
    ) -> Self {
        Self {
            query_id,
            query,
            tenant_id,
            tenant_name,
            user,
        }
    }

    pub fn query_id(&self) -> QueryId {
        self.query_id
    }

    pub fn query(&self) -> &str {
        &self.query
    }

    pub fn tenant_id(&self) -> Oid {
        self.tenant_id
    }

    pub fn tenant_name(&self) -> &str {
        &self.tenant_name
    }

    pub fn user_desc(&self) -> &UserDesc {
        &self.user
    }

    pub fn user_id(&self) -> Oid {
        *self.user.id()
    }

    pub fn user_name(&self) -> &str {
        self.user.name()
    }
}

#[derive(Debug)]
pub struct QueryStatus {
    state: QueryState,
    duration: Duration,
    processed_count: u64,
    error_count: u64,
}

impl QueryStatus {
    pub fn new(state: QueryState, duration: Duration) -> Self {
        Self {
            state,
            duration,
            processed_count: 0,
            error_count: 0,
        }
    }

    pub fn query_state(&self) -> &QueryState {
        &self.state
    }

    pub fn duration(&self) -> &Duration {
        &self.duration
    }

    pub fn processed_count(&self) -> u64 {
        self.processed_count
    }

    pub fn error_count(&self) -> u64 {
        self.error_count
    }
}

pub struct QueryStatusBuilder {
    state: QueryState,
    duration: Duration,
    processed_count: u64,
    error_count: u64,
}

impl QueryStatusBuilder {
    pub fn new(state: QueryState, duration: Duration) -> Self {
        Self {
            state,
            duration,
            processed_count: 0,
            error_count: 0,
        }
    }

    pub fn with_processed_count(mut self, processed_count: u64) -> Self {
        self.processed_count = processed_count;
        self
    }

    pub fn with_error_count(mut self, error_count: u64) -> Self {
        self.error_count = error_count;
        self
    }

    pub fn build(self) -> QueryStatus {
        QueryStatus {
            state: self.state,
            duration: self.duration,
            processed_count: self.processed_count,
            error_count: self.error_count,
        }
    }
}
