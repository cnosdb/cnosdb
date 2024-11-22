use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use models::auth::auth_cache::{AuthCache, AuthCacheKey};
use models::auth::user::User;
use models::oid::Oid;
use models::schema::query_info::{QueryId, QueryInfo};
use trace::SpanContext;

use super::execution::QueryState;
use crate::query::execution::{Output, QueryStateMachine};
use crate::query::logical_planner::Plan;
use crate::service::protocol::Query;
use crate::QueryResult;

#[async_trait]
pub trait QueryDispatcher: Send + Sync {
    async fn start(&self) -> QueryResult<()>;

    fn stop(&self);

    fn create_query_id(&self) -> QueryId;

    fn query_info(&self, id: &QueryId);

    async fn execute_query(
        &self,
        tenant_id: Oid,
        id: QueryId,
        query: &Query,
        span: Option<&SpanContext>,
    ) -> QueryResult<Output>;

    async fn build_logical_plan(
        &self,
        query_state_machine: Arc<QueryStateMachine>,
    ) -> QueryResult<Option<Plan>>;

    async fn execute_logical_plan(
        &self,
        logical_plan: Plan,
        query_state_machine: Arc<QueryStateMachine>,
    ) -> QueryResult<Output>;

    async fn build_query_state_machine(
        &self,
        tenant_id: Oid,
        id: QueryId,
        query: Query,
        span: Option<&SpanContext>,
        auth_cache: Arc<AuthCache<AuthCacheKey, User>>,
    ) -> QueryResult<Arc<QueryStateMachine>>;

    fn running_query_infos(&self) -> Vec<QueryInfo>;

    fn running_query_status(&self) -> Vec<QueryStatus>;

    fn cancel_query(&self, id: &QueryId);
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
