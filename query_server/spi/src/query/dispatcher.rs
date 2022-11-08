use crate::query::execution::Output;
use crate::service::protocol::{Query, QueryId};
use async_trait::async_trait;

use super::execution::QueryState;
use super::Result;

#[async_trait]
pub trait QueryDispatcher: Send + Sync {
    fn start(&self);

    fn stop(&self);

    fn create_query_id(&self) -> QueryId;

    fn query_info(&self, id: &QueryId);

    async fn execute_query(&self, id: QueryId, query: &Query) -> Result<Vec<Output>>;

    fn running_query_infos(&self) -> Vec<QueryInfo>;

    fn running_query_status(&self) -> Vec<QueryStatus>;

    fn cancel_query(&self, id: &QueryId);
}

#[derive(Debug)]
pub struct QueryInfo {
    query_id: QueryId,
}

impl QueryInfo {
    pub fn new(query_id: QueryId) -> Self {
        Self { query_id }
    }

    pub fn query_id(&self) -> QueryId {
        self.query_id
    }
}

#[derive(Debug)]
pub struct QueryStatus {
    state: QueryState,
}

impl QueryStatus {
    pub fn new(state: QueryState) -> Self {
        Self { state }
    }

    pub fn query_state(&self) -> &QueryState {
        &self.state
    }
}
