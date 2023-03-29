use std::sync::Arc;

use async_trait::async_trait;
use spi::query::execution::{Output, QueryStateMachineRef};
use spi::service::protocol::QueryId;
use spi::{QueryError, Result};

use super::SystemTask;
use crate::dispatcher::query_tracker::QueryTracker;

pub struct KillQueryTask {
    query_tracker: Arc<QueryTracker>,

    query_id: QueryId,
}

impl KillQueryTask {
    pub fn new(query_tracker: Arc<QueryTracker>, query_id: QueryId) -> Self {
        Self {
            query_tracker,
            query_id,
        }
    }
}

#[async_trait]
impl SystemTask for KillQueryTask {
    async fn execute(&self, _query_state_machine: QueryStateMachineRef) -> Result<Output> {
        if let Some(q) = self.query_tracker.expire_query(&self.query_id) {
            let _ = q.cancel();
        } else {
            return Err(QueryError::QueryNotFound {
                query_id: self.query_id,
            });
        }

        Ok(Output::Nil(()))
    }
}
