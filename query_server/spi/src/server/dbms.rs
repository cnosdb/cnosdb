use std::sync::Arc;

use async_trait::async_trait;

use crate::service::protocol::{Query, QueryHandle, QueryId};

use super::Result;

pub type DBMSRef = Arc<dyn DatabaseManagerSystem + Send + Sync>;

#[async_trait]
pub trait DatabaseManagerSystem {
    async fn execute(&self, query: &Query) -> Result<QueryHandle>;
    fn metrics(&self) -> String;
    fn cancel(&self, query_id: &QueryId);
}
