use std::sync::Arc;

use async_trait::async_trait;

use crate::service::protocol::{Query, QueryHandle};

use super::Result;

pub type DBMSRef = Arc<dyn DatabaseManagerSystem + Send + Sync>;

#[async_trait]
pub trait DatabaseManagerSystem {
    async fn execute(&self, query: &Query) -> Result<QueryHandle>;
}
