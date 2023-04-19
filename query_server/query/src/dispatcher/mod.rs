use async_trait::async_trait;
use spi::query::dispatcher::QueryInfo;
use spi::service::protocol::QueryId;
use spi::Result;

pub mod manager;
pub mod persister;
pub mod query_tracker;

#[async_trait]
pub trait QueryPersister {
    fn remove(&self, query_id: &QueryId) -> Result<()>;
    async fn save(&self, query_id: QueryId, query: QueryInfo) -> Result<()>;
    async fn queries(&self) -> Result<Vec<QueryInfo>>;
}
