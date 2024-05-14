use async_trait::async_trait;
use spi::query::dispatcher::QueryInfo;
use spi::service::protocol::QueryId;
use spi::QueryResult;

pub mod manager;
pub mod persister;
pub mod query_tracker;

#[async_trait]
pub trait QueryPersister {
    fn remove(&self, query_id: &QueryId) -> QueryResult<()>;
    async fn save(&self, query_id: QueryId, query: QueryInfo) -> QueryResult<()>;
    async fn queries(&self) -> QueryResult<Vec<QueryInfo>>;
}
