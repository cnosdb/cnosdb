use crate::query::execution::Output;
use crate::service::protocol::{Query, QueryId};
use async_trait::async_trait;
use datafusion::physical_plan::SendableRecordBatchStream;

use super::Result;

#[async_trait]
pub trait QueryDispatcher: Send + Sync {
    fn start(&self);

    fn stop(&self);

    fn create_query_id(&self) -> QueryId;

    fn get_query_info(&self, id: &QueryId);

    async fn execute_query(&self, id: QueryId, query: &Query) -> Result<Vec<Output>>;

    fn cancel_query(&self, id: &QueryId);
}
