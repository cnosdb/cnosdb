use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use snafu::ResultExt;
use spi::query::execution::{Output, QueryStateMachineRef};
use spi::query::logical_planner::ChecksumGroup;
use spi::query::recordbatch::RecordBatchStreamWrapper;
use spi::{CoordinatorSnafu, QueryResult};

use super::DDLDefinitionTask;

pub struct ChecksumGroupTask {
    schema: SchemaRef,
    stmt: ChecksumGroup,
}

impl ChecksumGroupTask {
    #[inline(always)]
    pub fn new(stmt: ChecksumGroup, schema: SchemaRef) -> Self {
        Self { schema, stmt }
    }
}

#[async_trait]
impl DDLDefinitionTask for ChecksumGroupTask {
    async fn execute(&self, query_state_machine: QueryStateMachineRef) -> QueryResult<Output> {
        let replication_set_id = self.stmt.replication_set_id;
        let tenant = query_state_machine.session.tenant();

        let coord = query_state_machine.coord.clone();
        let checksums = coord
            .replica_checksum(tenant, replication_set_id)
            .await
            .context(CoordinatorSnafu)?;
        let stream = RecordBatchStreamWrapper::new(self.schema.clone(), checksums);
        Ok(Output::StreamData(Box::pin(stream)))
    }
}
