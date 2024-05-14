use async_trait::async_trait;
use coordinator::VnodeSummarizerCmdType;
use models::arrow::SchemaRef;
use spi::query::execution::{Output, QueryStateMachineRef};
use spi::query::logical_planner::ShowCompaction;
use spi::query::recordbatch::RecordBatchStreamWrapper;
use spi::Result;

use super::DDLDefinitionTask;

pub struct ShowCompactionTask {
    schema: SchemaRef,
    stmt: ShowCompaction,
}

impl ShowCompactionTask {
    #[inline(always)]
    pub fn new(stmt: ShowCompaction, schema: SchemaRef) -> Self {
        Self { stmt, schema }
    }
}

#[async_trait]
impl DDLDefinitionTask for ShowCompactionTask {
    async fn execute(&self, query_state_machine: QueryStateMachineRef) -> Result<Output> {
        let node_id = self.stmt.node_id;
        let tenant = query_state_machine.session.tenant();

        let coord = query_state_machine.coord.clone();
        let cmd_type = VnodeSummarizerCmdType::ShowCompaction(node_id);
        let records = coord.vnode_summarizer(tenant, cmd_type).await?;
        let stream = RecordBatchStreamWrapper::new(self.schema.clone(), records);
        Ok(Output::StreamData(Box::pin(stream)))
    }
}
