use crate::execution::ddl::DDLDefinitionTask;
use async_trait::async_trait;
use snafu::ResultExt;
use spi::catalog::MetaDataRef;
use spi::query::execution;
use spi::query::execution::{ExecutionError, Output, QueryStateMachineRef};
use spi::query::logical_planner::DescribeTable;

pub struct DescribeTableTask {
    stmt: DescribeTable,
}

impl DescribeTableTask {
    pub fn new(stmt: DescribeTable) -> Self {
        Self { stmt }
    }
}

#[async_trait]
impl DDLDefinitionTask for DescribeTableTask {
    async fn execute(
        &self,
        query_state_machine: QueryStateMachineRef,
    ) -> Result<Output, ExecutionError> {
        describe_table(
            self.stmt.table_name.as_str(),
            query_state_machine.catalog.clone(),
        )
    }
}

fn describe_table(table_name: &str, catalog: MetaDataRef) -> Result<Output, ExecutionError> {
    catalog
        .describe_table(table_name)
        .context(execution::MetadataSnafu)
}
