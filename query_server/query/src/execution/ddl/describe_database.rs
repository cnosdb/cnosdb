use crate::execution::ddl::DDLDefinitionTask;
use async_trait::async_trait;
use snafu::ResultExt;
use spi::catalog::MetaDataRef;
use spi::query::execution;
use spi::query::execution::{ExecutionError, Output, QueryStateMachineRef};
use spi::query::logical_planner::DescribeDatabase;

pub struct DescribeDatabaseTask {
    stmt: DescribeDatabase,
}

impl DescribeDatabaseTask {
    pub fn new(stmt: DescribeDatabase) -> Self {
        Self { stmt }
    }
}

#[async_trait]
impl DDLDefinitionTask for DescribeDatabaseTask {
    async fn execute(
        &self,
        query_state_machine: QueryStateMachineRef,
    ) -> Result<Output, ExecutionError> {
        describe_database(
            self.stmt.database_name.as_str(),
            query_state_machine.catalog.clone(),
        )
    }
}

fn describe_database(database_name: &str, catalog: MetaDataRef) -> Result<Output, ExecutionError> {
    catalog
        .describe_database(database_name)
        .context(execution::MetadataSnafu)
}
