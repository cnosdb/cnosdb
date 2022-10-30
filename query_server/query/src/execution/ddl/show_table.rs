use crate::execution::ddl::DDLDefinitionTask;
use async_trait::async_trait;
use snafu::ResultExt;
use spi::catalog::MetaDataRef;
use spi::query::execution;
use spi::query::execution::{ExecutionError, Output, QueryStateMachineRef};

pub struct ShowTablesTask {
    database_name: Option<String>,
}

impl ShowTablesTask {
    pub fn new(database_name: Option<String>) -> Self {
        Self { database_name }
    }
}

#[async_trait]
impl DDLDefinitionTask for ShowTablesTask {
    async fn execute(
        &self,
        query_state_machine: QueryStateMachineRef,
    ) -> Result<Output, ExecutionError> {
        show_tables(&self.database_name, query_state_machine.catalog.clone())
    }
}

fn show_tables(
    database_name: &Option<String>,
    catalog: MetaDataRef,
) -> Result<Output, ExecutionError> {
    catalog
        .show_tables(database_name)
        .context(execution::MetadataSnafu)
}
