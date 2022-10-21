use crate::execution::ddl::DDLDefinitionTask;
use async_trait::async_trait;
use snafu::ResultExt;
use spi::catalog::MetaDataRef;
use spi::query::execution;
use spi::query::execution::{ExecutionError, Output, QueryStateMachineRef};

pub struct ShowDatabasesTask {}

impl ShowDatabasesTask {
    pub fn new() -> Self {
        ShowDatabasesTask {}
    }
}

#[async_trait]
impl DDLDefinitionTask for ShowDatabasesTask {
    async fn execute(
        &self,
        query_state_machine: QueryStateMachineRef,
    ) -> Result<Output, ExecutionError> {
        show_databases(query_state_machine.catalog.clone())
    }
}

fn show_databases(catalog: MetaDataRef) -> Result<Output, ExecutionError> {
    catalog.show_databases().context(execution::MetadataSnafu)
}
