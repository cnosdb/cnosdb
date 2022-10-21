use crate::execution::ddl::DDLDefinitionTask;
use async_trait::async_trait;
use snafu::ResultExt;
use spi::catalog::MetaDataRef;
use spi::query::execution;
use spi::query::execution::{ExecutionError, Output, QueryStateMachineRef};

pub struct ShowDatabaseTask {}

impl ShowDatabaseTask {
    pub fn new() -> Self {
        ShowDatabaseTask {}
    }
}

#[async_trait]
impl DDLDefinitionTask for ShowDatabaseTask {
    async fn execute(
        &self,
        query_state_machine: QueryStateMachineRef,
    ) -> Result<Output, ExecutionError> {
        return show_database(query_state_machine.catalog.clone());
    }
}

fn show_database(catalog: MetaDataRef) -> Result<Output, ExecutionError> {
    return catalog.show_database().context(execution::MetadataSnafu);
}
