use crate::execution::ddl::DDLDefinitionTask;
use async_trait::async_trait;
use snafu::ResultExt;
use spi::catalog::MetaDataRef;
use spi::query::execution;
use spi::query::execution::{ExecutionError, Output, QueryStateMachineRef};

pub struct ShowTableTask {
    database_name: String,
}

impl ShowTableTask {
    pub fn new(database_name: String) -> Self {
        Self { database_name }
    }
}

#[async_trait]
impl DDLDefinitionTask for ShowTableTask {
    async fn execute(
        &self,
        query_state_machine: QueryStateMachineRef,
    ) -> Result<Output, ExecutionError> {
        return show_table(
            self.database_name.as_str(),
            query_state_machine.catalog.clone(),
        );
    }
}

fn show_table(database_name: &str, catalog: MetaDataRef) -> Result<Output, ExecutionError> {
    return catalog
        .show_table(database_name)
        .context(execution::MetadataSnafu);
}
