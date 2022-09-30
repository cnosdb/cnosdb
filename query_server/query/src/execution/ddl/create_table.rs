use crate::execution::ddl::DDLDefinitionTask;
use crate::metadata::MetaDataRef;
use async_trait::async_trait;
use datafusion::sql::TableReference;
use snafu::ResultExt;
use spi::catalog::MetadataError;
use spi::query::execution;
use spi::query::execution::{ExecutionError, Output, QueryStateMachineRef};
use spi::query::logical_planner::CreateTable;

pub struct CreateTableTask {
    stmt: CreateTable,
}

impl CreateTableTask {
    pub fn new(stmt: CreateTable) -> Self {
        Self { stmt }
    }
}

#[async_trait]
impl DDLDefinitionTask for CreateTableTask {
    async fn execute(
        &self,
        catalog: MetaDataRef,
        query_state_machine: QueryStateMachineRef,
    ) -> Result<Output, ExecutionError> {
        let CreateTable {
            ref name,
            ref if_not_exists,
            ..
        } = self.stmt;

        let table_ref: TableReference = name.as_str().into();
        let table = catalog.table_provider(table_ref);

        match (if_not_exists, table) {
            // do not create if exists
            (true, Ok(_)) => Ok(Output::Nil(())),
            // Report an error if it exists
            (false, Ok(_)) => Err(MetadataError::TableAlreadyExists {
                table_name: name.clone(),
            })
            .context(execution::MetadataSnafu),
            // does not exist, create
            (_, Err(_)) => {
                create_table(&self.stmt, catalog, query_state_machine).await?;
                Ok(Output::Nil(()))
            }
        }
    }
}

async fn create_table(
    _stmt: &CreateTable,
    _catalog: MetaDataRef,
    _query_state_machine: QueryStateMachineRef,
) -> Result<(), ExecutionError> {
    todo!()
}
