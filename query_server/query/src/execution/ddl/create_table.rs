use crate::execution::ddl::DDLDefinitionTask;
use async_trait::async_trait;
use datafusion::sql::TableReference;
use models::schema::{TableSchema, TskvTableSchema};
use snafu::ResultExt;
use spi::catalog::{MetaDataRef, MetadataError};
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
        query_state_machine: QueryStateMachineRef,
    ) -> Result<Output, ExecutionError> {
        let CreateTable {
            ref name,
            ref if_not_exists,
            ..
        } = self.stmt;

        let table_ref: TableReference = name.as_str().into();
        let table = query_state_machine.catalog.table(table_ref);

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
                create_table(&self.stmt, query_state_machine.catalog.clone())?;
                Ok(Output::Nil(()))
            }
        }
    }
}

fn create_table(stmt: &CreateTable, catalog: MetaDataRef) -> Result<(), ExecutionError> {
    let CreateTable { name, .. } = stmt;
    let table_schema = build_schema(stmt, catalog.clone());
    catalog
        .create_table(name, TableSchema::TsKvTableSchema(table_schema))
        .context(execution::MetadataSnafu)
}

fn build_schema(stmt: &CreateTable, catalog: MetaDataRef) -> TskvTableSchema {
    let CreateTable { schema, name, .. } = stmt;

    let table: TableReference = name.as_str().into();
    let catalog_name = catalog.catalog_name();
    let schema_name = catalog.schema_name();
    let table_ref = table.resolve(&catalog_name, &schema_name);

    TskvTableSchema::new(
        table_ref.schema.to_string(),
        table.table().to_string(),
        schema.to_owned(),
    )
}
