use crate::execution::ddl::DDLDefinitionTask;
use async_trait::async_trait;
use models::schema::DatabaseSchema;
use snafu::ResultExt;
use spi::catalog::MetadataError;
use spi::query::execution;
use spi::query::execution::{ExecutionError, Output, QueryStateMachineRef};
use spi::query::logical_planner::CreateDatabase;

pub struct CreateDatabaseTask {
    stmt: CreateDatabase,
}

impl CreateDatabaseTask {
    pub fn new(stmt: CreateDatabase) -> Self {
        Self { stmt }
    }
}

#[async_trait]
impl DDLDefinitionTask for CreateDatabaseTask {
    async fn execute(
        &self,
        query_state_machine: QueryStateMachineRef,
    ) -> Result<Output, ExecutionError> {
        let CreateDatabase {
            ref name,
            ref if_not_exists,
            ..
        } = self.stmt;

        let db = query_state_machine
            .catalog
            .database_names()
            .context(execution::MetadataSnafu)?
            .contains(name);

        match (if_not_exists, db) {
            // do not create if exists
            (true, true) => Ok(Output::Nil(())),
            // Report an error if it exists
            (false, true) => Err(MetadataError::DatabaseAlreadyExists {
                database_name: name.clone(),
            })
            .context(execution::MetadataSnafu),
            // does not exist, create
            (_, false) => {
                create_database(&self.stmt, query_state_machine)?;
                Ok(Output::Nil(()))
            }
        }
    }
}

fn create_database(
    stmt: &CreateDatabase,
    machine: QueryStateMachineRef,
) -> Result<(), ExecutionError> {
    let CreateDatabase {
        ref name,
        options: _,
        ..
    } = stmt;

    let database_schema = DatabaseSchema::new(machine.session.tenant(), name);
    machine
        .catalog
        .create_database(name, database_schema)
        .context(execution::MetadataSnafu)?;
    Ok(())
}
