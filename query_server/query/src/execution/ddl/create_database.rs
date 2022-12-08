use crate::execution::ddl::DDLDefinitionTask;
use async_trait::async_trait;
use meta::meta_client::MetaError;
use models::schema::DatabaseSchema;
use snafu::ResultExt;

use spi::query::execution;
use spi::query::execution::MetadataSnafu;
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

        let tenant = query_state_machine.session.tenant();
        let client = query_state_machine
            .meta
            .tenant_manager()
            .tenant_meta(tenant)
            .ok_or(MetaError::TenantNotFound {
                tenant: tenant.to_string(),
            })
            .context(MetadataSnafu)?;
        let db = client
            .list_databases()
            .context(execution::MetadataSnafu)?
            .contains(name);

        match (if_not_exists, db) {
            // do not create if exists
            (true, true) => Ok(Output::Nil(())),
            // Report an error if it exists
            (false, true) => Err(MetaError::DatabaseAlreadyExists {
                database: name.clone(),
            })
            .context(execution::MetadataSnafu),
            // does not exist, create
            (_, false) => {
                create_database(&self.stmt, query_state_machine.clone())?;
                Ok(Output::Nil(()))
            }
        }
    }
}

fn create_database(
    stmt: &CreateDatabase,
    machine: QueryStateMachineRef,
) -> Result<(), ExecutionError> {
    let tenant = machine.session.tenant();
    let client = machine
        .meta
        .tenant_manager()
        .tenant_meta(tenant)
        .ok_or(MetaError::TenantNotFound {
            tenant: tenant.to_string(),
        })
        .context(MetadataSnafu)?;
    let CreateDatabase {
        ref name,
        
        ..
    } = stmt;

    let database_schema = DatabaseSchema::new(machine.session.tenant(), name);
    client
        .create_db(&database_schema)
        .context(execution::MetadataSnafu)?;
    Ok(())
}
