use async_trait::async_trait;
use meta::error::MetaError;
use models::schema::DatabaseSchema;
use spi::query::execution::{Output, QueryStateMachineRef};
use spi::query::logical_planner::CreateDatabase;
use spi::Result;

use crate::execution::ddl::DDLDefinitionTask;

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
    async fn execute(&self, query_state_machine: QueryStateMachineRef) -> Result<Output> {
        let CreateDatabase {
            ref name,
            ref if_not_exists,
            ..
        } = self.stmt;

        let tenant = query_state_machine.session.tenant();
        let client = query_state_machine.meta.tenant_meta(tenant).await.ok_or(
            MetaError::TenantNotFound {
                tenant: tenant.to_string(),
            },
        )?;
        // .context(MetaSnafu)?;
        let db = client
            .list_databases()?
            // .context(spi::MetaSnafu)?
            .contains(name);

        match (if_not_exists, db) {
            // do not create if exists
            (true, true) => Ok(Output::Nil(())),
            // Report an error if it exists
            (false, true) => Err(MetaError::DatabaseAlreadyExists {
                database: name.clone(),
            })?,
            // .context(spi::MetaSnafu),
            // does not exist, create
            (_, false) => {
                create_database(&self.stmt, query_state_machine).await?;
                Ok(Output::Nil(()))
            }
        }
    }
}

async fn create_database(stmt: &CreateDatabase, machine: QueryStateMachineRef) -> Result<()> {
    let tenant = machine.session.tenant();
    let client = machine
        .meta
        .tenant_meta(tenant)
        .await
        .ok_or(MetaError::TenantNotFound {
            tenant: tenant.to_string(),
        })?;
    // .context(MetaSnafu)?;
    let CreateDatabase {
        ref name,
        ref options,
        ..
    } = stmt;

    let mut database_schema = DatabaseSchema::new(machine.session.tenant(), name);
    database_schema.config = options.clone();
    client.create_db(database_schema).await?;
    // .context(spi::MetaSnafu)?;
    Ok(())
}
