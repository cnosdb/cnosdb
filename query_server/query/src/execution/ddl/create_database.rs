use async_trait::async_trait;
use meta::error::MetaError;
use models::schema::database_schema::DatabaseSchema;
use snafu::ResultExt;
use spi::query::execution::{Output, QueryStateMachineRef};
use spi::query::logical_planner::CreateDatabase;
use spi::{MetaSnafu, QueryError, QueryResult};

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
    async fn execute(&self, query_state_machine: QueryStateMachineRef) -> QueryResult<Output> {
        let res = create_database(&self.stmt, query_state_machine).await;
        if self.stmt.if_not_exists
            && matches!(
                res,
                Err(QueryError::Meta {
                    source: MetaError::DatabaseAlreadyExists { .. }
                })
            )
        {
            return Ok(Output::Nil(()));
        }
        res.map(|_| Output::Nil(()))
    }
}

async fn create_database(stmt: &CreateDatabase, machine: QueryStateMachineRef) -> QueryResult<()> {
    let tenant = machine.session.tenant();
    let client = machine
        .meta
        .tenant_meta(tenant)
        .await
        .ok_or_else(|| MetaError::TenantNotFound {
            tenant: tenant.to_string(),
        })
        .context(MetaSnafu)?;

    let CreateDatabase {
        ref name,
        ref options,
        ref config,
        ..
    } = stmt;
    let db_options = options.clone().build();
    let db_config = config.clone().build(machine.config());

    let database_schema =
        DatabaseSchema::new(machine.session.tenant(), name, db_options, db_config.into());
    client.create_db(database_schema).await.context(MetaSnafu)?;
    Ok(())
}
