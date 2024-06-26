use async_trait::async_trait;
use meta::error::MetaError;
use snafu::ResultExt;
use spi::query::execution::{Output, QueryStateMachineRef};
use spi::query::logical_planner::AlterDatabase;
use spi::{MetaSnafu, QueryResult};

use crate::execution::ddl::DDLDefinitionTask;

pub struct AlterDatabaseTask {
    stmt: AlterDatabase,
}

impl AlterDatabaseTask {
    pub fn new(stmt: AlterDatabase) -> AlterDatabaseTask {
        Self { stmt }
    }
}

#[async_trait]
impl DDLDefinitionTask for AlterDatabaseTask {
    async fn execute(&self, query_state_machine: QueryStateMachineRef) -> QueryResult<Output> {
        let tenant = query_state_machine.session.tenant();
        let client = query_state_machine
            .meta
            .tenant_meta(tenant)
            .await
            .ok_or_else(|| MetaError::TenantNotFound {
                tenant: tenant.to_string(),
            })
            .context(MetaSnafu)?;
        let mut schema = client
            .get_db_schema(&self.stmt.database_name)
            .context(MetaSnafu)?
            .ok_or_else(|| MetaError::DatabaseNotFound {
                database: self.stmt.database_name.clone(),
            })
            .context(MetaSnafu)?;

        if schema.is_hidden() {
            return Err(spi::QueryError::Meta {
                source: MetaError::DatabaseNotFound {
                    database: self.stmt.database_name.clone(),
                },
            });
        }
        schema.options.apply_builder(&self.stmt.database_options);

        client.alter_db_schema(schema).await.context(MetaSnafu)?;
        return Ok(Output::Nil(()));
    }
}
