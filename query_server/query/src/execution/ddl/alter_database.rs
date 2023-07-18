use async_trait::async_trait;
use meta::error::MetaError;
use models::schema::DatabaseOptions;
use spi::query::execution::{Output, QueryStateMachineRef};
use spi::query::logical_planner::AlterDatabase;
use spi::Result;

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
    async fn execute(&self, query_state_machine: QueryStateMachineRef) -> Result<Output> {
        let tenant = query_state_machine.session.tenant();
        let client = query_state_machine.meta.tenant_meta(tenant).await.ok_or(
            MetaError::TenantNotFound {
                tenant: tenant.to_string(),
            },
        )?;
        // .context(MetaSnafu)?;
        let mut schema = client
            .get_db_schema(&self.stmt.database_name)?
            // .context(spi::MetaSnafu)?
            .ok_or(MetaError::DatabaseNotFound {
                database: self.stmt.database_name.clone(),
            })?;
        // .context(spi::MetaSnafu)?;
        build_database_schema(&self.stmt.database_options, &mut schema.config);
        // client
        //     .alter_database(schema)
        //     .context(spi::MetaSnafu)?;

        client.alter_db_schema(&schema).await?;
        // .context(spi::MetaSnafu)?;
        return Ok(Output::Nil(()));
    }
}

fn build_database_schema(database_options: &DatabaseOptions, config: &mut DatabaseOptions) {
    if let Some(ttl) = database_options.ttl() {
        config.with_ttl(ttl.clone());
    }
    if let Some(replic) = database_options.replica() {
        config.with_replica(*replic);
    }
    if let Some(shard_num) = database_options.shard_num() {
        config.with_shard_num(*shard_num);
    }
    if let Some(vnode_duration) = database_options.vnode_duration() {
        config.with_vnode_duration(vnode_duration.clone());
    }
    if let Some(precision) = database_options.precision() {
        config.with_precision(*precision);
    }
}
