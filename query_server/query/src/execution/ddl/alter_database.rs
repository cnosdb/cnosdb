use crate::execution::ddl::DDLDefinitionTask;
use async_trait::async_trait;
use meta::meta_client::MetaError;
use models::schema::DatabaseOptions;
use snafu::ResultExt;
use spi::query::execution;
use spi::query::execution::MetadataSnafu;
use spi::query::execution::{ExecutionError, Output, QueryStateMachineRef};
use spi::query::logical_planner::AlterDatabase;

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
    async fn execute(
        &self,
        query_state_machine: QueryStateMachineRef,
    ) -> Result<Output, ExecutionError> {
        let tenant = query_state_machine.session.tenant();
        let client = query_state_machine
            .meta
            .tenant_manager()
            .tenant_meta(tenant)
            .ok_or(MetaError::TenantNotFound {
                tenant: tenant.to_string(),
            })
            .context(MetadataSnafu)?;
        let mut schema = client
            .get_db_schema(&self.stmt.database_name)
            .context(execution::MetadataSnafu)?
            .ok_or(MetaError::DatabaseNotFound {
                database: self.stmt.database_name.clone(),
            })
            .context(MetadataSnafu)?;
        build_database_schema(&self.stmt.database_options, &mut schema.config);
        // client
        //     .alter_database(schema)
        //     .context(execution::MetadataSnafu)?;

        todo!("alter method for meta")
        // return Ok(Output::Nil(()));
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
        config.with_precision(precision.clone());
    }
}
