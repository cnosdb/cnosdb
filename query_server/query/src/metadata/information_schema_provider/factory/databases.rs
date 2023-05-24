use std::sync::Arc;

use datafusion::datasource::MemTable;
use meta::error::MetaError;
use meta::model::MetaClientRef;
use models::auth::user::User;
use models::oid::Identifier;

use crate::dispatcher::query_tracker::QueryTracker;
use crate::metadata::information_schema_provider::builder::databases::InformationSchemaDatabasesBuilder;
use crate::metadata::information_schema_provider::InformationSchemaTableFactory;

pub const INFORMATION_SCHEMA_DATABASES: &str = "DATABASES";

/// This view only displays database information for which the current user has Read permission or higher.
pub struct DatabasesFactory {}

#[async_trait::async_trait]
impl InformationSchemaTableFactory for DatabasesFactory {
    fn table_name(&self) -> &'static str {
        INFORMATION_SCHEMA_DATABASES
    }

    async fn create(
        &self,
        user: &User,
        metadata: MetaClientRef,
        _query_tracker: Arc<QueryTracker>,
    ) -> std::result::Result<Arc<MemTable>, MetaError> {
        let mut builder = InformationSchemaDatabasesBuilder::default();

        let dbs = metadata.list_databases()?;
        let tenant = metadata.tenant();
        let tenant_id = tenant.id();
        let tenant_name = tenant.name();

        for db in dbs {
            // Check if the current user has at least read permission on this db, skip if not
            if !user.can_read_database(*tenant_id, &db) {
                continue;
            }

            if let Some(db_schema) = metadata.get_db_schema(&db)? {
                let options = db_schema.options();
                builder.append_row(
                    tenant_name,
                    db_schema.database_name(),
                    options.ttl_or_default().to_string(),
                    options.shard_num_or_default(),
                    options.vnode_duration_or_default().to_string(),
                    options.replica_or_default(),
                    options.precision_or_default().to_string(),
                );
            }
        }

        let mem_table = MemTable::try_from(builder)
            .map_err(|e| MetaError::CommonError { msg: e.to_string() })?;
        Ok(Arc::new(mem_table))
    }
}
