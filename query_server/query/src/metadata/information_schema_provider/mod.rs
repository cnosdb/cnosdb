mod builder;
mod factory;

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
pub use builder::databases::{
    DATABASES_DATABASE_NAME, DATABASES_PERCISION, DATABASES_REPLICA, DATABASES_SHARD,
    DATABASES_TENANT_NAME, DATABASES_TTL, DATABASES_VNODE_DURATION,
};
pub use builder::tables::{
    TABLES_TABLE_DATABASE, TABLES_TABLE_ENGINE, TABLES_TABLE_NAME, TABLES_TABLE_OPTIONS,
    TABLES_TABLE_TENANT, TABLES_TABLE_TYPE,
};
use datafusion::datasource::MemTable;
pub use factory::databases::INFORMATION_SCHEMA_DATABASES;
pub use factory::tables::INFORMATION_SCHEMA_TABLES;
use meta::error::MetaError;
use meta::model::MetaClientRef;
use models::auth::user::User;

use self::factory::columns::ColumnsFactory;
use self::factory::database_privileges::DatabasePrivilegesFactory;
use self::factory::databases::DatabasesFactory;
use self::factory::enabled_roles::EnabledRolesFactory;
use self::factory::members::MembersFactory;
use self::factory::queries::QueriesFactory;
use self::factory::roles::RolesFactory;
use self::factory::tables::TablesFactory;
use super::INFORMATION_SCHEMA;
use crate::dispatcher::query_tracker::QueryTracker;

pub struct InformationSchemaProvider {
    query_tracker: Arc<QueryTracker>,

    table_factories: HashMap<String, BoxSystemTableFactory>,
}

impl InformationSchemaProvider {
    pub fn new(query_tracker: Arc<QueryTracker>) -> Self {
        let mut provider = Self {
            query_tracker,
            table_factories: Default::default(),
        };

        provider.register_table_factory(Box::new(DatabasesFactory {}));
        provider.register_table_factory(Box::new(TablesFactory {}));
        provider.register_table_factory(Box::new(ColumnsFactory {}));
        provider.register_table_factory(Box::new(EnabledRolesFactory {}));
        provider.register_table_factory(Box::new(RolesFactory {}));
        provider.register_table_factory(Box::new(DatabasePrivilegesFactory {}));
        provider.register_table_factory(Box::new(MembersFactory {}));
        provider.register_table_factory(Box::new(QueriesFactory {}));

        provider
    }

    fn register_table_factory(&mut self, factory: BoxSystemTableFactory) {
        let _ = self
            .table_factories
            .insert(factory.table_name().to_ascii_lowercase(), factory);
    }

    pub fn name(&self) -> &str {
        INFORMATION_SCHEMA
    }

    pub fn _table_names(&self) -> Vec<String> {
        self.table_factories.keys().cloned().collect()
    }

    pub async fn table(
        &self,
        user: &User,
        name: &str,
        metadata: MetaClientRef,
    ) -> std::result::Result<Arc<MemTable>, MetaError> {
        if let Some(f) = self.table_factories.get(name.to_ascii_lowercase().as_str()) {
            return f
                .create(user, metadata.clone(), self.query_tracker.clone())
                .await;
        }

        Err(MetaError::TableNotFound {
            table: name.to_string(),
        })
    }
}

type BoxSystemTableFactory = Box<dyn InformationSchemaTableFactory + Send + Sync>;

#[async_trait]
pub trait InformationSchemaTableFactory {
    fn table_name(&self) -> &str;
    async fn create(
        &self,
        user: &User,
        metadata: MetaClientRef,
        query_tracker: Arc<QueryTracker>,
    ) -> std::result::Result<Arc<MemTable>, MetaError>;
}
