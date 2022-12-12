mod builder;
mod factory;

use std::{collections::HashMap, sync::Arc};

use datafusion::datasource::MemTable;
use meta::meta_client::{MetaClientRef, MetaError};
use models::auth::user::User;

use crate::dispatcher::query_tracker::QueryTracker;

use self::factory::{
    columns::ColumnsFactory, database_privileges::DatabasePrivilegesFactory,
    databases::DatabasesFactory, enabled_roles::EnabledRolesFactory, members::MembersFactory,
    queries::QueriesFactory, roles::RolesFactory, tables::TablesFactory,
};

const INFORMATION_SCHEMA: &str = "INFORMATION_SCHEMA";

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

    pub fn table(
        &self,
        user: &User,
        name: &str,
        metadata: MetaClientRef,
    ) -> std::result::Result<Arc<MemTable>, MetaError> {
        if let Some(f) = self.table_factories.get(name.to_ascii_lowercase().as_str()) {
            return f.create(user, metadata.clone(), self.query_tracker.clone());
        }

        Err(MetaError::TableNotFound {
            table: name.to_string(),
        })
    }
}

pub type BoxSystemTableFactory = Box<dyn InformationSchemaTableFactory + Send + Sync>;

pub trait InformationSchemaTableFactory {
    fn table_name(&self) -> &str;
    fn create(
        &self,
        user: &User,
        metadata: MetaClientRef,
        query_tracker: Arc<QueryTracker>,
    ) -> std::result::Result<Arc<MemTable>, MetaError>;
}
