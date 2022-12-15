mod builder;
mod factory;

use std::{collections::HashMap, sync::Arc};

use datafusion::datasource::MemTable;
use meta::meta_client::{MetaError, MetaRef};
use models::auth::user::User;

use self::factory::{tenants::ClusterSchemaTenantsFactory, users::ClusterSchemaUsersFactory};

use super::CLUSTER_SCHEMA;

pub struct ClusterSchemaProvider {
    table_factories: HashMap<String, BoxSystemTableFactory>,
}

impl ClusterSchemaProvider {
    pub fn new() -> Self {
        let mut provider = Self {
            table_factories: Default::default(),
        };

        provider.register_table_factory(Box::new(ClusterSchemaTenantsFactory {}));
        provider.register_table_factory(Box::new(ClusterSchemaUsersFactory {}));

        provider
    }

    fn register_table_factory(&mut self, factory: BoxSystemTableFactory) {
        let _ = self
            .table_factories
            .insert(factory.table_name().to_ascii_lowercase(), factory);
    }

    pub fn name(&self) -> &str {
        CLUSTER_SCHEMA
    }

    pub fn _table_names(&self) -> Vec<String> {
        self.table_factories.keys().cloned().collect()
    }

    pub fn table(
        &self,
        user: &User,
        name: &str,
        metadata: MetaRef,
    ) -> std::result::Result<Arc<MemTable>, MetaError> {
        if let Some(f) = self.table_factories.get(name.to_ascii_lowercase().as_str()) {
            return f.create(user, metadata.clone());
        }

        Err(MetaError::TableNotFound {
            table: name.to_string(),
        })
    }
}

type BoxSystemTableFactory = Box<dyn ClusterSchemaTableFactory + Send + Sync>;

pub trait ClusterSchemaTableFactory {
    fn table_name(&self) -> &str;
    fn create(
        &self,
        user: &User,
        metadata: MetaRef,
    ) -> std::result::Result<Arc<MemTable>, MetaError>;
}
