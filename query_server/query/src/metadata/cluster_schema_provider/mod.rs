mod builder;
mod factory;

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::datasource::MemTable;
use meta::error::MetaError;
use meta::model::MetaRef;
use models::auth::user::User;

use self::factory::tenants::ClusterSchemaTenantsFactory;
use self::factory::users::ClusterSchemaUsersFactory;
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

    pub async fn table(
        &self,
        user: &User,
        name: &str,
        metadata: MetaRef,
    ) -> std::result::Result<Arc<MemTable>, MetaError> {
        if let Some(f) = self.table_factories.get(name.to_ascii_lowercase().as_str()) {
            return f.create(user, metadata.clone()).await;
        }

        Err(MetaError::TableNotFound {
            table: name.to_string(),
        })
    }
}

type BoxSystemTableFactory = Box<dyn ClusterSchemaTableFactory + Send + Sync>;

#[async_trait]
pub trait ClusterSchemaTableFactory {
    fn table_name(&self) -> &str;
    async fn create(
        &self,
        user: &User,
        metadata: MetaRef,
    ) -> std::result::Result<Arc<MemTable>, MetaError>;
}
