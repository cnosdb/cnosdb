use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::datasource::TableProvider;
use meta::error::MetaError;
use meta::model::MetaRef;
use models::auth::user::User;

use self::factory::tenants::ClusterSchemaTenantsFactory;
use self::factory::users::ClusterSchemaUsersFactory;
use super::CLUSTER_SCHEMA;

mod builder;
mod factory;

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
    ) -> Result<Arc<dyn TableProvider>, MetaError> {
        match self.table_factories.get(name.to_ascii_lowercase().as_str()) {
            Some(f) => Ok(f.create(user, metadata.clone())),
            None => Err(MetaError::TableNotFound {
                table: name.to_string(),
            }),
        }
    }
}

type BoxSystemTableFactory = Box<dyn ClusterSchemaTableFactory + Send + Sync>;

#[async_trait]
pub trait ClusterSchemaTableFactory {
    fn table_name(&self) -> &str;
    fn create(&self, user: &User, metadata: MetaRef) -> Arc<dyn TableProvider>;
}
