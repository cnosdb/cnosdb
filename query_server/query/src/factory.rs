use parking_lot::RwLock;
use std::{any::Any, collections::HashMap, sync::Arc};

use crate::catalog::UserCatalog;
use datafusion::catalog::catalog::CatalogProvider;
use spi::catalog::{factory::CatalogManager, DEFAULT_CATALOG, DEFAULT_SCHEMA};
use tskv::engine::EngineRef;

// TODO refactor
pub struct TemporaryCatalogManager {
    tskv_client: EngineRef,
    catalogs: RwLock<HashMap<String, Arc<dyn CatalogProvider>>>,
}

impl TemporaryCatalogManager {
    pub fn new(tskv_client: EngineRef) -> Self {
        Self {
            tskv_client,
            catalogs: RwLock::new(HashMap::new()),
        }
    }
}

impl CatalogManager for TemporaryCatalogManager {
    fn init(&self) {
        // TODO
        let (name, catalog) = self.create_catalog_provider(DEFAULT_CATALOG, DEFAULT_SCHEMA);
        self.register_catalog(name, catalog);
    }

    fn create_catalog_provider(
        &self,
        default_catalog: &str,
        _default_schema: &str,
    ) -> (String, Arc<dyn CatalogProvider>) {
        (
            default_catalog.to_string(),
            Arc::new(UserCatalog::new(self.tskv_client.clone())),
        )
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn register_catalog(
        &self,
        name: String,
        catalog: Arc<dyn CatalogProvider>,
    ) -> Option<Arc<dyn CatalogProvider>> {
        let mut catalogs = self.catalogs.write();
        catalogs.insert(name, catalog)
    }

    fn catalog_names(&self) -> Vec<String> {
        let catalogs = self.catalogs.read();
        catalogs.keys().map(|s| s.to_string()).collect()
    }

    fn catalog(&self, name: &str) -> Option<Arc<dyn CatalogProvider>> {
        let catalogs = self.catalogs.read();
        catalogs.get(name).cloned()
    }

    fn drop_catalog(&self, name: &str) {
        let mut catalogs = self.catalogs.write();
        catalogs.remove(name);
    }
}
