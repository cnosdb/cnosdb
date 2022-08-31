use std::{any::Any, sync::Arc};

use datafusion::catalog::catalog::CatalogProvider;

/// TODO Unstable & refactor
pub trait CatalogManager: Send + Sync {
    fn init(&self);

    fn create_catalog_provider(
        &self,
        default_catalog: &str,
        default_schema: &str,
    ) -> (String, Arc<dyn CatalogProvider>);

    fn as_any(&self) -> &dyn Any;

    fn register_catalog(
        &self,
        name: String,
        catalog: Arc<dyn CatalogProvider>,
    ) -> Option<Arc<dyn CatalogProvider>>;

    fn catalog_names(&self) -> Vec<String>;

    fn catalog(&self, name: &str) -> Option<Arc<dyn CatalogProvider>>;

    fn drop_catalog(&self, name: &str);
}
