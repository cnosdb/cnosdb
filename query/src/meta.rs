use std::sync::Arc;

use datafusion::catalog::catalog::CatalogProvider;
use datafusion::catalog::TableReference;
use datafusion::datasource::DefaultTableSource;
use datafusion::logical_expr::TableSource;

use tskv::engine::EngineRef;

use crate::catalog::{CatalogRef, UserCatalog};
use crate::context::{DEFAULT_CATALOG, DEFAULT_SCHEMA};
use crate::error::{Error, Result};

pub type MetaRef = Arc<dyn Meta>;

pub trait Meta {
    fn new(&self, tenant: String, engine: EngineRef) -> MetaRef;
    fn catalog_name(&self) -> String;
    fn schema_name(&self) -> String;
    fn get_table_provider(&self, name: TableReference) -> Result<Arc<dyn TableSource>>;
    fn catalog(&self) -> CatalogRef;
}

pub struct LocalMeta {
    catalog_name: String,
    schema_name: String,
    catalog: Arc<UserCatalog>,
}

impl LocalMeta {
    pub fn new_with_default(engine: EngineRef) -> Self {
        Self {
            catalog_name: DEFAULT_CATALOG.to_string(),
            schema_name: DEFAULT_SCHEMA.to_string(),
            catalog: Arc::new(UserCatalog::new(engine)),
        }
    }
}

impl Meta for LocalMeta {
    //todo: local mode dont support multi-tenant
    fn new(&self, _tenant: String, engine: EngineRef) -> MetaRef {
        Arc::new(LocalMeta::new_with_default(engine))
    }

    fn catalog_name(&self) -> String {
        self.catalog_name.clone()
    }

    fn schema_name(&self) -> String {
        self.schema_name.clone()
    }

    fn get_table_provider(&self, table: TableReference) -> Result<Arc<dyn TableSource>> {
        let catalog_name = self.catalog_name();
        let schema_name = self.schema_name();
        let name = table.resolve(&catalog_name, &schema_name);
        // note: local mod dont support multiple catalog use DEFAULT_CATALOG
        // let catalog_name = name.catalog;
        let schema = match self.catalog.schema(name.schema) {
            None => {
                return Err(Error::DatabaseNameError {
                    name: name.schema.to_string(),
                });
            }
            Some(s) => s,
        };
        let table = match schema.table(name.table) {
            None => {
                return Err(Error::TableNameError {
                    name: name.table.to_string(),
                });
            }
            Some(t) => t,
        };
        Ok(Arc::new(DefaultTableSource::new(table.clone())))
    }

    fn catalog(&self) -> CatalogRef {
        self.catalog.clone()
    }
}

pub struct RemoteMeta {}
