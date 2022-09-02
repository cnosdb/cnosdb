use crate::catalog::{CatalogRef, UserCatalog, UserCatalogRef};
use crate::error::Error::{DatabaseName, TableName};
use crate::error::{Error, Result};
use crate::function::simple_func_manager::SimpleFunctionMetadataManager;
use datafusion::arrow::datatypes::DataType;
use datafusion::catalog::catalog::CatalogProvider;
use datafusion::{
    datasource::DefaultTableSource,
    error::DataFusionError,
    logical_expr::{AggregateUDF, ScalarUDF, TableSource},
    sql::{planner::ContextProvider, TableReference},
};
use spi::catalog::{DEFAULT_CATALOG, DEFAULT_SCHEMA};
use spi::query::function::FuncMetaManagerRef;
use std::sync::Arc;
use tskv::engine::EngineRef;

pub type MetaDataRef = Arc<dyn MetaData + Send + Sync>;

pub trait MetaData: Send + Sync {
    fn catalog_name(&self) -> String;
    fn schema_name(&self) -> String;
    fn table_provider(&self, name: TableReference) -> Result<Arc<dyn TableSource>>;
    fn catalog(&self) -> CatalogRef;
    fn function(&self) -> FuncMetaManagerRef;
    fn drop_table(&self, name: &str) -> Result<()>;
    fn drop_database(&self, name: &str) -> Result<()>;
}

/// remote meta
pub struct RemoteCatalogMeta {}

/// local meta
#[derive(Clone)]
pub struct LocalCatalogMeta {
    catalog_name: String,
    schema_name: String,
    catalog: UserCatalogRef,
    func_manager: FuncMetaManagerRef,
}

impl LocalCatalogMeta {
    pub fn new_with_default(engine: EngineRef) -> Self {
        Self {
            catalog_name: DEFAULT_CATALOG.to_string(),
            schema_name: DEFAULT_SCHEMA.to_string(),
            catalog: Arc::new(UserCatalog::new(engine)),
            func_manager: Arc::new(SimpleFunctionMetadataManager::default()),
        }
    }
}

impl MetaData for LocalCatalogMeta {
    //todo: local mode dont support multi-tenant

    fn catalog_name(&self) -> String {
        self.catalog_name.clone()
    }

    fn schema_name(&self) -> String {
        self.schema_name.clone()
    }

    fn table_provider(&self, table: TableReference) -> Result<Arc<dyn TableSource>> {
        let catalog_name = self.catalog_name();
        let schema_name = self.schema_name();
        let name = table.resolve(&catalog_name, &schema_name);
        // note: local mod dont support multiple catalog use DEFAULT_CATALOG
        // let catalog_name = name.catalog;
        let schema = match self.catalog.schema(name.schema) {
            None => {
                return Err(Error::DatabaseName {
                    name: name.schema.to_string(),
                });
            }
            Some(s) => s,
        };
        let table = match schema.table(name.table) {
            None => {
                return Err(Error::TableName {
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

    fn function(&self) -> FuncMetaManagerRef {
        self.func_manager.clone()
    }

    fn drop_table(&self, name: &str) -> Result<()> {
        let table: TableReference = name.into();
        let name = table.resolve(self.catalog_name.as_str(), self.schema_name.as_str());
        let schema = self.catalog.schema(name.schema);
        if let Some(db) = schema {
            match db.deregister_table(name.table) {
                Ok(_) => return Ok(()),
                Err(e) => {
                    return Err(TableName {
                        name: name.table.to_string(),
                    })
                }
            }
        }
        Err(DatabaseName {
            name: name.schema.to_string(),
        })
    }

    fn drop_database(&self, name: &str) -> Result<()> {
        self.catalog
            .deregister_schema(name)
            .map_err(|e| DatabaseName {
                name: name.to_string(),
            })
    }
}

pub struct MetadataProvider {
    meta: MetaDataRef,
}

impl MetadataProvider {
    #[inline(always)]
    pub fn new(meta: MetaDataRef) -> Self {
        Self { meta }
    }
}
impl ContextProvider for MetadataProvider {
    fn get_table_provider(
        &self,
        name: TableReference,
    ) -> datafusion::common::Result<Arc<dyn TableSource>> {
        match self.meta.table_provider(name) {
            Ok(table) => Ok(table),
            Err(_) => {
                let catalog_name = self.meta.catalog_name();
                let schema_name = self.meta.schema_name();
                let resolved_name = name.resolve(&catalog_name, &schema_name);
                Err(DataFusionError::Plan(format!(
                    "failed to resolve user:{}  db: {}, table: {}",
                    resolved_name.catalog, resolved_name.schema, resolved_name.table
                )))
            }
        }
    }

    fn get_function_meta(&self, name: &str) -> Option<Arc<ScalarUDF>> {
        self.meta.function().udf(name).ok()
    }

    fn get_aggregate_meta(&self, name: &str) -> Option<Arc<AggregateUDF>> {
        self.meta.function().udaf(name).ok()
    }

    fn get_variable_type(&self, _variable_names: &[String]) -> Option<DataType> {
        // TODO
        None
    }
}
