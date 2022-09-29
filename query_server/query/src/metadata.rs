use crate::catalog::{CatalogRef, UserCatalog, UserCatalogRef};
use datafusion::arrow::datatypes::DataType;
use datafusion::catalog::catalog::CatalogProvider;
use datafusion::datasource::TableProvider;
use datafusion::{
    datasource::DefaultTableSource,
    error::DataFusionError,
    logical_expr::{AggregateUDF, ScalarUDF, TableSource},
    sql::{planner::ContextProvider, TableReference},
};
use snafu::ResultExt;
use spi::catalog::{ExternalSnafu, MetadataError, DEFAULT_CATALOG, DEFAULT_SCHEMA};
use spi::query::function::FuncMetaManagerRef;
use std::sync::Arc;
use tskv::engine::EngineRef;

pub type MetaDataRef = Arc<dyn MetaData + Send + Sync>;

pub type Result<T> = std::result::Result<T, MetadataError>;

pub trait MetaData: Send + Sync {
    fn with_catalog(&self, catalog: &str) -> Arc<dyn MetaData + Send + Sync>;
    fn with_database(&self, database: &str) -> Arc<dyn MetaData + Send + Sync>;
    fn catalog_name(&self) -> String;
    fn schema_name(&self) -> String;
    fn table_provider(&self, name: TableReference) -> Result<Arc<dyn TableSource>>;
    fn catalog(&self) -> CatalogRef;
    fn function(&self) -> FuncMetaManagerRef;
    fn drop_table(&self, name: &str) -> Result<()>;
    fn drop_database(&self, name: &str) -> Result<()>;
    fn create_table(&self, name: &str, table: Arc<dyn TableProvider>) -> Result<()>;
}

/// remote meta
pub struct RemoteCatalogMeta {}

/// local meta
#[derive(Clone)]
pub struct LocalCatalogMeta {
    catalog_name: String,
    database_name: String,
    catalog: UserCatalogRef,
    func_manager: FuncMetaManagerRef,
}

impl LocalCatalogMeta {
    pub fn new_with_default(engine: EngineRef, func_manager: FuncMetaManagerRef) -> Self {
        Self {
            catalog_name: DEFAULT_CATALOG.to_string(),
            database_name: DEFAULT_SCHEMA.to_string(),
            catalog: Arc::new(UserCatalog::new(engine)),
            func_manager,
        }
    }
}

impl MetaData for LocalCatalogMeta {
    fn with_catalog(&self, catalog_name: &str) -> Arc<dyn MetaData + Send + Sync> {
        let mut metadata = self.clone();
        metadata.catalog_name = catalog_name.to_string();

        Arc::new(metadata)
    }

    fn with_database(&self, database: &str) -> Arc<dyn MetaData + Send + Sync> {
        let mut metadata = self.clone();
        metadata.database_name = database.to_string();

        Arc::new(metadata)
    }

    //todo: local mode dont support multi-tenant

    fn catalog_name(&self) -> String {
        self.catalog_name.clone()
    }

    fn schema_name(&self) -> String {
        self.database_name.clone()
    }

    fn table_provider(&self, table: TableReference) -> Result<Arc<dyn TableSource>> {
        let catalog_name = self.catalog_name();
        let schema_name = self.schema_name();
        let name = table.resolve(&catalog_name, &schema_name);
        // note: local mod dont support multiple catalog use DEFAULT_CATALOG
        // let catalog_name = name.catalog;
        let schema = match self.catalog.schema(name.schema) {
            None => {
                return Err(MetadataError::DatabaseNotExists {
                    database_name: name.schema.to_string(),
                });
            }
            Some(s) => s,
        };
        let table = match schema.table(name.table) {
            None => {
                return Err(MetadataError::TableNotExists {
                    table_name: name.table.to_string(),
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
        let name = table.resolve(self.catalog_name.as_str(), self.database_name.as_str());
        let schema = self.catalog.schema(name.schema);
        if let Some(db) = schema {
            return db
                .deregister_table(name.table)
                .map(|_| ())
                .context(ExternalSnafu);
        }

        Err(MetadataError::DatabaseNotExists {
            database_name: name.schema.to_string(),
        })
    }

    fn drop_database(&self, name: &str) -> Result<()> {
        self.catalog
            .deregister_schema(name)
            .map(|_| ())
            .context(ExternalSnafu)
    }

    fn create_table(&self, name: &str, table_provider: Arc<dyn TableProvider>) -> Result<()> {
        let table: TableReference = name.into();
        let table_ref = table.resolve(self.catalog_name.as_str(), self.database_name.as_str());

        self.catalog
            .schema(table_ref.schema)
            .ok_or_else(|| MetadataError::DatabaseNotExists {
                database_name: table_ref.schema.to_string(),
            })?
            // Currently the SchemaProvider creates a temporary table
            .register_table(table.table().to_owned(), table_provider)
            .map(|_| ())
            .context(ExternalSnafu)
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
