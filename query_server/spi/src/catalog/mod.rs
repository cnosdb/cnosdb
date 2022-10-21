use crate::query::execution::Output;
use crate::query::function::FuncMetaManagerRef;
use datafusion::catalog::catalog::CatalogProvider;
use datafusion::catalog::schema::SchemaProvider;
use datafusion::catalog::TableReference;
use datafusion::datasource::TableProvider;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::TableSource;
use models::schema::DatabaseSchema;
use snafu::Snafu;
use std::sync::Arc;

pub type MetaDataRef = Arc<dyn MetaData + Send + Sync>;
pub type Result<T> = std::result::Result<T, MetadataError>;
pub type CatalogRef = Arc<dyn CatalogProvider>;
pub type SchemaRef = Arc<dyn SchemaProvider>;
pub type TableRef = Arc<dyn TableProvider>;

#[allow(dead_code)]
pub const DEFAULT_DATABASE: &str = "public";
pub const DEFAULT_CATALOG: &str = "cnosdb";

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
    fn create_database(&self, name: &str, database: DatabaseSchema) -> Result<()>;
    fn database_names(&self) -> Vec<String>;
    fn describe_database(&self, name: &str) -> Result<Output>;
    fn describe_table(&self, table_name: &str) -> Result<Output>;
    fn show_database(&self) -> Result<Output>;
    fn show_table(&self, database_name: &str) -> Result<Output>;
}

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum MetadataError {
    #[snafu(display("External err: {}", source))]
    External { source: DataFusionError },

    #[snafu(display("Table {} already exists.", table_name))]
    TableAlreadyExists { table_name: String },

    #[snafu(display("Table {} not exists.", table_name))]
    TableNotExists { table_name: String },

    #[snafu(display("Database {} already exists.", database_name))]
    DatabaseAlreadyExists { database_name: String },

    #[snafu(display("Database {} not exists.", database_name))]
    DatabaseNotExists { database_name: String },
}
