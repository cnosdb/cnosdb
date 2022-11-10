use crate::query::execution::Output;
use crate::query::function::FuncMetaManagerRef;
use datafusion::catalog::catalog::CatalogProvider;
use datafusion::catalog::TableReference;
use models::schema::{DatabaseSchema, TableSchema};
use snafu::Snafu;
use std::any::Any;
use std::sync::Arc;

pub type MetaDataRef = Arc<dyn MetaData + Send + Sync>;
pub type Result<T> = std::result::Result<T, MetadataError>;
pub type CatalogRef = Arc<dyn CatalogProvider>;

#[allow(dead_code)]
pub const DEFAULT_DATABASE: &str = "public";
pub const DEFAULT_CATALOG: &str = "cnosdb";

pub trait MetaData: Send + Sync {
    fn as_any(&self) -> &dyn Any;
    fn with_catalog(&self, catalog: &str) -> Arc<dyn MetaData + Send + Sync>;
    fn with_database(&self, database: &str) -> Arc<dyn MetaData + Send + Sync>;
    fn catalog_name(&self) -> String;
    fn schema_name(&self) -> String;
    fn table(&self, name: TableReference) -> Result<TableSchema>;
    fn function(&self) -> FuncMetaManagerRef;
    fn drop_table(&self, name: &str) -> Result<()>;
    fn drop_database(&self, name: &str) -> Result<()>;
    fn create_table(&self, name: &str, table: TableSchema) -> Result<()>;
    fn create_database(&self, name: &str, database: DatabaseSchema) -> Result<()>;
    fn database_names(&self) -> Vec<String>;
    fn describe_database(&self, name: &str) -> Result<Output>;
    fn show_databases(&self) -> Result<Output>;
    fn show_tables(&self, database_name: &Option<String>) -> Result<Output>;
}

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum MetadataError {
    #[snafu(display("External err: {}", message))]
    External { message: String },

    #[snafu(display("Table {} already exists.", table_name))]
    TableAlreadyExists { table_name: String },

    #[snafu(display("Table {} not exists.", table_name))]
    TableNotExists { table_name: String },

    #[snafu(display("Database {} already exists.", database_name))]
    DatabaseAlreadyExists { database_name: String },

    #[snafu(display("Database {} not exists.", database_name))]
    DatabaseNotExists { database_name: String },

    #[snafu(display("Internal Error: {}.", error_msg))]
    InternalError { error_msg: String },

    #[snafu(display("Invalid schema: {}.", error_msg))]
    InvalidSchema { error_msg: String },
}
