use crate::query::function::FuncMetaManagerRef;
use datafusion::catalog::catalog::CatalogProvider;
use datafusion::catalog::TableReference;
use models::schema::{DatabaseSchema, TableColumn, TableSchema};
use snafu::Snafu;
use std::any::Any;
use std::sync::Arc;

pub type MetaDataRef = Arc<dyn MetaData>;
pub type Result<T> = std::result::Result<T, MetadataError>;
pub type CatalogRef = Arc<dyn CatalogProvider>;

#[allow(dead_code)]
pub const DEFAULT_DATABASE: &str = "public";
pub const DEFAULT_CATALOG: &str = "cnosdb";

pub trait MetaData: Send + Sync {
    fn as_any(&self) -> &dyn Any;
    fn with_catalog(&self, catalog: &str) -> Arc<dyn MetaData>;
    fn with_database(&self, database: &str) -> Arc<dyn MetaData>;
    fn catalog_name(&self) -> &str;
    fn schema_name(&self) -> &str;
    fn table(&self, name: TableReference) -> Result<TableSchema>;
    fn database(&self, name: &str) -> Result<DatabaseSchema>;
    fn function(&self) -> FuncMetaManagerRef;
    fn drop_table(&self, name: &str) -> Result<()>;
    fn drop_database(&self, name: &str) -> Result<()>;
    fn create_table(&self, name: &str, table: TableSchema) -> Result<()>;
    fn create_database(&self, name: &str, database: DatabaseSchema) -> Result<()>;
    fn database_names(&self) -> Result<Vec<String>>;
    fn show_tables(&self, database_name: &Option<String>) -> Result<Vec<String>>;
    fn alter_database(&self, database: DatabaseSchema) -> Result<()>;
    fn alter_table_add_column(&self, table_name: &str, column: TableColumn) -> Result<()>;
    fn alter_table_alter_column(
        &self,
        table_name: &str,
        column_name: &str,
        new_column: TableColumn,
    ) -> Result<()>;
    fn alter_table_drop_column(&self, table_name: &str, column_name: &str) -> Result<()>;

    // tenant
    // fn create_tenant(&self, name: String, options: TenantOptions) -> Result<Tenant>;
    // fn alter_tenant(&self, tenant_id: Oid, options: TenantOptions) -> Result<()>;
    // fn tenant(&self, name: &str) -> Result<Tenant>;
    // fn drop_tenant(&self, name: &str) -> Result<bool>;
}

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum MetadataError {
    #[snafu(display("External err: {}", message))]
    External { message: String },

    #[snafu(display("User {} already exists.", user_name))]
    UserAlreadyExists { user_name: String },

    #[snafu(display("Role {} already exists.", role_name))]
    RoleAlreadyExists { role_name: String },

    #[snafu(display("Tenant {} already exists.", tenant_name))]
    TenantAlreadyExists { tenant_name: String },

    #[snafu(display("Table {} already exists.", table_name))]
    TableAlreadyExists { table_name: String },

    #[snafu(display("Table {} not exists.", table_name))]
    TableNotExists { table_name: String },

    #[snafu(display("Table {} is not Tskv table", table_name))]
    TableIsNotTsKv { table_name: String },

    #[snafu(display("Database {} already exists.", database_name))]
    DatabaseAlreadyExists { database_name: String },

    #[snafu(display("Database {} not exists.", database_name))]
    DatabaseNotExists { database_name: String },

    #[snafu(display("Internal Error: {}.", error_msg))]
    InternalError { error_msg: String },

    #[snafu(display("Invalid schema: {}.", error_msg))]
    InvalidSchema { error_msg: String },
}
