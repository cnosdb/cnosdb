use datafusion::error::DataFusionError;
use snafu::Snafu;

#[allow(dead_code)]
pub const DEFAULT_SCHEMA: &str = "public";
pub const DEFAULT_CATALOG: &str = "cnosdb";

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum MetadataError {
    #[snafu(display("External err: {}", source))]
    External { source: DataFusionError },

    #[snafu(display("Table {} already exists.", table_name))]
    TableAlreadyExists { table_name: String },

    #[snafu(display("Table {} not exists.", table_name))]
    TableNotExists { table_name: String },

    #[snafu(display("Database {} not exists.", database_name))]
    DatabaseNotExists { database_name: String },
}
