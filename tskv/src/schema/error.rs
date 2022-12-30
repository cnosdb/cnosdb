use meta::error::MetaError;
use snafu::Snafu;

pub type Result<T> = std::result::Result<T, SchemaError>;

#[allow(clippy::large_enum_variant)]
#[derive(Snafu, Debug)]
#[snafu(visibility(pub))]
pub enum SchemaError {
    #[snafu(display("Error with apply to meta: {}", source))]
    Meta { source: MetaError },

    #[snafu(display("table '{}' not found", table))]
    TableNotFound { table: String },

    #[snafu(display("Unrecognized FieldType"))]
    FieldType,

    #[snafu(display("Not Found Field"))]
    NotFoundField,

    #[snafu(display("Column {} already exists", name))]
    ColumnAlreadyExists { name: String },

    #[snafu(display("database '{}' not found", database))]
    DatabaseNotFound { database: String },

    #[snafu(display("tenant '{}' not found from meta", tenant))]
    TenantNotFound { tenant: String },
}
