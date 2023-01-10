use meta::error::MetaError;
use snafu::Snafu;

pub type Result<T> = std::result::Result<T, SchemaError>;

#[allow(clippy::large_enum_variant)]
#[derive(Snafu, Debug)]
#[snafu(visibility(pub))]
pub enum SchemaError {
    Meta {
        source: MetaError,
    },

    #[snafu(display("table '{}' not found", table))]
    TableNotFound {
        table: String,
    },

    #[snafu(display("unrecognized field type {}", field))]
    FieldType {
        field: String,
    },

    #[snafu(display("field not found '{}'", field))]
    NotFoundField {
        field: String,
    },

    #[snafu(display("column '{}' already exists", name))]
    ColumnAlreadyExists {
        name: String,
    },

    #[snafu(display("database '{}' not found", database))]
    DatabaseNotFound {
        database: String,
    },

    #[snafu(display("tenant '{}' not found from meta", tenant))]
    TenantNotFound {
        tenant: String,
    },

    #[snafu(display("database '{}' already exists", database))]
    DatabaseAlreadyExists {
        database: String,
    },
}

impl From<MetaError> for SchemaError {
    fn from(value: MetaError) -> Self {
        SchemaError::Meta { source: value }
    }
}
