use meta::error::MetaError;
use models::schema::ColumnType;
use snafu::Snafu;

pub type Result<T> = std::result::Result<T, SchemaError>;

#[allow(clippy::large_enum_variant)]
#[derive(Snafu, Debug)]
#[snafu(visibility(pub))]
pub enum SchemaError {
    Meta {
        source: MetaError,
    },

    #[snafu(display("table '{database}.{table}' not found"))]
    TableNotFound {
        database: String,
        table: String,
    },

    #[snafu(display(
        "Column '{}' type error, found {} expected {}",
        column,
        found,
        expected
    ))]
    ColumnTypeError {
        column: String,
        found: ColumnType,
        expected: ColumnType,
    },

    #[snafu(display("field '{database}.{table}'.'{}' not found", field))]
    FieldNotFound {
        database: String,
        table: String,
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

    #[snafu(display("column '{}' not found", column))]
    ColumnNotFound {
        column: String,
    },
}

impl From<MetaError> for SchemaError {
    fn from(value: MetaError) -> Self {
        SchemaError::Meta { source: value }
    }
}
