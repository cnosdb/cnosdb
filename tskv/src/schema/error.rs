use meta::error::MetaError;
use models::schema::tskv_table_schema::ColumnType;
use snafu::{Backtrace, Location, Snafu};

pub type SchemaResult<T> = Result<T, SchemaError>;

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
        #[snafu(implicit)]
        location: Location,
        backtrace: Backtrace,
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
        #[snafu(implicit)]
        location: Location,
        backtrace: Backtrace,
    },

    #[snafu(display("field {} not found", msg))]
    FieldNotFound {
        msg: String,
        #[snafu(implicit)]
        location: Location,
        backtrace: Backtrace,
    },

    #[snafu(display("column '{}' already exists", name))]
    ColumnAlreadyExists {
        name: String,
        #[snafu(implicit)]
        location: Location,
        backtrace: Backtrace,
    },

    #[snafu(display("database '{}' not found", database))]
    DatabaseNotFound {
        database: String,
        #[snafu(implicit)]
        location: Location,
        backtrace: Backtrace,
    },

    #[snafu(display("tenant '{}' not found from meta", tenant))]
    TenantNotFound {
        tenant: String,
        #[snafu(implicit)]
        location: Location,
        backtrace: Backtrace,
    },

    #[snafu(display("database '{}' already exists", database))]
    DatabaseAlreadyExists {
        database: String,
        #[snafu(implicit)]
        location: Location,
        backtrace: Backtrace,
    },

    #[snafu(display("column '{}' not found", column))]
    ColumnNotFound {
        column: String,
        #[snafu(implicit)]
        location: Location,
        backtrace: Backtrace,
    },
}

impl From<MetaError> for SchemaError {
    fn from(value: MetaError) -> Self {
        SchemaError::Meta { source: value }
    }
}
