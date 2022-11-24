use snafu::Snafu;

pub type Result<T> = std::result::Result<T, SchemaError>;

#[derive(Snafu, Debug)]
pub enum SchemaError {
    #[snafu(display("Error with apply to meta: {}", message))]
    MetaError {
        message: String,
    },

    #[snafu(display("table '{}' not found", table))]
    TableNotFound { table: String },

    #[snafu(display("Unrecognized FieldType"))]
    FieldType,

    #[snafu(display("Not Found Field"))]
    NotFoundField,

    #[snafu(display("Column {} already exists", name))]
    ColumnAlreadyExists{ name: String },
}

