use snafu::Snafu;

use crate::record_file;
use sled;

#[derive(Snafu, Debug)]
pub enum IndexError {
    #[snafu(display("Unrecognized action"))]
    Action,

    #[snafu(display("Unrecognized version"))]
    Version,

    #[snafu(display("Unrecognized FieldType: {}", msg))]
    FieldType { msg: String },

    #[snafu(display("Not Found Field"))]
    NotFoundField,

    #[snafu(display("Series not exists"))]
    SeriesNotExists,

    #[snafu(display("Decode Series ID List failed"))]
    DecodeSeriesIDList,

    #[snafu(display("Decode TableSchema failed for '{}'", table))]
    DecodeTableSchema { table: String },

    #[snafu(display("index storage error: {}", msg))]
    IndexStroage { msg: String },

    #[snafu(display("table '{}' not found", table))]
    TableNotFound { table: String },

    #[snafu(display("column '{}' already exists", column))]
    ColumnAlreadyExists { column: String },
}

impl From<sled::Error> for IndexError {
    fn from(err: sled::Error) -> Self {
        IndexError::IndexStroage {
            msg: err.to_string(),
        }
    }
}

pub type IndexResult<T> = Result<T, IndexError>;
