use std::fmt::Debug;
use std::io;

use sled;
use snafu::Snafu;

use crate::file_system::error::FileSystemError;

#[derive(Snafu, Debug)]
pub enum IndexError {
    #[snafu(display("This feature is not implemented: {}", err))]
    NotImplemented { err: String },

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

    #[snafu(display("Decode Series Key failed for '{}'", msg))]
    DecodeSeriesKey { msg: String },

    #[snafu(display("index storage error: {}", msg))]
    IndexStroage { msg: String },

    #[snafu(display("roaring encode/decode error: {}", source))]
    RoaringBitmap { source: io::Error },

    #[snafu(display("binlog storage error: {}", msg))]
    IOErrors { msg: String },

    #[snafu(display("file error: {}", msg))]
    FileErrors { msg: String },

    #[snafu(display("column '{}' already exists", column))]
    ColumnAlreadyExists { column: String },

    #[snafu(display("series '{}' already exists", key))]
    SeriesAlreadyExists { key: String },

    #[snafu(display("Encode index binlog block failed for '{}'", msg))]
    EncodeIndexBinlog { msg: String },

    #[snafu(display("Decode index binlog block failed for '{}'", msg))]
    DecodeIndexBinlog { msg: String },

    #[snafu(display("file system error: {}", source))]
    FileSystemError { source: FileSystemError },
}

impl From<sled::Error> for IndexError {
    fn from(err: sled::Error) -> Self {
        IndexError::IndexStroage {
            msg: err.to_string(),
        }
    }
}

impl From<io::Error> for IndexError {
    fn from(err: io::Error) -> Self {
        IndexError::IOErrors {
            msg: err.to_string(),
        }
    }
}

pub type IndexResult<T> = Result<T, IndexError>;
