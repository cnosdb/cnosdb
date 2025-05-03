use std::fmt::Debug;
use std::io;

use snafu::{Backtrace, Location, Snafu};

use crate::file_system::error::FileSystemError;

#[derive(Snafu, Debug)]
#[snafu(visibility(pub))]
pub enum IndexError {
    #[snafu(display("Series not exists"))]
    SeriesNotExists {
        #[snafu(implicit)]
        location: Location,
        backtrace: Backtrace,
    },

    #[snafu(display("Decode Series ID List failed"))]
    DecodeSeriesIDList {
        #[snafu(implicit)]
        location: Location,
        backtrace: Backtrace,
    },

    #[snafu(display("Decode Series Key failed for '{}'", msg))]
    DecodeSeriesKey {
        msg: String,
        #[snafu(implicit)]
        location: Location,
        backtrace: Backtrace,
    },

    #[snafu(display("index storage error: {}", msg))]
    IndexStorage {
        msg: String,
        #[snafu(implicit)]
        location: Location,
        backtrace: Backtrace,
    },

    #[snafu(display("roaring encode/decode error: {}", source))]
    RoaringBitmap {
        source: io::Error,
        #[snafu(implicit)]
        location: Location,
        backtrace: Backtrace,
    },

    #[snafu(display("binlog storage error: {}", source))]
    IOErrors {
        source: io::Error,
        #[snafu(implicit)]
        location: Location,
        backtrace: Backtrace,
    },

    #[snafu(display("file error: {}", msg))]
    FileErrors {
        msg: String,
        #[snafu(implicit)]
        location: Location,
        backtrace: Backtrace,
    },

    #[snafu(display("column '{}' already exists", column))]
    ColumnAlreadyExists {
        column: String,
        #[snafu(implicit)]
        location: Location,
        backtrace: Backtrace,
    },

    #[snafu(display("series '{}' already exists", key))]
    SeriesAlreadyExists {
        key: String,
        #[snafu(implicit)]
        location: Location,
        backtrace: Backtrace,
    },

    #[snafu(display("Encode index binlog block failed for '{}'", msg))]
    EncodeIndexBinlog {
        msg: String,
        #[snafu(implicit)]
        location: Location,
        backtrace: Backtrace,
    },

    #[snafu(display("Decode index binlog block failed for '{}'", msg))]
    DecodeIndexBinlog {
        msg: String,
        #[snafu(implicit)]
        location: Location,
        backtrace: Backtrace,
    },

    #[snafu(display("file system error: {}", source))]
    FileSystemError { source: FileSystemError },
}

pub type IndexResult<T> = Result<T, IndexError>;
