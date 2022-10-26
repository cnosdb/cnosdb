use std::num::ParseIntError;
use std::path::{Path, PathBuf};
use std::string::FromUtf8Error;

use snafu::{Backtrace, Snafu};
use tokio::sync::mpsc::error::SendError;

use models::SeriesId;

use crate::{
    tsm::{ReadTsmError, WriteTsmError},
    wal,
};

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Snafu, Debug)]
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(display("{}", source))]
    IO {
        source: std::io::Error,
        backtrace: Backtrace,
    },

    #[snafu(display("Unable to open file '{}': {}", path.display(), source))]
    OpenFile {
        path: PathBuf,
        source: std::io::Error,
        backtrace: Backtrace,
    },

    #[snafu(display("Error with read file: {}", source))]
    ReadFile {
        source: std::io::Error,
        backtrace: Backtrace,
    },

    #[snafu(display("Unable to write file: {}", source))]
    WriteFile {
        source: std::io::Error,
        backtrace: Backtrace,
    },

    #[snafu(display("Unable to sync file: {}", source))]
    SyncFile {
        source: std::io::Error,
        backtrace: Backtrace,
    },

    #[snafu(display("File {} has wrong name format: {}", file_name, message))]
    InvalidFileName {
        file_name: String,
        message: String,
        backtrace: Backtrace,
    },

    #[snafu(display("async file system stopped"))]
    Cancel { backtrace: Backtrace },

    #[snafu(display("fails to send to channel"))]
    Send { backtrace: Backtrace },

    #[snafu(display("fails to receive from channel"))]
    Receive {
        source: tokio::sync::oneshot::error::RecvError,
        backtrace: Backtrace,
    },

    #[snafu(display("invalid flatbuffers: {}", source))]
    InvalidFlatbuffer {
        source: flatbuffers::InvalidFlatbuffer,
        backtrace: Backtrace,
    },

    #[snafu(display("wal truncated"))]
    WalTruncated,

    #[snafu(display("read record file block: {}", source))]
    LogRecordErr {
        source: crate::record_file::RecordFileError,
        backtrace: Backtrace,
    },

    #[snafu(display("read record file block: {}", source))]
    Encode {
        source: bincode::Error,
        backtrace: Backtrace,
    },

    #[snafu(display("read record file block: {}", source))]
    Decode {
        source: bincode::Error,
        backtrace: Backtrace,
    },

    #[snafu(display("Index: : {}", source))]
    IndexErr {
        source: crate::index::IndexError,
        backtrace: Backtrace,
    },

    #[snafu(display("can't create table {} in database {}, because {}", table, db, source))]
    CreateTable {
        db: String,
        table: String,
        source: crate::index::IndexError,
        backtrace: Backtrace,
    },

    #[snafu(display("error apply edits to summary"))]
    ErrApplyEdit,

    #[snafu(display("read tsm block file error: {}", source))]
    ReadTsm {
        source: ReadTsmError,
        backtrace: Backtrace,
    },

    #[snafu(display("write tsm block file error: {}", source))]
    WriteTsm {
        source: WriteTsmError,
        backtrace: Backtrace,
    },

    #[snafu(display("compact tsm block file error: {}", reason))]
    Compact {
        reason: String,
        backtrace: Backtrace,
    },

    #[snafu(display("unable to walk dir: {}", source))]
    UnableToWalkDir {
        source: walkdir::Error,
        backtrace: Backtrace,
    },

    #[snafu(display("database not found: {}", database))]
    DatabaseNotFound {
        database: String,
        backtrace: Backtrace,
    },

    #[snafu(display("database '{}' already exists", database))]
    DatabaseAlreadyExists {
        database: String,
        backtrace: Backtrace,
    },

    #[snafu(display("invalid model: {}", source))]
    InvalidModel {
        source: models::Error,
        backtrace: Backtrace,
    },

    #[snafu(display("invalid tseries id : {}", tf_id))]
    InvalidTsfid { tf_id: u32, backtrace: Backtrace },

    #[snafu(display("character set error"))]
    ErrCharacterSet {
        source: FromUtf8Error,
        backtrace: Backtrace,
    },

    #[snafu(display("panics in thread: {}", reason))]
    ThreadJoin {
        reason: String,
        backtrace: Backtrace,
    },

    #[snafu(display("data fusion RecordBatch::try_new {}", reason))]
    DataFusionNew {
        reason: String,
        backtrace: Backtrace,
    },

    #[snafu(display("can't find field name: {}", field_name))]
    NotFoundField {
        field_name: String,
        backtrace: Backtrace,
    },
}
