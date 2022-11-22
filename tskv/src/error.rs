use std::path::{Path, PathBuf};

use models::SeriesId;
use snafu::Snafu;

use crate::{
    tsm::{ReadTsmError, WriteTsmError},
    wal,
};

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Snafu, Debug)]
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(display("{}", source))]
    IO { source: std::io::Error },

    #[snafu(display("Unable to open file '{}': {}", path.display(), source))]
    OpenFile {
        path: PathBuf,
        source: std::io::Error,
    },

    #[snafu(display("Error with read file '{}': {}", path.display(), source))]
    ReadFile {
        path: PathBuf,
        source: std::io::Error,
    },

    #[snafu(display("Unable to write file '{}': {}", path.display(), source))]
    WriteFile {
        path: PathBuf,
        source: std::io::Error,
    },

    #[snafu(display("Unable to sync file: {}", source))]
    SyncFile { source: std::io::Error },

    #[snafu(display("File {} has wrong name format: {}", file_name, message))]
    InvalidFileName { file_name: String, message: String },

    #[snafu(display("File '{}' has wrong format: {}", path.display(), message))]
    InvalidFileFormat { path: PathBuf, message: String },

    #[snafu(display("async file system stopped"))]
    Cancel,

    #[snafu(display("fails to send to channel"))]
    Send,

    #[snafu(display("fails to receive from channel"))]
    Receive {
        source: tokio::sync::oneshot::error::RecvError,
    },

    #[snafu(display("invalid flatbuffers: {}", source))]
    InvalidFlatbuffer {
        source: flatbuffers::InvalidFlatbuffer,
    },

    #[snafu(display("wal truncated"))]
    WalTruncated,

    #[snafu(display("read/write record file block: {}", reason))]
    RecordFileIo { reason: String },

    #[snafu(display("Unexpected eof"))]
    Eof,

    #[snafu(display("read record file block: {}", source))]
    Encode { source: bincode::Error },

    #[snafu(display("read record file block: {}", source))]
    Decode { source: bincode::Error },

    #[snafu(display("Index: : {}", source))]
    IndexErr { source: crate::index::IndexError },

    #[snafu(display("error apply edits to summary"))]
    ErrApplyEdit,

    #[snafu(display("read tsm block file error: {}", source))]
    ReadTsm { source: ReadTsmError },

    #[snafu(display("write tsm block file error: {}", source))]
    WriteTsm { source: WriteTsmError },

    #[snafu(display("compact tsm block file error: {}", reason))]
    Compact { reason: String },

    #[snafu(display("unable to walk dir: {}", source))]
    UnableToWalkDir { source: walkdir::Error },

    #[snafu(display("database not found: {}", database))]
    DatabaseNotFound { database: String },

    #[snafu(display("database '{}' already exists", database))]
    DatabaseAlreadyExists { database: String },

    #[snafu(display("invalid model: {}", source))]
    InvalidModel { source: models::Error },

    #[snafu(display("invalid tseries id : {}", tf_id))]
    InvalidTsfid { tf_id: u32 },

    #[snafu(display("character set error"))]
    ErrCharacterSet,

    #[snafu(display("panics in thread: {}", reason))]
    ThreadJoin { reason: String },

    #[snafu(display("data fusion RecordBatch::try_new {}", reason))]
    DataFusionNew { reason: String },

    #[snafu(display("can't find field name: {}", reason))]
    NotFoundField { reason: String },

    #[snafu(display("unknown type"))]
    UnKnowType,

    #[snafu(display("can't find tags in point or insert row"))]
    InvalidPoint,

    #[snafu(display("Invalid parameter : {}", reason))]
    InvalidParam { reason: String },

    #[snafu(display("file has no footer"))]
    NoFooter,
}
