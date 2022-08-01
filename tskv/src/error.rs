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

    #[snafu(display("Unable to open file: {}", source))]
    OpenFile { source: std::io::Error },

    #[snafu(display("Error with read file : {}", source))]
    ReadFile { source: std::io::Error },

    #[snafu(display("Unable to write file: {}", source))]
    WriteFile { source: std::io::Error },

    #[snafu(display("Unable to sync file: {}", source))]
    SyncFile { source: std::io::Error },

    #[snafu(display("File {} has wrong name format: {}", file_name, message))]
    InvalidFileName { file_name: String, message: String },

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

    #[snafu(display("read record file block: {}", source))]
    LogRecordErr {
        source: crate::record_file::RecordFileError,
    },

    #[snafu(display("read record file block: {}", source))]
    Encode { source: bincode::Error },

    #[snafu(display("read record file block: {}", source))]
    Decode { source: bincode::Error },

    #[snafu(display("Forward Index: : {}", source))]
    ForwardIndexErr {
        source: crate::forward_index::ForwardIndexError,
    },

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

    #[snafu(display("invalid model: {}", source))]
    InvalidModel { source: models::Error },

    #[snafu(display("invalid tseries id : {}", tf_id))]
    InvalidTsfid { tf_id: u32 },
}
