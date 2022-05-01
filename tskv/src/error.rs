use snafu::Snafu;

use crate::wal;

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("Unable to open file: {}", source))]
    UnableToOpenFile { source: std::io::Error },
    #[snafu(display("Unable to write file: {}", source))]
    UnableToWriteBytes { source: std::io::Error },
    #[snafu(display("Unable to sync file: {}", source))]
    UnableToSyncFile { source: std::io::Error },
    #[snafu(display("File {} has wrong name format to have an id", file_name))]
    InvalidFileName { file_name: String },
    #[snafu(display("{}", source))]
    IO { source: std::io::Error },
    #[snafu(display("async file system stopped"))]
    Cancel,
    #[snafu(display("fails to send to channel"))]
    Send,
    #[snafu(display("fails to receive from channel"))]
    Receive,
    #[snafu(display("invalid flatbuffers: {}", source))]
    InvalidFlatbuffer { source: flatbuffers::InvalidFlatbuffer },
    // #[snafu(display("parse flatbuffers: {}", source))]
    // ParseFlatbuffer { source: ParseFlatbufferError },
    #[snafu(display("unable to write wal: {}", source))]
    WriteAheadLog { source: wal::WalError },
    #[snafu(display("read record file block: {}", source))]
    LogRecordErr { source: crate::record_file::RecordFileError },
    #[snafu(display("read record file block: {}", source))]
    Encode { source: bincode::Error },
    #[snafu(display("read record file block: {}", source))]
    Decode { source: bincode::Error },
    #[snafu(display("error apply edits to summary"))]
    ErrApplyEdit,
}
