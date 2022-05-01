use snafu::{self, Snafu};

use crate::{error, file_manager};

#[derive(Snafu, Debug)]
pub enum RecordFileError {
    #[snafu(display("Error with write file : {}", source))]
    WriteFile {
        source: std::io::Error,
    },

    #[snafu(display("Error with read file : {}", source))]
    ReadFile {
        source: std::io::Error,
    },

    #[snafu(display("Error with open file"))]
    OpenFile,

    #[snafu(display("Error with sync file : {}", source))]
    SyncFile {
        source: std::io::Error,
    },

    #[snafu(display("Error with get file len : {}", source))]
    GetFileLen {
        source: std::io::Error,
    },

    EOF,

    InvalidPos,
}

pub type RecordFileResult<T> = std::result::Result<T, RecordFileError>;
