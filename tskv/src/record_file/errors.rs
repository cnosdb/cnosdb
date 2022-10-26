use std::path::PathBuf;

use snafu::{self, Backtrace, Snafu};

use crate::file_system::file_manager;

#[derive(Snafu, Debug)]
#[snafu(module(error), visibility(pub))]
pub enum RecordFileError {
    #[snafu(display("Error with write file : {}", source))]
    WriteFile {
        source: std::io::Error,
        backtrace: Backtrace,
    },

    #[snafu(display("Error with read file : {}", source))]
    ReadFile {
        source: std::io::Error,
        backtrace: Backtrace,
    },

    #[snafu(display("Error with open file '{}': {}", path.display(), source))]
    OpenFile {
        path: PathBuf,
        source: file_manager::FileError,
        backtrace: Backtrace,
    },

    #[snafu(display("Error with sync file : {}", source))]
    SyncFile {
        source: std::io::Error,
        backtrace: Backtrace,
    },

    #[snafu(display("Error with get file len : {}", source))]
    GetFileLen {
        source: std::io::Error,
        backtrace: Backtrace,
    },

    Eof {
        backtrace: Backtrace,
    },

    InvalidPos {
        backtrace: Backtrace,
    },

    InvalidTryInto {
        backtrace: Backtrace,
    },
}

pub type RecordFileResult<T> = Result<T, RecordFileError>;
