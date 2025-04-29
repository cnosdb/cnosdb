use std::path::PathBuf;

use snafu::{Backtrace, Location, Snafu};

pub type FileSystemResult<T> = Result<T, FileSystemError>;

#[derive(Snafu, Debug)]
#[snafu(visibility(pub))]
pub enum FileSystemError {
    #[snafu(display("File error: {:?}", source))]
    StdIOError {
        source: std::io::Error,
        #[snafu(implicit)]
        location: Location,
        backtrace: Backtrace,
    },

    #[snafu(display("Unable to open file '{}': {}", path.display(), source))]
    UnableToOpenFile {
        path: PathBuf,
        source: std::io::Error,
        #[snafu(implicit)]
        location: Location,
        backtrace: Backtrace,
    },

    #[snafu(display("Unable to write file '{}': {}", path.display(), source))]
    UnableToWriteBytes {
        path: PathBuf,
        source: std::io::Error,
        #[snafu(implicit)]
        location: Location,
        backtrace: Backtrace,
    },

    #[snafu(display("Unable to sync file '{}': {}", path.display(), source))]
    UnableToSyncFile {
        path: PathBuf,
        source: std::io::Error,
        #[snafu(implicit)]
        location: Location,
        backtrace: Backtrace,
    },

    #[snafu(display("async file system stopped"))]
    Cancel {
        #[snafu(implicit)]
        location: Location,
        backtrace: Backtrace,
    },
}
