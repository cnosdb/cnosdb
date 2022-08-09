use snafu::Snafu;

use crate::record_file;
use sled;

#[derive(Snafu, Debug)]
pub enum IndexError {
    #[snafu(display("Error with write record file: {}", source))]
    WriteFile {
        source: record_file::RecordFileError,
    },

    #[snafu(display("Error with read record file: {}", source))]
    ReadFile {
        source: record_file::RecordFileError,
    },

    #[snafu(display("Error with write close file: {}", source))]
    CloseFile {
        source: record_file::RecordFileError,
    },

    #[snafu(display("Unrecognized action"))]
    Action,

    #[snafu(display("Unrecognized version"))]
    Version,

    #[snafu(display("Unrecognized FieldType"))]
    FieldType,

    #[snafu(display("Series not exists"))]
    SeriesNotExists,

    #[snafu(display("Decode Series ID List"))]
    DecodeSeriesIDList,

    #[snafu(display("index storage error: {}", msg))]
    IndexStroage { msg: String },
}

impl From<sled::Error> for IndexError {
    fn from(err: sled::Error) -> Self {
        IndexError::IndexStroage {
            msg: err.to_string(),
        }
    }
}

pub type IndexResult<T> = std::result::Result<T, IndexError>;
