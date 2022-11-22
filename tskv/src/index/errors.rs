use snafu::Snafu;

use crate::record_file;
use sled;

#[derive(Snafu, Debug)]
pub enum IndexError {
    #[snafu(display("Unrecognized action"))]
    Action,

    #[snafu(display("Unrecognized version"))]
    Version,

    #[snafu(display("Unrecognized FieldType"))]
    FieldType,

    #[snafu(display("Not Found Field"))]
    NotFoundField,

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

pub type IndexResult<T> = Result<T, IndexError>;
