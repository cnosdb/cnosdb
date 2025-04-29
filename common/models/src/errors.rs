use std::fmt::Debug;
use std::io;

use arrow_schema::ArrowError;
use datafusion::error::DataFusionError;
use protos::PointsError;
use snafu::{Backtrace, IntoError, Location, Snafu};

pub type ModelResult<T, E = ModelError> = std::result::Result<T, E>;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum ModelError {
    Datafusion {
        source: DataFusionError,
        #[snafu(implicit)]
        location: Location,
        backtrace: Backtrace,
    },

    #[snafu(display("Arrow error: {}", source))]
    Arrow {
        source: ArrowError,
        #[snafu(implicit)]
        location: Location,
        backtrace: Backtrace,
    },

    #[snafu(display("Invalid point: {}", source))]
    InvalidPoint { source: PointsError },

    #[snafu(display("Invalid tag: {}", err))]
    InvalidTag {
        err: String,
        #[snafu(implicit)]
        location: Location,
        backtrace: Backtrace,
    },

    #[snafu(display("Invalid field: {}", err))]
    InvalidField {
        err: String,
        #[snafu(implicit)]
        location: Location,
        backtrace: Backtrace,
    },

    #[snafu(display("Invalid flatbuffer message: {}", err))]
    InvalidFlatbufferMessage {
        err: String,
        #[snafu(implicit)]
        location: Location,
        backtrace: Backtrace,
    },

    #[snafu(display("Invalid serde message: {}", source))]
    InvalidSerdeMessage {
        source: bincode::Error,
        #[snafu(implicit)]
        location: Location,
        backtrace: Backtrace,
    },

    #[snafu(display("Invalid query expr message: {}", source))]
    InvalidQueryExprMsg {
        source: bincode::Error,
        #[snafu(implicit)]
        location: Location,
        backtrace: Backtrace,
    },

    #[snafu(display(
        "Internal error: {}. This was likely caused by a bug in Cnosdb's \
    code and we would welcome that you file an bug report in our issue tracker",
        err
    ))]
    Internal {
        err: String,
        #[snafu(implicit)]
        location: Location,
        backtrace: Backtrace,
    },

    #[snafu(display("IO operator: {}", source))]
    IOErrors {
        source: io::Error,
        #[snafu(implicit)]
        location: Location,
        backtrace: Backtrace,
    },

    #[snafu(display("Failed to convert vec to string, because: {}", msg))]
    EncodingError {
        msg: String,
        #[snafu(implicit)]
        location: Location,
        backtrace: Backtrace,
    },

    #[snafu(display("RecordBatch is None"))]
    NoneRecordBatch {
        #[snafu(implicit)]
        location: Location,
        backtrace: Backtrace,
    },

    #[snafu(display("Dump Error: {}", msg))]
    DumpError {
        msg: String,
        #[snafu(implicit)]
        location: Location,
        backtrace: Backtrace,
    },

    #[snafu(display("{msg}"))]
    Common {
        msg: String,
        #[snafu(implicit)]
        location: Location,
        backtrace: Backtrace,
    },
}

impl From<DataFusionError> for ModelError {
    fn from(value: DataFusionError) -> Self {
        match value {
            DataFusionError::External(e) if e.downcast_ref::<ArrowError>().is_some() => {
                let arrow_error = *e.downcast::<ArrowError>().unwrap();
                ArrowSnafu.into_error(arrow_error)
            }

            DataFusionError::External(e) if e.downcast_ref::<DataFusionError>().is_some() => {
                let datafusion_error = *e.downcast::<DataFusionError>().unwrap();
                Self::from(datafusion_error)
            }

            DataFusionError::External(e) if e.downcast_ref::<Self>().is_some() => {
                match *e.downcast::<Self>().unwrap() {
                    Self::Datafusion { source, .. } => Self::from(source),
                    e => e,
                }
            }

            DataFusionError::ArrowError(e, _backtrace) => ArrowSnafu.into_error(e),
            v => DatafusionSnafu.into_error(v),
        }
    }
}

pub fn tuple_err<T, R, E>(value: (ModelResult<T, E>, ModelResult<R, E>)) -> ModelResult<(T, R), E> {
    match value {
        (Ok(e), Ok(e1)) => Ok((e, e1)),
        (Err(e), Ok(_)) => Err(e),
        (Ok(_), Err(e1)) => Err(e1),
        (Err(e), Err(_)) => Err(e),
    }
}

pub fn check_err(r: libc::c_int) -> io::Result<libc::c_int> {
    #[cfg(windows)]
    {
        if r == 0 {
            Err(io::Error::last_os_error())
        } else {
            Ok(r)
        }
    }
    #[cfg(not(windows))]
    {
        if r == -1 {
            Err(io::Error::last_os_error())
        } else {
            Ok(r)
        }
    }
}
