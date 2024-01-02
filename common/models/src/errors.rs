use std::fmt::Debug;
use std::io;
use std::string::FromUtf8Error;

use arrow_schema::ArrowError;
use datafusion::error::DataFusionError;
use snafu::Snafu;

#[macro_export]
macro_rules! define_result {
    ($t:ty) => {
        pub type Result<T, E = $t> = std::result::Result<T, E>;
    };
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum Error {
    Datafusion {
        source: DataFusionError,
    },

    Arrow {
        source: ArrowError,
    },

    #[snafu(display("Invalid point: {}", err))]
    InvalidPoint {
        err: String,
    },

    #[snafu(display("Invalid tag: {}", err))]
    InvalidTag {
        err: String,
    },

    #[snafu(display("Invalid field: {}", err))]
    InvalidField {
        err: String,
    },

    #[snafu(display("Invalid flatbuffer message: {}", err))]
    InvalidFlatbufferMessage {
        err: String,
    },

    #[snafu(display("Invalid serde message: {}", err))]
    InvalidSerdeMessage {
        err: String,
    },

    #[snafu(display("Invalid query expr message: {}", err))]
    InvalidQueryExprMsg {
        err: String,
    },

    #[snafu(display(
        "Internal error: {}. This was likely caused by a bug in Cnosdb's \
    code and we would welcome that you file an bug report in our issue tracker",
        err
    ))]
    Internal {
        err: String,
    },

    #[snafu(display("IO operator: {}", err))]
    IOErrors {
        err: String,
    },

    #[snafu(display("Failed to convert vec to string"))]
    EncodingError,

    #[snafu(display("RecordBatch is None"))]
    NoneRecordBatch,

    #[snafu(display("Dump Error: {}", msg))]
    DumpError {
        msg: String,
    },

    #[snafu(display("{msg}"))]
    Common {
        msg: String,
    },
}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Self {
        Error::IOErrors {
            err: err.to_string(),
        }
    }
}

impl From<FromUtf8Error> for Error {
    fn from(_: FromUtf8Error) -> Self {
        Error::EncodingError
    }
}

impl From<DataFusionError> for Error {
    fn from(value: DataFusionError) -> Self {
        match value {
            DataFusionError::External(e) if e.downcast_ref::<ArrowError>().is_some() => {
                let arrow_error = *e.downcast::<ArrowError>().unwrap();
                Self::from(arrow_error)
            }

            DataFusionError::External(e) if e.downcast_ref::<DataFusionError>().is_some() => {
                let datafusion_error = *e.downcast::<DataFusionError>().unwrap();
                Self::from(datafusion_error)
            }

            DataFusionError::External(e) if e.downcast_ref::<Self>().is_some() => {
                match *e.downcast::<Self>().unwrap() {
                    Self::Arrow { source } => Self::from(source),
                    Self::Datafusion { source } => Self::from(source),
                    e => e,
                }
            }

            DataFusionError::ArrowError(e) => Self::from(e),
            v => Self::Datafusion { source: v },
        }
    }
}

impl From<ArrowError> for Error {
    fn from(value: ArrowError) -> Self {
        Self::Arrow { source: value }
    }
}

pub fn tuple_err<T, R, E>(value: (Result<T, E>, Result<R, E>)) -> Result<(T, R), E> {
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
