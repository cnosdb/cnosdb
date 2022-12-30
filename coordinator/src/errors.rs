use meta::error::MetaError;
use models::error_code::{ErrorCode, ErrorCoder};
use snafu::Snafu;
use std::{fmt::Debug, io};

#[derive(Snafu, Debug, ErrorCoder)]
#[snafu(visibility(pub))]
#[error_code(mod_code = "05")]
pub enum CoordinatorError {
    TskvError {
        source: tskv::Error,
    },

    Meta {
        source: MetaError,
    },

    ArrowErrError {
        source: datafusion::arrow::error::ArrowError,
    },

    #[snafu(display("meta request error: {}", msg))]
    #[error_code(code = 1)]
    MetaRequest {
        msg: String,
    },

    #[snafu(display("io error: {}", msg))]
    #[error_code(code = 2)]
    IOErrors {
        msg: String,
    },

    #[snafu(display("Invalid serde message: {}", err))]
    #[error_code(code = 3)]
    InvalidSerdeMsg {
        err: String,
    },

    #[snafu(display("fails to send to channel: {}", msg))]
    #[error_code(code = 4)]
    ChannelSend {
        msg: String,
    },

    #[snafu(display("fails to recv from channel: {}", msg))]
    #[error_code(code = 5)]
    ChannelRecv {
        msg: String,
    },

    #[snafu(display("write vnode error: {}", msg))]
    #[error_code(code = 6)]
    WriteVnode {
        msg: String,
    },

    #[snafu(display("Error from models: {}", source))]
    #[error_code(code = 7)]
    ModelsError {
        source: models::Error,
    },

    #[snafu(display("Error from tskv index: {}", source))]
    #[error_code(code = 8)]
    TskvIndexError {
        source: tskv::index::IndexError,
    },

    #[snafu(display("not found tenant: {}", name))]
    #[error_code(code = 9)]
    TenantNotFound {
        name: String,
    },

    #[snafu(display("invalid flatbuffers: {}", source))]
    #[error_code(code = 10)]
    InvalidFlatbuffer {
        source: flatbuffers::InvalidFlatbuffer,
    },

    #[snafu(display("unknow coordinator command: {}", cmd))]
    #[error_code(code = 11)]
    UnKnownCoordCmd {
        cmd: u32,
    },

    #[snafu(display("coordinator command parse failed"))]
    #[error_code(code = 12)]
    CoordCommandParseErr,

    #[snafu(display("unexpect response message"))]
    #[error_code(code = 13)]
    UnExpectResponse,

    #[snafu(display("error msg: {}", msg))]
    #[error_code(code = 14)]
    CommonError {
        msg: String,
    },
}

impl From<meta::error::MetaError> for CoordinatorError {
    fn from(err: meta::error::MetaError) -> Self {
        CoordinatorError::MetaRequest {
            msg: err.to_string(),
        }
    }
}

impl From<io::Error> for CoordinatorError {
    fn from(err: io::Error) -> Self {
        CoordinatorError::IOErrors {
            msg: err.to_string(),
        }
    }
}

impl From<tskv::Error> for CoordinatorError {
    fn from(err: tskv::Error) -> Self {
        CoordinatorError::TskvError { source: err }
    }
}

impl From<datafusion::arrow::error::ArrowError> for CoordinatorError {
    fn from(err: datafusion::arrow::error::ArrowError) -> Self {
        CoordinatorError::ArrowErrError { source: err }
    }
}

impl<T> From<tokio::sync::mpsc::error::SendError<T>> for CoordinatorError {
    fn from(err: tokio::sync::mpsc::error::SendError<T>) -> Self {
        CoordinatorError::ChannelSend {
            msg: err.to_string(),
        }
    }
}

impl From<tokio::sync::oneshot::error::RecvError> for CoordinatorError {
    fn from(err: tokio::sync::oneshot::error::RecvError) -> Self {
        CoordinatorError::ChannelRecv {
            msg: err.to_string(),
        }
    }
}

// impl From<tskv::index::IndexError> for CoordinatorError {
//     fn from(err: tskv::index::IndexError) -> Self {
//         CoordinatorError::TskvIndexError { source: err }
//     }
// }

impl From<models::Error> for CoordinatorError {
    fn from(err: models::Error) -> Self {
        CoordinatorError::ModelsError { source: err }
    }
}

impl CoordinatorError {
    pub fn error_code(&self) -> &dyn ErrorCode {
        match self {
            CoordinatorError::Meta { source } => source.error_code(),
            _ => self,
        }
    }
}

pub type CoordinatorResult<T> = Result<T, CoordinatorError>;
