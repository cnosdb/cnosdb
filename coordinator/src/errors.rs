use snafu::Snafu;
use std::{fmt::Debug, io};

#[derive(Snafu, Debug)]
#[snafu(visibility(pub))]
pub enum CoordinatorError {
    #[snafu(display("Not Found Field"))]
    NotFoundField,

    #[snafu(display("index storage error: {}", msg))]
    IndexStroage { msg: String },

    #[snafu(display("meta request error: {}", msg))]
    MetaRequest { msg: String },

    #[snafu(display("io error: {}", msg))]
    IOErrors { msg: String },

    #[snafu(display("Invalid serde message: {}", err))]
    InvalidSerdeMsg { err: String },

    // #[snafu(display("fails to receive from channel"))]
    // Receive {
    //     source: tokio::sync::oneshot::error::RecvError,
    // },
    #[snafu(display("fails to send to channel: {}", msg))]
    ChannelSend { msg: String },

    #[snafu(display("fails to recv from channel: {}", msg))]
    ChannelRecv { msg: String },

    #[snafu(display("write vnode error: {}", msg))]
    WriteVnode { msg: String },

    #[snafu(display("Error from models: {}", source))]
    ModelsError { source: models::Error },

    #[snafu(display("Error from tskv: {}", source))]
    TskvError { source: tskv::Error },

    #[snafu(display("Error from tskv index: {}", source))]
    TskvIndexError { source: tskv::index::IndexError },

    #[snafu(display("Error from arrow: {}", source))]
    ArrowErrError {
        source: datafusion::arrow::error::ArrowError,
    },

    #[snafu(display("not found tenant: {}", name))]
    TenantNotFound { name: String },

    #[snafu(display("not found vnode: {}", id))]
    VnodeNotFound { id: u32 },

    #[snafu(display("invalid flatbuffers: {}", source))]
    InvalidFlatbuffer {
        source: flatbuffers::InvalidFlatbuffer,
    },

    #[snafu(display("unknow coordinator command: {}", cmd))]
    UnKnownCoordCmd { cmd: u32 },

    #[snafu(display("coordinator command parse failed"))]
    CoordCommandParseErr,

    #[snafu(display("unexpect response message"))]
    UnExpectResponse,

    #[snafu(display("error msg: {}", msg))]
    CommonError { msg: String },
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

impl From<tskv::index::IndexError> for CoordinatorError {
    fn from(err: tskv::index::IndexError) -> Self {
        CoordinatorError::TskvIndexError { source: err }
    }
}

impl From<models::Error> for CoordinatorError {
    fn from(err: models::Error) -> Self {
        CoordinatorError::ModelsError { source: err }
    }
}

pub type CoordinatorResult<T> = Result<T, CoordinatorError>;
