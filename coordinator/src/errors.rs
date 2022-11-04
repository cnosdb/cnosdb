use crate::meta_client;
use snafu::Snafu;
use std::io;

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

    #[snafu(display("fails to receive from channel"))]
    Receive {
        source: tokio::sync::oneshot::error::RecvError,
    },

    #[snafu(display("write vnode error: {}", msg))]
    WriteVnode { msg: String },

    #[snafu(display("Error from tskv: {}", source))]
    TskvError { source: tskv::Error },

    #[snafu(display("not found tenant: {}", name))]
    TenantNotFound { name: String },

    #[snafu(display("unknow coordinator command: {}", cmd))]
    UnKnownCoordCmd { cmd: u32 },

    #[snafu(display("coordinator command parse failed"))]
    CoordCmmandParseErr,

    #[snafu(display("unexpect response message"))]
    UnExpectResponse,
}

impl From<meta_client::MetaError> for CoordinatorError {
    fn from(err: meta_client::MetaError) -> Self {
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

pub type CoordinatorResult<T> = Result<T, CoordinatorError>;
