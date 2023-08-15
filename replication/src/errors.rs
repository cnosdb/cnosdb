use std::fmt::Debug;

use flatbuffers::InvalidFlatbuffer;
use meta::error::MetaError;
use models::error_code::{ErrorCode, ErrorCoder};
use protos::PointsError;
use snafu::Snafu;
use tonic::Status;

#[derive(Snafu, Debug, ErrorCoder)]
#[snafu(visibility(pub))]
#[error_code(mod_code = "06")]
pub enum ReplicationError {
    TskvError {
        source: tskv::Error,
    },

    Meta {
        source: MetaError,
    },

    StateStorageErr {
        source: heed::Error,
    },

    #[snafu(display("Io error: {}", msg))]
    #[error_code(code = 1)]
    IOErrors {
        msg: String,
    },

    #[snafu(display("Fails to send to channel: {}", msg))]
    #[error_code(code = 2)]
    ChannelSend {
        msg: String,
    },

    #[snafu(display("Fails to recv from channel: {}", msg))]
    #[error_code(code = 3)]
    ChannelRecv {
        msg: String,
    },

    #[snafu(display("grpc client request error: {}", msg))]
    #[error_code(code = 4)]
    GRPCRequest {
        msg: String,
    },

    #[snafu(display("message invalid encode/decode failed: {}", msg))]
    #[error_code(code = 5)]
    MsgInvalid {
        msg: String,
    },

    #[snafu(display("ReplicationSet not found: {}", id))]
    #[error_code(code = 6)]
    ReplicationSetNotFound {
        id: u32,
    },

    #[snafu(display("Not enough valid replica of ReplicationSet({})", id))]
    #[error_code(code = 7)]
    NoValidReplica {
        id: u32,
    },

    #[snafu(display("Raft group not init: {}", id))]
    #[error_code(code = 8)]
    GroupNotInit {
        id: u32,
    },

    #[snafu(display("Raft group internal error: {}", msg))]
    #[error_code(code = 9)]
    RaftInternalErr {
        msg: String,
    },

    #[snafu(display("Process message timeout: {}", msg))]
    #[error_code(code = 10)]
    ProcessTimeout {
        msg: String,
    },
}

impl From<std::io::Error> for ReplicationError {
    fn from(err: std::io::Error) -> Self {
        ReplicationError::IOErrors {
            msg: err.to_string(),
        }
    }
}

impl From<tskv::Error> for ReplicationError {
    fn from(err: tskv::Error) -> Self {
        match err {
            tskv::Error::Meta { source } => ReplicationError::Meta { source },

            other => ReplicationError::TskvError { source: other },
        }
    }
}

impl From<heed::Error> for ReplicationError {
    fn from(err: heed::Error) -> Self {
        ReplicationError::StateStorageErr { source: err }
    }
}

impl<T> From<tokio::sync::mpsc::error::SendError<T>> for ReplicationError {
    fn from(err: tokio::sync::mpsc::error::SendError<T>) -> Self {
        ReplicationError::ChannelSend {
            msg: err.to_string(),
        }
    }
}

impl From<tokio::sync::oneshot::error::RecvError> for ReplicationError {
    fn from(err: tokio::sync::oneshot::error::RecvError) -> Self {
        ReplicationError::ChannelRecv {
            msg: err.to_string(),
        }
    }
}

impl From<Status> for ReplicationError {
    fn from(s: Status) -> Self {
        ReplicationError::GRPCRequest {
            msg: format!("grpc status: {}", s),
        }
    }
}

impl From<PointsError> for ReplicationError {
    fn from(e: PointsError) -> Self {
        ReplicationError::MsgInvalid { msg: e.to_string() }
    }
}

impl From<models::Error> for ReplicationError {
    fn from(e: models::Error) -> Self {
        ReplicationError::MsgInvalid { msg: e.to_string() }
    }
}

impl From<InvalidFlatbuffer> for ReplicationError {
    fn from(e: InvalidFlatbuffer) -> Self {
        ReplicationError::MsgInvalid { msg: e.to_string() }
    }
}

impl From<prost::DecodeError> for ReplicationError {
    fn from(e: prost::DecodeError) -> Self {
        ReplicationError::MsgInvalid { msg: e.to_string() }
    }
}

impl From<prost::EncodeError> for ReplicationError {
    fn from(e: prost::EncodeError) -> Self {
        ReplicationError::MsgInvalid { msg: e.to_string() }
    }
}

impl From<bincode::Error> for ReplicationError {
    fn from(e: bincode::Error) -> Self {
        ReplicationError::MsgInvalid { msg: e.to_string() }
    }
}

impl From<std::string::FromUtf8Error> for ReplicationError {
    fn from(e: std::string::FromUtf8Error) -> Self {
        ReplicationError::MsgInvalid { msg: e.to_string() }
    }
}

impl From<serde_json::Error> for ReplicationError {
    fn from(e: serde_json::Error) -> Self {
        ReplicationError::MsgInvalid { msg: e.to_string() }
    }
}

unsafe impl Send for ReplicationError {}
unsafe impl Sync for ReplicationError {}

impl warp::reject::Reject for ReplicationError {}

pub type ReplicationResult<T> = Result<T, ReplicationError>;
