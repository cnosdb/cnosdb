use std::fmt::Debug;

use flatbuffers::InvalidFlatbuffer;
use models::error_code::{ErrorCode, ErrorCoder};
use protos::PointsError;
use serde::{Deserialize, Serialize};
use snafu::Snafu;
use tonic::Status;

#[derive(Snafu, Serialize, Deserialize, Debug, ErrorCoder)]
#[snafu(visibility(pub))]
#[error_code(mod_code = "06")]
pub enum ReplicationError {
    #[snafu(display("stroage operation error: {}", msg))]
    #[error_code(code = 1)]
    StorageErr { msg: String },

    #[snafu(display("grpc client request error: {}", msg))]
    #[error_code(code = 4)]
    GRPCRequest { msg: String },

    #[snafu(display("message invalid encode/decode failed: {}", msg))]
    #[error_code(code = 5)]
    MsgInvalid { msg: String },

    #[snafu(display("ReplicationSet not found: {}", id))]
    #[error_code(code = 6)]
    ReplicationSetNotFound { id: u32 },

    #[snafu(display("Not enough valid replica of ReplicationSet({})", id))]
    #[error_code(code = 7)]
    NoValidReplica { id: u32 },

    #[snafu(display("Raft group not init: {}", id))]
    #[error_code(code = 8)]
    GroupNotInit { id: u32 },

    #[snafu(display("Raft group internal error: {}", msg))]
    #[error_code(code = 9)]
    RaftInternalErr { msg: String },

    #[snafu(display("Process message timeout: {}", msg))]
    #[error_code(code = 10)]
    ProcessTimeout { msg: String },

    #[snafu(display("Apply engine failed: {}", msg))]
    #[error_code(code = 11)]
    ApplyEngineErr { msg: String },

    #[snafu(display("Create snapshot failed: {}", msg))]
    #[error_code(code = 12)]
    CreateSnapshotErr { msg: String },

    #[snafu(display("Restore snapshot failed: {}", msg))]
    #[error_code(code = 13)]
    RestoreSnapshotErr { msg: String },

    #[snafu(display("Destory raft node failed: {}", msg))]
    #[error_code(code = 14)]
    DestoryRaftNodeErr { msg: String },
}

impl From<std::io::Error> for ReplicationError {
    fn from(err: std::io::Error) -> Self {
        ReplicationError::StorageErr {
            msg: err.to_string(),
        }
    }
}

impl From<heed::Error> for ReplicationError {
    fn from(err: heed::Error) -> Self {
        ReplicationError::StorageErr {
            msg: err.to_string(),
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
