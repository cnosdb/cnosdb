use std::fmt::Debug;

use derive_traits::{ErrorCode, ErrorCoder};
use models::meta_data::ReplicationSetId;
use protos::PointsError;
use snafu::{Backtrace, Location, Snafu};

#[derive(Snafu, Debug, ErrorCoder)]
#[snafu(visibility(pub))]
#[error_code(mod_code = "06")]
pub enum ReplicationError {
    #[snafu(display("storage operation error: {}", msg))]
    #[error_code(code = 1)]
    StorageErr {
        msg: String,
        #[snafu(implicit)]
        location: Location,
        backtrace: Backtrace,
    },

    #[snafu(display("head error: {}", source))]
    #[error_code(code = 2)]
    HeedError {
        source: heed::Error,
        #[snafu(implicit)]
        location: Location,
        backtrace: Backtrace,
    },

    #[snafu(display("io error: {}", source))]
    #[error_code(code = 3)]
    IOErr {
        source: std::io::Error,
        #[snafu(implicit)]
        location: Location,
        backtrace: Backtrace,
    },

    #[snafu(display("grpc client request error: {}", msg))]
    #[error_code(code = 4)]
    GRPCRequest {
        msg: String,
        #[snafu(implicit)]
        location: Location,
        backtrace: Backtrace,
    },

    #[snafu(display("message invalid encode/decode failed: {}", msg))]
    #[error_code(code = 5)]
    MsgInvalid {
        msg: String,
        #[snafu(implicit)]
        location: Location,
        backtrace: Backtrace,
    },

    #[snafu(display("ReplicationSet not found: {}", id))]
    #[error_code(code = 6)]
    ReplicationSetNotFound {
        id: u32,
        #[snafu(implicit)]
        location: Location,
        backtrace: Backtrace,
    },

    #[snafu(display("Not enough valid replica of ReplicationSet({})", id))]
    #[error_code(code = 7)]
    NoValidReplica {
        id: u32,
        #[snafu(implicit)]
        location: Location,
        backtrace: Backtrace,
    },

    #[snafu(display("Raft group not init: {}", id))]
    #[error_code(code = 8)]
    GroupNotInit {
        id: u32,
        #[snafu(implicit)]
        location: Location,
        backtrace: Backtrace,
    },

    #[snafu(display("Raft group internal error: {}", msg))]
    #[error_code(code = 9)]
    RaftInternalErr { msg: String },

    #[snafu(display("Process message timeout: {}", msg))]
    #[error_code(code = 10)]
    ProcessTimeout {
        msg: String,
        #[snafu(implicit)]
        location: Location,
        backtrace: Backtrace,
    },

    #[snafu(display("Apply engine failed: {}", msg))]
    #[error_code(code = 11)]
    ApplyEngineErr { msg: String },

    #[snafu(display("Get/Create snapshot failed: {}", msg))]
    #[error_code(code = 12)]
    SnapshotErr {
        msg: String,
        #[snafu(implicit)]
        location: Location,
        backtrace: Backtrace,
    },

    #[snafu(display("Restore snapshot failed: {}", msg))]
    #[error_code(code = 13)]
    RestoreSnapshotErr { msg: String },

    #[snafu(display("Destroy raft node failed: {}", msg))]
    #[error_code(code = 14)]
    DestroyRaftNodeErr { msg: String },

    #[snafu(display("Can't found entry by index: {}", index))]
    #[error_code(code = 15)]
    EntryNotFound {
        index: u64,
        #[snafu(implicit)]
        location: Location,
        backtrace: Backtrace,
    },

    #[snafu(display("Raft node already shutdown: {}", id))]
    #[error_code(code = 16)]
    AlreadyShutdown { id: ReplicationSetId },

    #[snafu(display("points error: {}", source))]
    #[error_code(code = 17)]
    PointsError { source: PointsError },

    #[snafu(display("models error: {}", source))]
    #[error_code(code = 18)]
    ModelError { source: models::ModelError },
}

impl ReplicationError {
    pub fn error_code(&self) -> &dyn ErrorCode {
        self
    }
}

unsafe impl Send for ReplicationError {}
unsafe impl Sync for ReplicationError {}

pub type ReplicationResult<T> = Result<T, ReplicationError>;
