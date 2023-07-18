use std::error::Error;
use std::io;

use error_code::{ErrorCode, ErrorCoder};
use openraft::{AnyError, ErrorSubject, ErrorVerb, StorageError, StorageIOError};
use serde::{Deserialize, Serialize};
use snafu::Snafu;

use crate::limiter::limiter_kind::RequestLimiterKind;
use crate::{client, ClusterNodeId};

pub type StorageIOResult<T> = Result<T, StorageIOError<ClusterNodeId>>;
pub type StorageResult<T> = Result<T, StorageError<ClusterNodeId>>;
pub type MetaResult<T> = Result<T, MetaError>;

#[derive(Snafu, Serialize, Deserialize, Debug, ErrorCoder)]
#[snafu(visibility(pub))]
#[error_code(mod_code = "03")]
pub enum MetaError {
    #[snafu(display("The member {} of tenant {} already exists", member_name, tenant_name))]
    #[error_code(code = 1)]
    MemberAlreadyExists {
        member_name: String,
        tenant_name: String,
    },

    #[snafu(display("The member {} of tenant {} not found", member_name, tenant_name))]
    #[error_code(code = 2)]
    MemberNotFound {
        member_name: String,
        tenant_name: String,
    },

    #[snafu(display("The privilege {} already exists", name))]
    #[error_code(code = 3)]
    PrivilegeAlreadyExists { name: String },

    #[snafu(display("The privilege {} not found", name))]
    #[error_code(code = 4)]
    PrivilegeNotFound { name: String },

    #[snafu(display("The role {} already exists", role))]
    #[error_code(code = 5)]
    RoleAlreadyExists { role: String },

    #[snafu(display("The role {} not found", role))]
    #[error_code(code = 6)]
    RoleNotFound { role: String },

    #[snafu(display("The user {} already exists", user))]
    #[error_code(code = 7)]
    UserAlreadyExists { user: String },

    #[snafu(display("The user {} not found", user))]
    #[error_code(code = 8)]
    UserNotFound { user: String },

    #[snafu(display("The tenant {} already exists", tenant))]
    #[error_code(code = 9)]
    TenantAlreadyExists { tenant: String },

    #[snafu(display("The tenant {} not found", tenant))]
    #[error_code(code = 10)]
    TenantNotFound { tenant: String },

    #[snafu(display("Not Found Field"))]
    #[error_code(code = 11)]
    NotFoundField,

    #[snafu(display("Not Found DB: {}", db))]
    #[error_code(code = 13)]
    NotFoundDb { db: String },

    #[snafu(display("Not Found Data Node: {}", id))]
    #[error_code(code = 14)]
    NotFoundNode { id: u64 },

    #[snafu(display("Request meta cluster error: {}", msg))]
    #[error_code(code = 15)]
    MetaClientErr { msg: String },

    #[snafu(display("Error: {}", msg))]
    #[error_code(code = 16)]
    CommonError { msg: String },

    #[snafu(display("Database not found: {:?}", database))]
    #[error_code(code = 17)]
    DatabaseNotFound { database: String },

    #[snafu(display("Meta Store Operator Error: {:?}", msg))]
    MetaStoreErr { msg: String },

    #[snafu(display("Database {:?} already exists", database))]
    #[error_code(code = 18)]
    DatabaseAlreadyExists { database: String },

    #[snafu(display("Table not found: {:?}", table))]
    #[error_code(code = 19)]
    TableNotFound { table: String },

    #[snafu(display("Table {} already exists.", table_name))]
    #[error_code(code = 20)]
    TableAlreadyExists { table_name: String },

    #[snafu(display("Module raft error reason: {}", source))]
    #[error_code(code = 21)]
    Raft {
        source: StorageIOError<ClusterNodeId>,
    },

    #[snafu(display("Module sled error reason: {}", msg))]
    #[error_code(code = 22)]
    SledConflict { msg: String },

    #[snafu(display("{} reached limit", kind))]
    #[error_code(code = 24)]
    RequestLimit { kind: RequestLimiterKind },

    #[snafu(display("An error occurred while processing the data. Please try again"))]
    #[error_code(code = 25)]
    Retry,

    #[snafu(display("{}", msg))]
    ObjectLimit { msg: String },
    // RaftRPC{
    //     source: RPCError<ClusterNodeId, ClusterNode, Err>
    // }
    #[snafu(display("Connect to Meta error reason: {}", msg))]
    #[error_code(code = 26)]
    ConnectMetaError { msg: String },

    #[snafu(display("Encode message error reason: {}", err))]
    #[error_code(code = 27)]
    SerdeMsgEncode { err: String },

    #[snafu(display("Decode message error reason: {}", err))]
    #[error_code(code = 28)]
    SerdeMsgDecode { err: String },

    #[snafu(display("Operation meta store io error: {}", err))]
    #[error_code(code = 29)]
    MetaStoreIO { err: String },

    #[snafu(display("Data node already exist: {}", addr))]
    #[error_code(code = 30)]
    DataNodeExist { addr: String },

    #[snafu(display("The bucket {} not found", id))]
    #[error_code(code = 31)]
    BucketNotFound { id: u32 },

    #[snafu(display("database {} attribute invalid!", name))]
    #[error_code(code = 32)]
    DatabaseSchemaInvalid { name: String },

    #[snafu(display("update table {} conflict", name))]
    #[error_code(code = 33)]
    UpdateTableConflict { name: String },

    #[snafu(display("Operation not support: {}", msg))]
    #[error_code(code = 34)]
    NotSupport { msg: String },
}
impl MetaError {
    pub fn error_code(&self) -> &dyn ErrorCode {
        self
    }
}

pub fn sm_r_err<E: Error + 'static>(e: E) -> StorageIOError<ClusterNodeId> {
    StorageIOError::new(
        ErrorSubject::StateMachine,
        ErrorVerb::Read,
        AnyError::new(&e),
    )
}
pub fn sm_w_err<E: Error + 'static>(e: E) -> StorageIOError<ClusterNodeId> {
    StorageIOError::new(
        ErrorSubject::StateMachine,
        ErrorVerb::Write,
        AnyError::new(&e),
    )
}
pub fn s_r_err<E: Error + 'static>(e: E) -> StorageIOError<ClusterNodeId> {
    StorageIOError::new(ErrorSubject::Store, ErrorVerb::Read, AnyError::new(&e))
}
pub fn s_w_err<E: Error + 'static>(e: E) -> StorageIOError<ClusterNodeId> {
    StorageIOError::new(ErrorSubject::Store, ErrorVerb::Write, AnyError::new(&e))
}
pub fn v_r_err<E: Error + 'static>(e: E) -> StorageIOError<ClusterNodeId> {
    StorageIOError::new(ErrorSubject::Vote, ErrorVerb::Read, AnyError::new(&e))
}
pub fn v_w_err<E: Error + 'static>(e: E) -> StorageIOError<ClusterNodeId> {
    StorageIOError::new(ErrorSubject::Vote, ErrorVerb::Write, AnyError::new(&e))
}
pub fn l_r_err<E: Error + 'static>(e: E) -> StorageIOError<ClusterNodeId> {
    StorageIOError::new(ErrorSubject::Logs, ErrorVerb::Read, AnyError::new(&e))
}
pub fn l_w_err<E: Error + 'static>(e: E) -> StorageIOError<ClusterNodeId> {
    StorageIOError::new(ErrorSubject::Logs, ErrorVerb::Write, AnyError::new(&e))
}

pub fn ct_err<E: Error + 'static>(e: E) -> MetaError {
    MetaError::SledConflict { msg: e.to_string() }
}

impl From<StorageIOError<ClusterNodeId>> for MetaError {
    fn from(err: StorageIOError<ClusterNodeId>) -> Self {
        MetaError::MetaStoreIO {
            err: err.to_string(),
        }
    }
}

impl From<io::Error> for MetaError {
    fn from(err: io::Error) -> Self {
        MetaError::CommonError {
            msg: err.to_string(),
        }
    }
}

impl From<client::WriteError> for MetaError {
    fn from(err: client::WriteError) -> Self {
        MetaError::MetaClientErr {
            msg: err.to_string(),
        }
    }
}

#[test]
fn test_mod_code() {
    let e = MetaError::NotFoundDb { db: "".to_string() };
    assert!(e.error_code().code().starts_with("03"));
}
