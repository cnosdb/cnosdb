use error_code::{ErrorCode, ErrorCoder};
use serde::{Deserialize, Serialize};
use snafu::Snafu;

use crate::limiter::limiter_kind::RequestLimiterKind;

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

    #[snafu(display("Module sled error reason: {}", msg))]
    #[error_code(code = 22)]
    SledConflict { msg: String },

    #[snafu(display("{} reached limit", kind))]
    #[error_code(code = 24)]
    RequestLimit { kind: RequestLimiterKind },

    #[snafu(display(
        "An error occurred while processing the data: {}. Please try again",
        msg
    ))]
    #[error_code(code = 25)]
    Retry { msg: String },

    #[snafu(display("{}", msg))]
    ObjectLimit { msg: String },

    #[snafu(display("Connect to Meta error reason: {}", msg))]
    #[error_code(code = 26)]
    ConnectMetaError { msg: String },

    #[snafu(display("Encode/Decode message error reason: {}", err))]
    #[error_code(code = 27)]
    SerdeMsgInvalid { err: String },

    #[snafu(display("Raft group replication error: {}", err))]
    #[error_code(code = 28)]
    ReplicationErr { err: String },

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

    #[snafu(display("Create limiter fail: {}", msg))]
    #[error_code(code = 35)]
    LimiterCreate { msg: String },

    #[snafu(display("Operation request need to send to new address: {}", new_leader))]
    #[error_code(code = 36)]
    ChangeLeader { new_leader: String },

    #[snafu(display("The vnode {} not found", id))]
    #[error_code(code = 37)]
    VnodeNotFound { id: u32 },

    #[snafu(display(
        "Valid node is not enough, need: {}, but found: {}",
        need,
        valid_node_num
    ))]
    #[error_code(code = 53)]
    ValidNodeNotEnough { need: u64, valid_node_num: u32 },
}

impl MetaError {
    pub fn error_code(&self) -> &dyn ErrorCode {
        self
    }
}

impl From<std::io::Error> for MetaError {
    fn from(err: std::io::Error) -> Self {
        MetaError::MetaStoreIO {
            err: err.to_string(),
        }
    }
}

impl From<heed::Error> for MetaError {
    fn from(err: heed::Error) -> Self {
        MetaError::MetaStoreIO {
            err: err.to_string(),
        }
    }
}

impl From<serde_json::Error> for MetaError {
    fn from(e: serde_json::Error) -> Self {
        MetaError::SerdeMsgInvalid { err: e.to_string() }
    }
}

impl From<replication::errors::ReplicationError> for MetaError {
    fn from(err: replication::errors::ReplicationError) -> Self {
        MetaError::ReplicationErr {
            err: err.to_string(),
        }
    }
}

impl warp::reject::Reject for MetaError {}

#[test]
fn test_mod_code() {
    let e = MetaError::NotFoundDb { db: "".to_string() };
    assert!(e.error_code().code().starts_with("03"));
}
