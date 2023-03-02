pub mod errors;
pub mod file_info;
pub mod hh_queue;
pub mod metrics;
pub mod reader;
pub mod service;
pub mod service_mock;
pub mod vnode_mgr;
pub mod writer;

pub const FAILED_RESPONSE_CODE: i32 = -1;
pub const FINISH_RESPONSE_CODE: i32 = 0;
pub const SUCCESS_RESPONSE_CODE: i32 = 1;

#[derive(Debug)]
pub struct WriteRequest {
    pub tenant: String,
    pub level: models::consistency_level::ConsistencyLevel,
    pub request: protos::kv_service::WritePointsRequest,
}

#[derive(Debug, Clone)]
pub enum VnodeManagerCmdType {
    Copy(u32, u64),    // vnode id, dst node id
    Move(u32, u64),    // vnode id, dst node id
    Drop(u32),         // vnode id
    Compact(Vec<u32>), // vnode is list
}

pub fn status_response_to_result(
    status: &protos::kv_service::StatusResponse,
) -> errors::CoordinatorResult<()> {
    if status.code == SUCCESS_RESPONSE_CODE {
        Ok(())
    } else {
        Err(errors::CoordinatorError::GRPCRequest {
            msg: format!("server status: {}, {}", status.code, status.data),
        })
    }
}
