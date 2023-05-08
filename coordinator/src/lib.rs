use std::fmt::Debug;

use datafusion::arrow::record_batch::RecordBatch;
use meta::model::{MetaClientRef, MetaRef};
use models::consistency_level::ConsistencyLevel;
use models::schema::Precision;
use protos::kv_service::{AdminCommandRequest, WritePointsRequest};
use tskv::query_iterator::QueryOption;
use tskv::EngineRef;

use crate::errors::CoordinatorResult;
use crate::reader::ReaderIterator;

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
    pub precision: Precision,
    pub request: protos::kv_service::WritePointsRequest,
}

#[derive(Debug, Clone)]
pub enum VnodeManagerCmdType {
    /// vnode id, dst node id
    Copy(u32, u64),
    /// vnode id, dst node id
    Move(u32, u64),
    /// vnode id
    Drop(u32),
    /// vnode id list
    Compact(Vec<u32>),
}

#[derive(Debug, Clone)]
pub enum VnodeSummarizerCmdType {
    /// replication set id
    Checksum(u32),
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

#[async_trait::async_trait]
pub trait Coordinator: Send + Sync + Debug {
    fn node_id(&self) -> u64;
    fn meta_manager(&self) -> MetaRef;
    fn store_engine(&self) -> Option<EngineRef>;
    async fn tenant_meta(&self, tenant: &str) -> Option<MetaClientRef>;

    async fn write_points(
        &self,
        tenant: String,
        level: ConsistencyLevel,
        precision: Precision,
        request: WritePointsRequest,
    ) -> CoordinatorResult<()>;

    fn read_record(&self, option: QueryOption) -> CoordinatorResult<ReaderIterator>;

    fn tag_scan(&self, option: QueryOption) -> CoordinatorResult<ReaderIterator>;

    async fn broadcast_command(&self, req: AdminCommandRequest) -> CoordinatorResult<()>;

    /// A manager to manage vnode.
    async fn vnode_manager(
        &self,
        tenant: &str,
        cmd_type: VnodeManagerCmdType,
    ) -> CoordinatorResult<()>;

    /// A summarizer to summarize vnode info.
    async fn vnode_summarizer(
        &self,
        tenant: &str,
        cmd_type: VnodeSummarizerCmdType,
    ) -> CoordinatorResult<Vec<RecordBatch>>;
}
