#![feature(stmt_expr_attributes)]
use std::fmt::Debug;
use std::pin::Pin;

use datafusion::arrow::record_batch::RecordBatch;
use futures::Stream;
use meta::model::{MetaClientRef, MetaRef};
use models::consistency_level::ConsistencyLevel;
use models::meta_data::VnodeInfo;
use models::object_reference::ResolvedTable;
use models::predicate::domain::ResolvedPredicateRef;
use models::schema::Precision;
use protos::kv_service::{AdminCommandRequest, WritePointsRequest};
use tskv::query_iterator::{QueryOption, TskvSourceMetrics};
use tskv::EngineRef;

use crate::errors::CoordinatorResult;

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

pub type SendableCoordinatorRecordBatchStream = Pin<Box<dyn CoordinatorRecordBatchStream + Send>>;

pub trait CoordinatorRecordBatchStream: Stream<Item = CoordinatorResult<RecordBatch>> {}

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
pub trait Coordinator: Send + Sync {
    fn node_id(&self) -> u64;
    fn meta_manager(&self) -> MetaRef;
    fn store_engine(&self) -> Option<EngineRef>;
    async fn tenant_meta(&self, tenant: &str) -> Option<MetaClientRef>;

    /// get all vnodes of a table to quering
    async fn table_vnodes(
        &self,
        table: &ResolvedTable,
        predicate: ResolvedPredicateRef,
    ) -> CoordinatorResult<Vec<VnodeInfo>>;

    async fn write_points(
        &self,
        tenant: String,
        level: ConsistencyLevel,
        precision: Precision,
        request: WritePointsRequest,
    ) -> CoordinatorResult<()>;

    fn table_scan(
        &self,
        option: QueryOption,
        metrics: TskvSourceMetrics,
    ) -> CoordinatorResult<SendableCoordinatorRecordBatchStream>;

    fn tag_scan(
        &self,
        option: QueryOption,
        metrics: TskvSourceMetrics,
    ) -> CoordinatorResult<SendableCoordinatorRecordBatchStream>;

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
