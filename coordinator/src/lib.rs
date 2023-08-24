#![feature(stmt_expr_attributes)]
#![feature(arc_unwrap_or_clone)]
#![feature(allocator_api)]

use std::fmt::Debug;
use std::pin::Pin;
use std::sync::Arc;

use datafusion::arrow::record_batch::RecordBatch;
use errors::CoordinatorError;
use futures::Stream;
use meta::model::{MetaClientRef, MetaRef};
use models::meta_data::{ReplicationSet, VnodeAllInfo};
use models::object_reference::ResolvedTable;
use models::predicate::domain::ResolvedPredicateRef;
use models::schema::{Precision, TskvTableSchemaRef};
use protocol_parser::Line;
use protos::kv_service::AdminCommandRequest;
use trace::SpanContext;
use tskv::reader::QueryOption;
use tskv::EngineRef;

use crate::errors::CoordinatorResult;
use crate::service::CoordServiceMetrics;

pub mod errors;
pub mod file_info;
pub mod hh_queue;
pub mod metrics;
pub mod raft_writer;
pub mod reader;
pub mod service;
pub mod service_mock;
pub mod vnode_mgr;
pub mod writer;

pub const FAILED_RESPONSE_CODE: i32 = -1;
pub const FINISH_RESPONSE_CODE: i32 = 0;
pub const SUCCESS_RESPONSE_CODE: i32 = 1;

pub type SendableCoordinatorRecordBatchStream =
    Pin<Box<dyn Stream<Item = CoordinatorResult<RecordBatch>> + Send>>;

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
        Err(errors::CoordinatorError::CommonError {
            msg: "Unreachable".to_string(),
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
    ) -> CoordinatorResult<Vec<ReplicationSet>>;

    async fn write_lines<'a>(
        &self,
        tenant: &str,
        db: &str,
        precision: Precision,
        lines: Vec<Line<'a>>,
        span_ctx: Option<&SpanContext>,
    ) -> CoordinatorResult<usize>;

    async fn write_record_batch<'a>(
        &self,
        table_schema: TskvTableSchemaRef,
        record_batch: RecordBatch,
        span_ctx: Option<&SpanContext>,
    ) -> CoordinatorResult<usize>;

    fn table_scan(
        &self,
        option: QueryOption,
        span_ctx: Option<&SpanContext>,
    ) -> CoordinatorResult<SendableCoordinatorRecordBatchStream>;

    fn tag_scan(
        &self,
        option: QueryOption,
        span_ctx: Option<&SpanContext>,
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

    fn metrics(&self) -> &Arc<CoordServiceMetrics>;
}

async fn get_vnode_all_info(
    meta: MetaRef,
    tenant: &str,
    vnode_id: u32,
) -> CoordinatorResult<VnodeAllInfo> {
    match meta.tenant_meta(tenant).await {
        Some(meta_client) => match meta_client.get_vnode_all_info(vnode_id) {
            Some(all_info) => Ok(all_info),
            None => Err(CoordinatorError::VnodeNotFound { id: vnode_id }),
        },

        None => Err(CoordinatorError::TenantNotFound {
            name: tenant.to_string(),
        }),
    }
}
