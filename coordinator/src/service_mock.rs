#![allow(dead_code, unused_variables)]

use std::fmt::Debug;
use std::sync::Arc;
use std::todo;

use datafusion::arrow::record_batch::RecordBatch;
use meta::model::meta_client_mock::{MockMetaClient, MockMetaManager};
use meta::model::{MetaClientRef, MetaRef};
use models::consistency_level::ConsistencyLevel;
use models::meta_data::{VnodeInfo, VnodeStatus};
use models::object_reference::ResolvedTable;
use models::predicate::domain::ResolvedPredicateRef;
use models::schema::Precision;
use protos::kv_service::{AdminCommandRequest, WritePointsRequest};
use trace::SpanContext;
use tskv::engine_mock::MockEngine;
use tskv::query_iterator::QueryOption;
use tskv::EngineRef;

use crate::errors::CoordinatorResult;
use crate::service::CoordServiceMetrics;
use crate::{
    Coordinator, SendableCoordinatorRecordBatchStream, VnodeManagerCmdType, VnodeSummarizerCmdType,
};

pub const WITH_NONEMPTY_DATABASE_FOR_TEST: &str = "with_nonempty_database";

#[derive(Debug, Default)]
pub struct MockCoordinator {}

#[async_trait::async_trait]
impl Coordinator for MockCoordinator {
    fn node_id(&self) -> u64 {
        0
    }

    fn meta_manager(&self) -> MetaRef {
        Arc::new(MockMetaManager::default())
    }

    fn store_engine(&self) -> Option<EngineRef> {
        Some(Arc::new(MockEngine::default()))
    }

    async fn tenant_meta(&self, tenant: &str) -> Option<MetaClientRef> {
        Some(Arc::new(MockMetaClient::default()))
    }

    async fn table_vnodes(
        &self,
        table: &ResolvedTable,
        _predicate: ResolvedPredicateRef,
    ) -> CoordinatorResult<Vec<VnodeInfo>> {
        if table.database() == WITH_NONEMPTY_DATABASE_FOR_TEST {
            return Ok(vec![
                VnodeInfo {
                    id: 0,
                    node_id: 0,
                    status: VnodeStatus::Running,
                },
                VnodeInfo {
                    id: 1,
                    node_id: 0,
                    status: VnodeStatus::Running,
                },
                VnodeInfo {
                    id: 2,
                    node_id: 0,
                    status: VnodeStatus::Running,
                },
                VnodeInfo {
                    id: 3,
                    node_id: 0,
                    status: VnodeStatus::Running,
                },
                VnodeInfo {
                    id: 4,
                    node_id: 0,
                    status: VnodeStatus::Running,
                },
                VnodeInfo {
                    id: 5,
                    node_id: 0,
                    status: VnodeStatus::Running,
                },
                VnodeInfo {
                    id: 6,
                    node_id: 0,
                    status: VnodeStatus::Running,
                },
                VnodeInfo {
                    id: 7,
                    node_id: 0,
                    status: VnodeStatus::Running,
                },
            ]);
        }
        Ok(vec![])
    }

    async fn write_points(
        &self,
        tenant: String,
        level: ConsistencyLevel,
        precision: Precision,
        req: WritePointsRequest,
        _span_ctx: Option<&SpanContext>,
    ) -> CoordinatorResult<()> {
        Ok(())
    }

    fn table_scan(
        &self,
        option: QueryOption,
        _span_ctx: Option<&SpanContext>,
    ) -> CoordinatorResult<SendableCoordinatorRecordBatchStream> {
        // TODO
        todo!()
    }

    fn tag_scan(
        &self,
        option: QueryOption,
        _span_ctx: Option<&SpanContext>,
    ) -> CoordinatorResult<SendableCoordinatorRecordBatchStream> {
        todo!("tag_scan")
    }

    async fn broadcast_command(&self, req: AdminCommandRequest) -> CoordinatorResult<()> {
        Ok(())
    }

    async fn vnode_manager(
        &self,
        tenant: &str,
        cmd_type: VnodeManagerCmdType,
    ) -> CoordinatorResult<()> {
        Ok(())
    }

    async fn vnode_summarizer(
        &self,
        tenant: &str,
        cmd_type: VnodeSummarizerCmdType,
    ) -> CoordinatorResult<Vec<RecordBatch>> {
        Ok(vec![])
    }

    fn metrics(&self) -> &Arc<CoordServiceMetrics> {
        todo!()
    }
}
