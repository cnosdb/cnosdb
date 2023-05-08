#![allow(dead_code, unused_variables)]

use std::fmt::Debug;
use std::sync::Arc;

use datafusion::arrow::record_batch::RecordBatch;
use meta::model::meta_client_mock::{MockMetaClient, MockMetaManager};
use meta::model::{MetaClientRef, MetaRef};
use models::consistency_level::ConsistencyLevel;
use models::schema::Precision;
use protos::kv_service::{AdminCommandRequest, WritePointsRequest};
use tskv::engine_mock::MockEngine;
use tskv::query_iterator::QueryOption;
use tskv::EngineRef;

use crate::errors::CoordinatorResult;
use crate::reader::ReaderIterator;
use crate::{Coordinator, VnodeManagerCmdType, VnodeSummarizerCmdType};

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

    async fn write_points(
        &self,
        tenant: String,
        level: ConsistencyLevel,
        precision: Precision,
        req: WritePointsRequest,
    ) -> CoordinatorResult<()> {
        Ok(())
    }

    fn read_record(&self, option: QueryOption) -> CoordinatorResult<ReaderIterator> {
        let (it, _) = ReaderIterator::new();
        Ok(it)
    }

    fn tag_scan(&self, option: QueryOption) -> CoordinatorResult<ReaderIterator> {
        let (it, _) = ReaderIterator::new();
        Ok(it)
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
}
