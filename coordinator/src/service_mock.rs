#![allow(unused_variables)]
use std::fmt::Debug;
use std::sync::Arc;

use meta::meta_client_mock::{MockMetaClient, MockMetaManager};
use meta::{MetaClientRef, MetaRef};
use models::consistency_level::ConsistencyLevel;
use protos::kv_service::{AdminCommandRequest, WritePointsRequest};
use tskv::engine::{EngineRef, MockEngine};
use tskv::iterator::QueryOption;

use crate::errors::CoordinatorResult;
use crate::reader::ReaderIterator;
use crate::VnodeManagerCmdType;

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
        request: WritePointsRequest,
    ) -> CoordinatorResult<()>;

    fn read_record(&self, option: QueryOption) -> CoordinatorResult<ReaderIterator>;

    async fn broadcast_command(&self, req: AdminCommandRequest) -> CoordinatorResult<()>;

    async fn vnode_manager(
        &self,
        tenant: &str,
        cmd_type: VnodeManagerCmdType,
    ) -> CoordinatorResult<()>;
}

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
        req: WritePointsRequest,
    ) -> CoordinatorResult<()> {
        Ok(())
    }

    fn read_record(&self, option: QueryOption) -> CoordinatorResult<ReaderIterator> {
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
}
