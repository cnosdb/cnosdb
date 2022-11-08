use std::fmt::Debug;
use std::sync::Arc;

use config::{ClusterConfig, HintedOffConfig};
use tokio::sync::mpsc;
use tskv::engine::EngineRef;

use crate::errors::*;
use crate::hh_queue::HintedOffManager;
use crate::meta_client::{MetaClientRef, MetaRef, RemoteMetaManager};
use crate::meta_client_mock::MockMetaClient;
use crate::writer::{PointWriter, VnodeMapping};

pub type CoordinatorRef = Arc<dyn Coordinator>;

#[async_trait::async_trait]
pub trait Coordinator: Send + Sync {
    fn tenant_meta(&self, tenant: &String) -> Option<MetaClientRef>;
    async fn write_points(&self, mapping: &mut VnodeMapping<'_>) -> CoordinatorResult<()>;
}

#[derive(Default)]
pub struct MockCoordinator {}

#[async_trait::async_trait]
impl Coordinator for MockCoordinator {
    fn tenant_meta(&self, tenant: &String) -> Option<MetaClientRef> {
        Some(Arc::new(Box::new(MockMetaClient::default())))
    }

    async fn write_points(&self, mapping: &mut VnodeMapping<'_>) -> CoordinatorResult<()> {
        Ok(())
    }
}

pub struct CoordService {
    meta: MetaRef,
    writer: Arc<PointWriter>,
    handoff: Arc<HintedOffManager>,
}
impl CoordService {
    pub fn new(kv_inst: EngineRef, cluster: ClusterConfig, handoff_cfg: HintedOffConfig) -> Self {
        let meta_manager: MetaRef = Arc::new(Box::new(RemoteMetaManager::new(cluster.clone())));
        let (hh_sender, hh_receiver) = mpsc::channel(128);

        let point_writer = Arc::new(PointWriter::new(
            cluster.node_id,
            kv_inst,
            meta_manager.clone(),
            hh_sender,
        ));

        let hh_manager = Arc::new(HintedOffManager::new(
            handoff_cfg.clone(),
            point_writer.clone(),
        ));

        tokio::spawn(HintedOffManager::write_handoff_job(
            hh_manager.clone(),
            hh_receiver,
        ));

        Self {
            meta: meta_manager,
            writer: point_writer,
            handoff: hh_manager,
        }
    }
}
#[async_trait::async_trait]
impl Coordinator for CoordService {
    fn tenant_meta(&self, tenant: &String) -> Option<MetaClientRef> {
        self.meta.tenant_meta(tenant)
    }

    async fn write_points(&self, mapping: &mut VnodeMapping<'_>) -> CoordinatorResult<()> {
        self.writer
            .write_points(mapping, self.handoff.clone())
            .await
    }
}
