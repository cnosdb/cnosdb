use std::fmt::Debug;
use std::sync::Arc;

use config::{ClusterConfig, HintedOffConfig};
use models::consistency_level::ConsistencyLevel;
use protos::kv_service::WritePointsRpcRequest;
use snafu::ResultExt;
use tokio::sync::mpsc::{self, Receiver, Sender};
use tokio::sync::oneshot;
use tskv::engine::EngineRef;

use crate::command::{CoordinatorIntCmd, WritePointsRequest};
use crate::errors::*;
use crate::hh_queue::HintedOffManager;
use crate::meta_client::{MetaClientRef, MetaRef, RemoteMetaManager};
use crate::meta_client_mock::MockMetaClient;
use crate::writer::{PointWriter, VnodeMapping};

pub type CoordinatorRef = Arc<dyn Coordinator>;

#[async_trait::async_trait]
pub trait Coordinator: Send + Sync {
    fn tenant_meta(&self, tenant: &String) -> Option<MetaClientRef>;

    async fn write_points(
        &self,
        tenant: String,
        level: ConsistencyLevel,
        request: WritePointsRpcRequest,
    ) -> CoordinatorResult<()>;
}

#[derive(Default)]
pub struct MockCoordinator {}

#[async_trait::async_trait]
impl Coordinator for MockCoordinator {
    fn tenant_meta(&self, tenant: &String) -> Option<MetaClientRef> {
        Some(Arc::new(MockMetaClient::default()))
    }

    async fn write_points(
        &self,
        tenant: String,
        level: ConsistencyLevel,
        req: WritePointsRpcRequest,
    ) -> CoordinatorResult<()> {
        Ok(())
    }
}

pub struct CoordService {
    meta: MetaRef,
    writer: Arc<PointWriter>,
    handoff: Arc<HintedOffManager>,
    coord_sender: Sender<CoordinatorIntCmd>,
}
impl CoordService {
    pub fn new(
        kv_inst: EngineRef,
        cluster: ClusterConfig,
        handoff_cfg: HintedOffConfig,
    ) -> Arc<Self> {
        let meta_manager: MetaRef = Arc::new(RemoteMetaManager::new(cluster.clone()));

        let (hh_sender, hh_receiver) = mpsc::channel(1024);
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

        let (coord_sender, coord_receiver) = mpsc::channel(1024);
        let coord = Arc::new(Self {
            meta: meta_manager,
            writer: point_writer,
            handoff: hh_manager,
            coord_sender,
        });
        tokio::spawn(CoordService::coord_service(coord.clone(), coord_receiver));

        coord
    }

    async fn coord_service(coord: Arc<CoordService>, mut requests: Receiver<CoordinatorIntCmd>) {
        while let Some(request) = requests.recv().await {
            match request {
                CoordinatorIntCmd::WritePointsCmd(req) => {
                    tokio::spawn(CoordService::process_write_point_request(
                        coord.clone(),
                        req,
                    ));
                }
            }
        }
    }

    async fn process_write_point_request(coord: Arc<CoordService>, req: WritePointsRequest) {
        let result = coord.writer.write_points(&req, coord.handoff.clone()).await;
        req.sender.send(result).expect("successful");
    }
}

#[async_trait::async_trait]
impl Coordinator for CoordService {
    fn tenant_meta(&self, tenant: &String) -> Option<MetaClientRef> {
        self.meta.tenant_meta(tenant)
    }

    async fn write_points(
        &self,
        tenant: String,
        level: ConsistencyLevel,
        request: WritePointsRpcRequest,
    ) -> CoordinatorResult<()> {
        let (sender, receiver) = oneshot::channel();
        let req = WritePointsRequest {
            tenant,
            level,
            request,
            sender,
        };

        self.coord_sender
            .send(CoordinatorIntCmd::WritePointsCmd(req))
            .await?;
        let result = receiver.await?;

        result
    }
}
