use std::fmt::Debug;
use std::sync::Arc;

use config::{ClusterConfig, HintedOffConfig};
use models::consistency_level::ConsistencyLevel;
use models::meta_data::{BucketInfo, DatabaseInfo, ExpiredBucketInfo};
use models::predicate::domain::{ColumnDomains, PredicateRef};
use models::schema::{DatabaseSchema, TableSchema, TskvTableSchema};
use models::*;

use protos::kv_service::WritePointsRpcRequest;
use snafu::ResultExt;
use tokio::net::TcpStream;
use tokio::sync::mpsc::{self, Receiver, Sender};
use tokio::sync::oneshot;

use trace::info;
use tskv::engine::{EngineRef, MockEngine};
use tskv::TimeRange;

use meta::meta_client::{MetaClientRef, MetaRef, RemoteMetaManager};
use meta::meta_client_mock::{MockMetaClient, MockMetaManager};

use datafusion::arrow::datatypes::SchemaRef;
use tskv::iterator::QueryOption;

use crate::command::*;
use crate::errors::*;
use crate::hh_queue::HintedOffManager;
use crate::reader::{QueryExecutor, ReaderIterator};
use crate::writer::{PointWriter, VnodeMapping};

pub type CoordinatorRef = Arc<dyn Coordinator>;

#[async_trait::async_trait]
pub trait Coordinator: Send + Sync + Debug {
    fn meta_manager(&self) -> MetaRef;
    fn store_engine(&self) -> EngineRef;
    fn tenant_meta(&self, tenant: &str) -> Option<MetaClientRef>;
    async fn write_points(
        &self,
        tenant: String,
        level: ConsistencyLevel,
        request: WritePointsRpcRequest,
    ) -> CoordinatorResult<()>;

    async fn exec_admin_stat_on_all_node(
        &self,
        req: AdminStatementRequest,
    ) -> CoordinatorResult<()>;

    async fn read_record(&self, option: QueryOption) -> CoordinatorResult<ReaderIterator>;
}

#[derive(Debug, Default)]
pub struct MockCoordinator {}

#[async_trait::async_trait]
impl Coordinator for MockCoordinator {
    fn meta_manager(&self) -> MetaRef {
        Arc::new(MockMetaManager::default())
    }

    fn store_engine(&self) -> EngineRef {
        Arc::new(MockEngine::default())
    }

    fn tenant_meta(&self, tenant: &str) -> Option<MetaClientRef> {
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

    async fn exec_admin_stat_on_all_node(
        &self,
        req: AdminStatementRequest,
    ) -> CoordinatorResult<()> {
        Ok(())
    }

    async fn read_record(&self, option: QueryOption) -> CoordinatorResult<ReaderIterator> {
        let (it, _) = ReaderIterator::new();
        Ok(it)
    }
}

#[derive(Debug)]
pub struct CoordService {
    meta: MetaRef,
    kv_inst: EngineRef,
    writer: Arc<PointWriter>,
    handoff: Arc<HintedOffManager>,
    coord_sender: Sender<CoordinatorIntCmd>,
}

impl CoordService {
    pub async fn new(
        kv_inst: EngineRef,
        cluster: ClusterConfig,
        handoff_cfg: HintedOffConfig,
    ) -> Arc<Self> {
        let meta_manager: MetaRef = Arc::new(RemoteMetaManager::new(cluster.clone()));

        let (hh_sender, hh_receiver) = mpsc::channel(1024);
        let point_writer = Arc::new(PointWriter::new(
            cluster.node_id,
            kv_inst.clone(),
            meta_manager.clone(),
            hh_sender,
        ));

        let hh_manager = Arc::new(HintedOffManager::new(handoff_cfg, point_writer.clone()).await);
        tokio::spawn(HintedOffManager::write_handoff_job(
            hh_manager.clone(),
            hh_receiver,
        ));

        let (coord_sender, coord_receiver) = mpsc::channel(1024);
        let coord = Arc::new(Self {
            kv_inst,
            coord_sender,
            meta: meta_manager,
            writer: point_writer,
            handoff: hh_manager,
        });

        tokio::spawn(CoordService::db_ttl_service(coord.clone()));
        tokio::spawn(CoordService::coord_service(coord.clone(), coord_receiver));

        coord
    }

    async fn coord_service(coord: Arc<CoordService>, mut requests: Receiver<CoordinatorIntCmd>) {
        while let Some(request) = requests.recv().await {
            match request {
                CoordinatorIntCmd::WritePointsCmd(req) => {
                    tokio::spawn(CoordService::write_point_request(coord.clone(), req));
                }

                CoordinatorIntCmd::SelectStatementCmd(req) => {
                    tokio::spawn(CoordService::select_statement_request(coord.clone(), req));
                }

                CoordinatorIntCmd::AdminStatementCmd(req, sender) => {
                    tokio::spawn(CoordService::admin_statement_request(
                        coord.clone(),
                        req,
                        sender,
                    ));
                }
            }
        }
    }

    async fn db_ttl_service(coord: Arc<CoordService>) {
        loop {
            let dur = tokio::time::Duration::from_secs(5);
            tokio::time::sleep(dur).await;

            let expired = coord.meta.expired_bucket();
            for info in expired.iter() {
                let result = coord.delete_expired_bucket(info).await;

                info!("delete expired bucket :{:?}, {:?}", info, result);
            }
        }
    }

    async fn delete_expired_bucket(&self, info: &ExpiredBucketInfo) -> CoordinatorResult<()> {
        for repl_set in info.bucket.shard_group.iter() {
            for vnode in repl_set.vnodes.iter() {
                let req = AdminStatementRequest {
                    tenant: info.tenant.clone(),
                    stmt: AdminStatementType::DeleteVnode(info.database.clone(), vnode.id),
                };

                let cmd = CoordinatorTcpCmd::AdminStatementCmd(req);
                self.exec_on_node(vnode.node_id, cmd).await?;
            }
        }

        let meta = self
            .tenant_meta(&info.tenant)
            .ok_or(CoordinatorError::TenantNotFound {
                name: info.tenant.clone(),
            })?;

        meta.delete_bucket(&info.database, info.bucket.id)?;

        Ok(())
    }

    async fn write_point_request(coord: Arc<CoordService>, req: WritePointsRequest) {
        let result = coord.writer.write_points(&req).await;
        req.sender.send(result).expect("successful");
    }

    async fn admin_statement_request(
        coord: Arc<CoordService>,
        req: AdminStatementRequest,
        sender: oneshot::Sender<CoordinatorResult<()>>,
    ) {
        let meta = coord.meta.admin_meta();
        let nodes = meta.data_nodes();

        let mut requests = vec![];
        for node in nodes.iter() {
            let cmd = CoordinatorTcpCmd::AdminStatementCmd(req.clone());
            let request = coord.exec_on_node(node.id, cmd.clone());
            requests.push(request);
        }

        match futures::future::try_join_all(requests).await {
            Ok(_) => sender.send(Ok(())).expect("success"),
            Err(err) => sender.send(Err(err)).expect("success"),
        };
    }

    async fn select_statement_request(coord: Arc<CoordService>, req: SelectStatementRequest) {
        let executor = QueryExecutor::new(
            req.option,
            coord.kv_inst.clone(),
            coord.meta.clone(),
            req.sender.clone(),
        );

        if let Err(err) = executor.execute().await {
            info!("select statement execute failed: {}", err.to_string());
            let _ = req.sender.send(Err(err)).await;
        } else {
            info!("select statement execute success");
        }
    }

    async fn exec_on_node(&self, node_id: u64, cmd: CoordinatorTcpCmd) -> CoordinatorResult<()> {
        let mut conn = self.meta.admin_meta().get_node_conn(node_id).await?;

        send_command(&mut conn, &cmd).await?;
        let rsp_cmd = recv_command(&mut conn).await?;
        if let CoordinatorTcpCmd::StatusResponseCmd(msg) = rsp_cmd {
            self.meta.admin_meta().put_node_conn(node_id, conn);
            if msg.code == crate::command::SUCCESS_RESPONSE_CODE {
                Ok(())
            } else {
                Err(CoordinatorError::WriteVnode {
                    msg: format!("code: {}, msg: {}", msg.code, msg.data),
                })
            }
        } else {
            Err(CoordinatorError::UnExpectResponse)
        }
    }
}

#[async_trait::async_trait]
impl Coordinator for CoordService {
    fn meta_manager(&self) -> MetaRef {
        self.meta.clone()
    }

    fn store_engine(&self) -> EngineRef {
        self.kv_inst.clone()
    }

    fn tenant_meta(&self, tenant: &str) -> Option<MetaClientRef> {
        self.meta.tenant_manager().tenant_meta(tenant)
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

        receiver.await?
    }

    async fn exec_admin_stat_on_all_node(
        &self,
        req: AdminStatementRequest,
    ) -> CoordinatorResult<()> {
        let (sender, receiver) = oneshot::channel();

        self.coord_sender
            .send(CoordinatorIntCmd::AdminStatementCmd(req, sender))
            .await?;

        receiver.await?
    }

    async fn read_record(&self, option: QueryOption) -> CoordinatorResult<ReaderIterator> {
        let (iterator, sender) = ReaderIterator::new();

        let req = SelectStatementRequest { option, sender };
        self.coord_sender
            .send(CoordinatorIntCmd::SelectStatementCmd(req))
            .await?;

        Ok(iterator)
    }
}
