use std::fmt::Debug;
use std::sync::Arc;

use config::{ClusterConfig, HintedOffConfig};
use models::consistency_level::ConsistencyLevel;
use models::meta_data::DatabaseInfo;
use models::predicate::domain::{ColumnDomains, PredicateRef};
use models::schema::{DatabaseSchema, TableSchema, TskvTableSchema};
use models::*;

use protos::kv_service::WritePointsRpcRequest;
use snafu::ResultExt;
//use std::sync::mpsc::{self, Receiver, Sender};
use tokio::sync::mpsc::{self, Receiver, Sender};
use tokio::sync::oneshot;
use trace::info;
use tskv::engine::{EngineRef, MockEngine};
use tskv::TimeRange;

use datafusion::arrow::datatypes::SchemaRef;
use tskv::iterator::QueryOption;

use crate::command::{CoordinatorIntCmd, SelectStatementRequest, WritePointsRequest};
use crate::errors::*;
use crate::hh_queue::HintedOffManager;
use crate::meta_client::{MetaClientRef, MetaRef, RemoteMetaManager};
use crate::meta_client_mock::{MockMetaClient, MockMetaManager};
use crate::reader::{QueryExecutor, ReaderIterator};
use crate::writer::{PointWriter, VnodeMapping};

pub type CoordinatorRef = Arc<dyn Coordinator>;

#[async_trait::async_trait]
pub trait Coordinator: Send + Sync + Debug {
    fn meta_manager(&self) -> MetaRef;
    fn store_engine(&self) -> EngineRef;
    fn tenant_meta(&self, tenant: &String) -> Option<MetaClientRef>;
    async fn write_points(
        &self,
        tenant: String,
        level: ConsistencyLevel,
        request: WritePointsRpcRequest,
    ) -> CoordinatorResult<()>;
    fn create_db(&self, tenant: &String, info: DatabaseInfo) -> CoordinatorResult<()>;

    async fn read_record(&self, option: QueryOption) -> CoordinatorResult<ReaderIterator>;

    // fn create_db(&self, tenant: &String, info: &DatabaseSchema) -> CoordinatorResult<()>;
    // fn db_schema(&self, tenant: &String, name: &String) -> Option<DatabaseSchema>;
    // fn list_databases(&self, tenant: &String) -> CoordinatorResult<Vec<String>>;
    // fn drop_db(&self, tenant: &String, name: &String) -> CoordinatorResult<()>;

    // fn create_table(&self, tenant: &String, schema: &TableSchema) -> CoordinatorResult<()>;
    // fn table_schema(
    //     &self,
    //     tenant: &String,
    //     db: &String,
    //     table: &String,
    // ) -> CoordinatorResult<Option<TableSchema>>;
    // fn list_tables(&self, tenant: &String, db: &String) -> CoordinatorResult<Vec<String>>;
    // fn drop_table(&self, tenant: &String, db: &String, table: &String) -> CoordinatorResult<()>;
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

    fn tenant_meta(&self, tenant: &String) -> Option<MetaClientRef> {
        Some(Arc::new(MockMetaClient::default()))
    }

    fn create_db(&self, tenant: &String, info: DatabaseInfo) -> CoordinatorResult<()> {
        Ok(())
    }

    async fn write_points(
        &self,
        tenant: String,
        level: ConsistencyLevel,
        req: WritePointsRpcRequest,
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
    pub fn new(
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
            kv_inst,
            coord_sender,
            meta: meta_manager,
            writer: point_writer,
            handoff: hh_manager,
        });
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
            }
        }
    }

    async fn write_point_request(coord: Arc<CoordService>, req: WritePointsRequest) {
        let result = coord.writer.write_points(&req, coord.handoff.clone()).await;
        req.sender.send(result).expect("successful");
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
}

#[async_trait::async_trait]
impl Coordinator for CoordService {
    fn meta_manager(&self) -> MetaRef {
        self.meta.clone()
    }

    fn store_engine(&self) -> EngineRef {
        self.kv_inst.clone()
    }

    fn tenant_meta(&self, tenant: &String) -> Option<MetaClientRef> {
        self.meta.tenant_meta(tenant)
    }

    fn create_db(&self, tenant: &String, info: DatabaseInfo) -> CoordinatorResult<()> {
        let meta = self
            .tenant_meta(tenant)
            .ok_or(CoordinatorError::TenantNotFound {
                name: tenant.clone(),
            })?;

        meta.create_db(&info.name, &info)?;

        Ok(())
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

    async fn read_record(&self, option: QueryOption) -> CoordinatorResult<ReaderIterator> {
        let (iterator, sender) = ReaderIterator::new();

        let req = SelectStatementRequest { option, sender };
        self.coord_sender
            .send(CoordinatorIntCmd::SelectStatementCmd(req))
            .await?;

        Ok(iterator)
    }
}
