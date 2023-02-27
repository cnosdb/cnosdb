use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;
use std::time::Duration;

use config::{ClusterConfig, HintedOffConfig};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use futures::TryFutureExt;
use meta::error::MetaError;
use meta::meta_client_mock::{MockMetaClient, MockMetaManager};
use meta::meta_manager::RemoteMetaManager;
use meta::{MetaClientRef, MetaRef};
use models::consistency_level::ConsistencyLevel;
use models::meta_data::{BucketInfo, DatabaseInfo, ExpiredBucketInfo, VnodeAllInfo};
use models::predicate::domain::{ColumnDomains, PredicateRef};
use models::schema::{DatabaseSchema, TableColumn, TableSchema, TskvTableSchema};
use models::*;
use protos::kv_service::admin_command_request::Command::*;
use protos::kv_service::tskv_service_client::TskvServiceClient;
use protos::kv_service::{StatusResponse, *};
use snafu::ResultExt;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::mpsc::{self, Receiver, Sender};
use tokio::sync::oneshot;
use tonic::transport::Channel;
use tower::timeout::Timeout;
use trace::info;
use tskv::engine::{EngineRef, MockEngine};
use tskv::iterator::QueryOption;
use tskv::TimeRange;

use crate::errors::*;
use crate::file_info::*;
use crate::hh_queue::HintedOffManager;
use crate::reader::{QueryExecutor, ReaderIterator};
use crate::service_mock::Coordinator;
use crate::writer::{PointWriter, VnodeMapping};
use crate::{status_response_to_result, VnodeManagerCmdType, WriteRequest};

pub type CoordinatorRef = Arc<dyn Coordinator>;

#[derive(Debug)]
pub struct CoordService {
    node_id: u64,
    meta: MetaRef,
    kv_inst: Option<EngineRef>,
    writer: Arc<PointWriter>,
    handoff: Arc<HintedOffManager>,
}

impl CoordService {
    pub async fn new(
        kv_inst: Option<EngineRef>,
        meta_manager: MetaRef,
        cluster: ClusterConfig,
        handoff_cfg: HintedOffConfig,
    ) -> Arc<Self> {
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

        let coord = Arc::new(Self {
            kv_inst,
            node_id: cluster.node_id,
            meta: meta_manager,
            writer: point_writer,
            handoff: hh_manager,
        });

        tokio::spawn(CoordService::db_ttl_service(coord.clone()));

        coord
    }

    async fn db_ttl_service(coord: Arc<CoordService>) {
        loop {
            let dur = tokio::time::Duration::from_secs(5);
            tokio::time::sleep(dur).await;

            let expired = coord.meta.expired_bucket().await;
            for info in expired.iter() {
                let result = coord.delete_expired_bucket(info).await;

                info!("delete expired bucket :{:?}, {:?}", info, result);
            }
        }
    }

    async fn delete_expired_bucket(&self, info: &ExpiredBucketInfo) -> CoordinatorResult<()> {
        for repl_set in info.bucket.shard_group.iter() {
            for vnode in repl_set.vnodes.iter() {
                let cmd = AdminCommandRequest {
                    tenant: info.tenant.clone(),
                    command: Some(DelVnode(DeleteVnodeRequest {
                        db: info.database.clone(),
                        vnode_id: vnode.id,
                    })),
                };

                self.exec_admin_command_on_node(vnode.node_id, cmd).await?;
            }
        }

        let meta =
            self.tenant_meta(&info.tenant)
                .await
                .ok_or(CoordinatorError::TenantNotFound {
                    name: info.tenant.clone(),
                })?;

        meta.delete_bucket(&info.database, info.bucket.id).await?;

        Ok(())
    }

    async fn get_vnode_all_info(
        &self,
        tenant: &str,
        vnode_id: u32,
    ) -> CoordinatorResult<VnodeAllInfo> {
        match self.tenant_meta(tenant).await {
            Some(meta_client) => match meta_client.get_vnode_all_info(vnode_id) {
                Some(all_info) => Ok(all_info),
                None => Err(CoordinatorError::VnodeNotFound { id: vnode_id }),
            },

            None => Err(CoordinatorError::TenantNotFound {
                name: tenant.to_string(),
            }),
        }
    }

    async fn select_statement_request(
        kv_inst: Option<EngineRef>,
        meta: MetaRef,
        option: QueryOption,
        sender: Sender<CoordinatorResult<RecordBatch>>,
    ) {
        let tenant = option.tenant.as_str();

        if let Err(e) = meta
            .tenant_manager()
            .limiter(tenant)
            .await
            .check_query()
            .await
            .map_err(|e| CoordinatorError::Meta { source: e })
        {
            let _ = sender.send(Err(e)).await;
            return;
        }

        let now = tokio::time::Instant::now();
        info!("select statement execute now: {:?}", now);
        let executor = QueryExecutor::new(option, kv_inst.clone(), meta, sender.clone());
        if let Err(err) = executor.execute().await {
            info!(
                "select statement execute failed: {}, start at: {:?} elapsed: {:?}",
                err,
                now,
                now.elapsed(),
            );
            let _ = sender.send(Err(err)).await;
        } else {
            info!(
                "select statement execute success, start at: {:?} elapsed: {:?}",
                now,
                now.elapsed(),
            );
        }
    }

    async fn exec_admin_command_on_node(
        &self,
        node_id: u64,
        req: AdminCommandRequest,
    ) -> CoordinatorResult<()> {
        let channel = self.meta.admin_meta().get_node_conn(node_id).await?;

        let timeout_channel = Timeout::new(channel, Duration::from_secs(60 * 60));

        let mut client = TskvServiceClient::<Timeout<Channel>>::new(timeout_channel);
        let request = tonic::Request::new(req.clone());

        let response = client.exec_admin_command(request).await?.into_inner();
        status_response_to_result(&response)
    }
}

//***************************** Coordinator Interface ***************************************** */
#[async_trait::async_trait]
impl Coordinator for CoordService {
    fn node_id(&self) -> u64 {
        self.node_id
    }

    fn meta_manager(&self) -> MetaRef {
        self.meta.clone()
    }

    fn store_engine(&self) -> Option<EngineRef> {
        self.kv_inst.clone()
    }

    async fn tenant_meta(&self, tenant: &str) -> Option<MetaClientRef> {
        self.meta.tenant_manager().tenant_meta(tenant).await
    }

    async fn write_points(
        &self,
        tenant: String,
        level: ConsistencyLevel,
        request: WritePointsRequest,
    ) -> CoordinatorResult<()> {
        let limiter = self.meta.tenant_manager().limiter(&tenant).await;
        limiter.check_write().await?;
        let data_len = request.points.len();
        limiter.check_data_in(data_len).await?;

        let req = WriteRequest {
            tenant: tenant.clone(),
            level,
            request,
        };

        let now = tokio::time::Instant::now();
        info!("write points, now: {:?}", now);
        let res = self.writer.write_points(&req).await;
        info!(
            "write points result: {:?}, start at: {:?} elapsed: {:?}",
            res,
            now,
            now.elapsed()
        );

        res
    }

    fn read_record(&self, option: QueryOption) -> CoordinatorResult<ReaderIterator> {
        let (iterator, sender) = ReaderIterator::new();

        tokio::spawn(CoordService::select_statement_request(
            self.kv_inst.clone(),
            self.meta.clone(),
            option,
            sender,
        ));

        Ok(iterator)
    }

    async fn broadcast_command(&self, req: AdminCommandRequest) -> CoordinatorResult<()> {
        let nodes = self.meta.admin_meta().data_nodes().await;

        let now = tokio::time::Instant::now();
        let mut requests = vec![];
        for node in nodes.iter() {
            info!("exec command:{:?} on node:{:?}, now:{:?}", req, node, now);

            requests.push(self.exec_admin_command_on_node(node.id, req.clone()));
        }

        let result = futures::future::try_join_all(requests).await;

        info!(
            "exec command:{:?} at:{:?}, elapsed:{:?}, result:{:?}",
            req,
            now,
            now.elapsed(),
            result
        );

        result?;
        Ok(())
    }

    async fn vnode_manager(
        &self,
        tenant: &str,
        cmd_type: VnodeManagerCmdType,
    ) -> CoordinatorResult<()> {
        let (grpc_req, req_node_id) = match cmd_type {
            VnodeManagerCmdType::Copy(vnode_id, node_id) => {
                let all_info = self.get_vnode_all_info(tenant, vnode_id).await?;
                if all_info.node_id == node_id {
                    return Err(CoordinatorError::CommonError {
                        msg: format!("Vnode: {} Already in {}", all_info.vnode_id, node_id),
                    });
                }

                (
                    AdminCommandRequest {
                        tenant: tenant.to_string(),
                        command: Some(CopyVnode(CopyVnodeRequest { vnode_id })),
                    },
                    node_id,
                )
            }

            VnodeManagerCmdType::Move(vnode_id, node_id) => {
                let all_info = self.get_vnode_all_info(tenant, vnode_id).await?;
                if all_info.node_id == node_id {
                    return Err(CoordinatorError::CommonError {
                        msg: format!("move vnode: {} already in {}", all_info.vnode_id, node_id),
                    });
                }

                (
                    AdminCommandRequest {
                        tenant: tenant.to_string(),
                        command: Some(MoveVnode(MoveVnodeRequest { vnode_id })),
                    },
                    node_id,
                )
            }

            VnodeManagerCmdType::Drop(vnode_id) => {
                let all_info = self.get_vnode_all_info(tenant, vnode_id).await?;
                let db = all_info.db_name;
                (
                    AdminCommandRequest {
                        tenant: tenant.to_string(),
                        command: Some(DelVnode(DeleteVnodeRequest { db, vnode_id })),
                    },
                    all_info.node_id,
                )
            }

            VnodeManagerCmdType::Compact(vnode_ids) => {
                let meta = self.meta.admin_meta();

                // Group vnode ids by node id.
                let mut node_vnode_ids_map: HashMap<u64, Vec<u32>> = HashMap::new();
                for vnode_id in vnode_ids.iter() {
                    let vnode = self.get_vnode_all_info(tenant, *vnode_id).await?;
                    node_vnode_ids_map
                        .entry(vnode.node_id)
                        .or_default()
                        .push(*vnode_id);
                }
                let nodes = meta.data_nodes().await;

                // Send grouped vnode ids to nodes.
                let mut req_futures = vec![];
                for node in nodes {
                    if let Some(vnode_ids) = node_vnode_ids_map.remove(&node.id) {
                        let cmd = AdminCommandRequest {
                            tenant: tenant.to_string(),
                            command: Some(CompactVnode(CompactVnodeRequest { vnode_ids })),
                        };
                        req_futures.push(self.exec_admin_command_on_node(node.id, cmd));
                    }
                }

                let result = futures::future::try_join_all(req_futures).await?;

                return Ok(());
            }
        };

        self.exec_admin_command_on_node(req_node_id, grpc_req).await
    }
}
