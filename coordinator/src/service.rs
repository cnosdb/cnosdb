use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;
use std::time::Duration;
use std::vec;

use config::{Config, HintedOffConfig};
use datafusion::arrow::record_batch::RecordBatch;
use meta::model::{MetaClientRef, MetaRef};
use metrics::count::U64Counter;
use metrics::label::Labels;
use metrics::metric::Metric;
use metrics::metric_register::MetricsRegister;
use models::consistency_level::ConsistencyLevel;
use models::meta_data::{ExpiredBucketInfo, ReplicationSet};
use models::object_reference::ResolvedTable;
use models::predicate::domain::{ResolvedPredicateRef, TimeRanges};
use models::record_batch_decode;
use models::schema::{Precision, DEFAULT_CATALOG};
use protos::kv_service::admin_command_request::Command::*;
use protos::kv_service::tskv_service_client::TskvServiceClient;
use protos::kv_service::{WritePointsRequest, *};
use protos::models::Points;
use tokio::runtime::Runtime;
use tokio::sync::mpsc;
use tonic::transport::Channel;
use tower::timeout::Timeout;
use trace::{debug, error, info, SpanContext, SpanExt, SpanRecorder};
use tskv::EngineRef;

use crate::errors::*;
use crate::hh_queue::HintedOffManager;
use crate::metrics::LPReporter;
use crate::reader::replica_selection::{DynamicReplicaSelectioner, DynamicReplicaSelectionerRef};
use crate::reader::table_scan::opener::TemporaryTableScanOpener;
use crate::reader::tag_scan::opener::TemporaryTagScanOpener;
use crate::reader::{CheckFuture, CheckedCoordinatorRecordBatchStream};
use crate::writer::PointWriter;
use crate::{
    status_response_to_result, Coordinator, QueryOption, SendableCoordinatorRecordBatchStream,
    VnodeManagerCmdType, VnodeSummarizerCmdType, WriteRequest,
};

pub type CoordinatorRef = Arc<dyn Coordinator>;

#[derive(Clone)]
pub struct CoordService {
    node_id: u64,
    meta: MetaRef,
    config: Config,
    runtime: Arc<Runtime>,
    kv_inst: Option<EngineRef>,
    writer: Arc<PointWriter>,
    metrics: Arc<CoordServiceMetrics>,

    replica_selectioner: DynamicReplicaSelectionerRef,
}

#[derive(Debug)]
pub struct CoordServiceMetrics {
    data_in: Metric<U64Counter>,
    sql_data_in: Metric<U64Counter>,
    data_out: Metric<U64Counter>,
    sql_write_row: Metric<U64Counter>,
    sql_points_data_in: Metric<U64Counter>,
}

impl CoordServiceMetrics {
    pub fn new(register: &MetricsRegister) -> Self {
        let data_in = register.metric("coord_data_in", "tenant data in");
        let data_out = register.metric("coord_data_out", "tenant data out");
        let sql_data_in = register.metric("sql_data_in", "Traffic written through sql");
        let sql_write_row = register.metric("sql_write_row", "sql write row");
        let sql_points_data_in = register.metric("sql_points_data_in", "sql points data in");
        Self {
            data_in,
            data_out,
            sql_data_in,
            sql_write_row,
            sql_points_data_in,
        }
    }

    pub fn tenant_db_labels<'a>(tenant: &'a str, db: &'a str) -> impl Into<Labels> + 'a {
        [("tenant", tenant), ("database", db)]
    }

    pub fn data_in(&self, tenant: &str, db: &str) -> U64Counter {
        self.data_in.recorder(Self::tenant_db_labels(tenant, db))
    }

    pub fn data_out(&self, tenant: &str, db: &str) -> U64Counter {
        self.data_out.recorder(Self::tenant_db_labels(tenant, db))
    }
    pub fn sql_data_in(&self, tenant: &str, db: &str) -> U64Counter {
        self.sql_data_in
            .recorder(Self::tenant_db_labels(tenant, db))
    }
    pub fn sql_write_row(&self, tenant: &str, db: &str) -> U64Counter {
        self.sql_write_row
            .recorder(Self::tenant_db_labels(tenant, db))
    }
    pub fn sql_points_data_in(&self, tenant: &str, db: &str) -> U64Counter {
        self.sql_points_data_in
            .recorder(Self::tenant_db_labels(tenant, db))
    }
}

impl CoordService {
    pub async fn new(
        runtime: Arc<Runtime>,
        kv_inst: Option<EngineRef>,
        meta_manager: MetaRef,
        config: Config,
        handoff_cfg: HintedOffConfig,
        metrics_register: Arc<MetricsRegister>,
    ) -> Arc<Self> {
        let (hh_sender, hh_receiver) = mpsc::channel(1024);
        let point_writer = Arc::new(PointWriter::new(
            config.node_basic.node_id,
            config.query.write_timeout_ms,
            kv_inst.clone(),
            meta_manager.clone(),
            hh_sender,
        ));

        let hh_manager = Arc::new(
            HintedOffManager::new(handoff_cfg, meta_manager.clone(), point_writer.clone()).await,
        );
        tokio::spawn(HintedOffManager::write_handoff_job(hh_manager, hh_receiver));

        let replica_selectioner = Arc::new(DynamicReplicaSelectioner::new(meta_manager.clone()));
        let coord = Arc::new(Self {
            runtime,
            kv_inst,
            config: config.clone(),
            node_id: config.node_basic.node_id,
            meta: meta_manager,
            writer: point_writer,
            metrics: Arc::new(CoordServiceMetrics::new(metrics_register.as_ref())),
            replica_selectioner,
        });

        tokio::spawn(CoordService::db_ttl_service(coord.clone()));

        if config.node_basic.store_metrics {
            tokio::spawn(CoordService::metrics_service(
                coord.clone(),
                metrics_register.clone(),
            ));
        }

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

    async fn metrics_service(
        coord: Arc<CoordService>,
        root_metrics_register: Arc<MetricsRegister>,
    ) {
        let start = tokio::time::Instant::now() + tokio::time::Duration::from_secs(10);
        let interval = tokio::time::Duration::from_secs(10);
        let mut intv = tokio::time::interval_at(start, interval);
        loop {
            intv.tick().await;
            let mut points_buffer = Vec::new();
            let mut reporter = LPReporter::new("usage_schema", &mut points_buffer);
            root_metrics_register.report(&mut reporter);

            for points in points_buffer {
                let req = WritePointsRequest {
                    version: 0,
                    meta: None,
                    points,
                };

                if let Err(e) = coord
                    .write_points(
                        DEFAULT_CATALOG.to_string(),
                        ConsistencyLevel::Any,
                        Precision::NS,
                        req,
                        // metrics service 不采集trace
                        None,
                    )
                    .await
                {
                    error!("write metrics to {DEFAULT_CATALOG} fail. {e}")
                }
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

    async fn get_replication_set(
        &self,
        tenant: &str,
        replication_set_id: u32,
    ) -> CoordinatorResult<ReplicationSet> {
        match self.tenant_meta(tenant).await {
            Some(meta_client) => match meta_client.get_replication_set(replication_set_id) {
                Some(repl_set) => Ok(repl_set),
                None => Err(CoordinatorError::ReplicationSetNotFound {
                    id: replication_set_id,
                }),
            },

            None => Err(CoordinatorError::TenantNotFound {
                name: tenant.to_string(),
            }),
        }
    }

    async fn exec_admin_command_on_node(
        &self,
        node_id: u64,
        req: AdminCommandRequest,
    ) -> CoordinatorResult<()> {
        let channel = self.meta.get_node_conn(node_id).await?;

        let timeout_channel = Timeout::new(channel, Duration::from_secs(60 * 60));

        let mut client = TskvServiceClient::<Timeout<Channel>>::new(timeout_channel);
        let request = tonic::Request::new(req.clone());

        let response = client
            .exec_admin_command(request)
            .await
            .map_err(tskv::Error::from)?
            .into_inner();
        status_response_to_result(&response)
    }

    async fn prune_shards(
        &self,
        table: &ResolvedTable,
        time_ranges: &TimeRanges,
    ) -> Result<Vec<ReplicationSet>, CoordinatorError> {
        let tenant = table.tenant();
        let database = table.database();
        let meta = self.meta_manager().tenant_meta(tenant).await.ok_or(
            CoordinatorError::TenantNotFound {
                name: tenant.to_string(),
            },
        )?;
        let buckets = meta.mapping_bucket(database, time_ranges.min_ts(), time_ranges.max_ts())?;
        let shards = buckets.into_iter().flat_map(|b| b.shard_group).collect();

        Ok(shards)
    }

    fn build_query_checker(&self, tenant: &str) -> CheckFuture {
        let tenant = tenant.to_string();
        let meta = self.meta.clone();

        let checker = async move {
            meta.limiter(&tenant)
                .await
                .check_query()
                .await
                .map_err(CoordinatorError::from)
        };

        Box::pin(checker)
    }

    async fn exec_admin_fetch_command_on_node(
        &self,
        node_id: u64,
        req: AdminFetchCommandRequest,
    ) -> CoordinatorResult<RecordBatch> {
        let channel = self.meta.get_node_conn(node_id).await?;

        let timeout_channel = Timeout::new(channel, Duration::from_secs(60 * 60));

        let mut client = TskvServiceClient::<Timeout<Channel>>::new(timeout_channel);
        let request = tonic::Request::new(req.clone());

        let response = client
            .exec_admin_fetch_command(request)
            .await
            .map_err(tskv::Error::from)?
            .into_inner();
        match record_batch_decode(&response.data) {
            Ok(r) => Ok(r),
            Err(e) => Err(CoordinatorError::ArrowError { source: e }),
        }
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
        self.meta.tenant_meta(tenant).await
    }

    async fn table_vnodes(
        &self,
        table: &ResolvedTable,
        predicate: ResolvedPredicateRef,
    ) -> CoordinatorResult<Vec<ReplicationSet>> {
        // 1. 根据传入的过滤条件获取表的分片信息（包括副本）
        let shards = self
            .prune_shards(table, predicate.time_ranges().as_ref())
            .await?;
        // 2. 选择最优的副本
        let optimal_shards = self.replica_selectioner.select(shards)?;

        Ok(optimal_shards)
    }

    async fn write_points(
        &self,
        tenant: String,
        level: ConsistencyLevel,
        precision: Precision,
        request: WritePointsRequest,
        span_ctx: Option<&SpanContext>,
    ) -> CoordinatorResult<()> {
        {
            let _span_recorder = SpanRecorder::new(span_ctx.child_span("limit check"));

            let limiter = self.meta.limiter(&tenant).await;
            let points = request.points.as_slice();

            let fb_points = flatbuffers::root::<Points>(points)?;
            let db = fb_points.db_ext()?;

            let write_size = points.len();

            limiter.check_write().await?;
            limiter.check_data_in(write_size).await?;

            self.metrics
                .data_in(tenant.as_str(), db)
                .inc(write_size as u64);
        }

        let req = WriteRequest {
            tenant: tenant.clone(),
            level,
            precision,
            request,
        };

        let now = tokio::time::Instant::now();
        trace::trace!("write points, now: {:?}", now);
        let res = self.writer.write_points(&req, span_ctx).await;
        trace::trace!(
            "write points result: {:?}, start at: {:?} elapsed: {:?}",
            res,
            now,
            now.elapsed()
        );

        res
    }

    fn table_scan(
        &self,
        option: QueryOption,
        span_ctx: Option<&SpanContext>,
    ) -> CoordinatorResult<SendableCoordinatorRecordBatchStream> {
        let checker = self.build_query_checker(&option.table_schema.tenant);

        let opener = TemporaryTableScanOpener::new(
            self.config.query.clone(),
            self.kv_inst.clone(),
            self.runtime.clone(),
            self.meta.clone(),
            span_ctx,
        );

        Ok(Box::pin(CheckedCoordinatorRecordBatchStream::new(
            option,
            opener,
            self.meta.clone(),
            Box::pin(checker),
            &self.metrics,
        )))
    }

    fn tag_scan(
        &self,
        option: QueryOption,
        span_ctx: Option<&SpanContext>,
    ) -> CoordinatorResult<SendableCoordinatorRecordBatchStream> {
        let checker = self.build_query_checker(&option.table_schema.tenant);

        let opener = TemporaryTagScanOpener::new(
            self.config.query.clone(),
            self.kv_inst.clone(),
            self.meta.clone(),
            span_ctx,
        );

        Ok(Box::pin(CheckedCoordinatorRecordBatchStream::new(
            option,
            opener,
            self.meta.clone(),
            Box::pin(checker),
            &self.metrics,
        )))
    }

    async fn broadcast_command(&self, req: AdminCommandRequest) -> CoordinatorResult<()> {
        let nodes = self.meta.data_nodes().await;

        let now = tokio::time::Instant::now();
        let mut requests = vec![];
        for node in nodes.iter() {
            info!("exec command:{:?} on node:{:?}, now:{:?}", req, node, now);

            requests.push(self.exec_admin_command_on_node(node.id, req.clone()));
        }

        for result in futures::future::join_all(requests).await {
            debug!(
                "exec command:{:?} at:{:?}, elapsed:{:?}, result:{:?}",
                req,
                now,
                now.elapsed(),
                result
            );
            result?
        }
        Ok(())
    }

    async fn vnode_manager(
        &self,
        tenant: &str,
        cmd_type: VnodeManagerCmdType,
    ) -> CoordinatorResult<()> {
        let (grpc_req, req_node_id) = match cmd_type {
            VnodeManagerCmdType::Copy(vnode_id, node_id) => {
                let all_info =
                    crate::get_vnode_all_info(self.meta.clone(), tenant, vnode_id).await?;
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
                let all_info =
                    crate::get_vnode_all_info(self.meta.clone(), tenant, vnode_id).await?;
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
                let all_info =
                    crate::get_vnode_all_info(self.meta.clone(), tenant, vnode_id).await?;
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
                // Group vnode ids by node id.
                let mut node_vnode_ids_map: HashMap<u64, Vec<u32>> = HashMap::new();
                for vnode_id in vnode_ids.iter() {
                    let vnode =
                        crate::get_vnode_all_info(self.meta.clone(), tenant, *vnode_id).await?;
                    node_vnode_ids_map
                        .entry(vnode.node_id)
                        .or_default()
                        .push(*vnode_id);
                }
                let nodes = self.meta.data_nodes().await;

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

                for res in futures::future::join_all(req_futures).await {
                    res?
                }

                return Ok(());
            }
        };

        self.exec_admin_command_on_node(req_node_id, grpc_req)
            .await
            .map(|_| ())
    }

    async fn vnode_summarizer(
        &self,
        tenant: &str,
        cmd_type: VnodeSummarizerCmdType,
    ) -> CoordinatorResult<Vec<RecordBatch>> {
        match cmd_type {
            VnodeSummarizerCmdType::Checksum(replication_set_id) => {
                let replication_set = self.get_replication_set(tenant, replication_set_id).await?;

                // Group vnode ids by node id.
                let mut node_vnode_ids_map: HashMap<u64, Vec<u32>> = HashMap::new();
                for vnode in replication_set.vnodes {
                    node_vnode_ids_map
                        .entry(vnode.node_id)
                        .or_default()
                        .push(vnode.id);
                }

                let nodes = self.meta.data_nodes().await;

                // Send grouped vnode ids to nodes.
                let mut req_futures = vec![];
                for node in nodes {
                    if let Some(vnode_ids) = node_vnode_ids_map.remove(&node.id) {
                        for vnode_id in vnode_ids {
                            let cmd = AdminFetchCommandRequest {
                                tenant: tenant.to_string(),
                                command: Some(
                                    admin_fetch_command_request::Command::FetchVnodeChecksum(
                                        FetchVnodeChecksumRequest { vnode_id },
                                    ),
                                ),
                            };
                            req_futures.push(self.exec_admin_fetch_command_on_node(node.id, cmd));
                        }
                    }
                }
                let record_batches = futures::future::try_join_all(req_futures).await?;

                return Ok(record_batches);
            }
        }
    }

    fn metrics(&self) -> &Arc<CoordServiceMetrics> {
        &self.metrics
    }
}
