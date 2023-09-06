#![allow(dead_code)]
#![allow(unused)]
#![allow(clippy::type_complexity)]

use std::collections::HashMap;
use std::fmt::Debug;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use std::vec;

use config::Config;
use datafusion::arrow::array::{
    Array, ArrayRef, Int64Array, StringArray, TimestampMicrosecondArray, TimestampMillisecondArray,
    TimestampNanosecondArray, UInt32Array,
};
use datafusion::arrow::compute::take;
use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion::arrow::record_batch::RecordBatch;
use meta::error::MetaError;
use meta::model::{MetaClientRef, MetaRef};
use metrics::count::U64Counter;
use metrics::label::Labels;
use metrics::metric::Metric;
use metrics::metric_register::MetricsRegister;
use models::meta_data::{ExpiredBucketInfo, ReplicationSet, ReplicationSetId, VnodeStatus};
use models::object_reference::ResolvedTable;
use models::predicate::domain::{ResolvedPredicateRef, TimeRanges};
use models::record_batch_decode;
use models::schema::{
    timestamp_convert, ColumnType, Precision, TskvTableSchemaRef, DEFAULT_CATALOG, TIME_FIELD,
};
use protocol_parser::lines_convert::{
    arrow_array_to_points, line_to_batches, mutable_batches_to_point,
};
use protocol_parser::Line;
use protos::kv_service::admin_command_request::Command::*;
use protos::kv_service::tskv_service_client::TskvServiceClient;
use protos::kv_service::*;
use tokio::runtime::Runtime;
use tokio::sync::mpsc;
use tonic::transport::Channel;
use tower::timeout::Timeout;
use trace::{debug, error, info, SpanContext, SpanExt, SpanRecorder};
use tskv::EngineRef;
use utils::BkdrHasher;

use crate::errors::*;
use crate::hh_queue::HintedOffManager;
use crate::metrics::LPReporter;
use crate::raft::manager::RaftNodesManager;
use crate::raft::writer::RaftWriter;
use crate::reader::replica_selection::{DynamicReplicaSelectioner, DynamicReplicaSelectionerRef};
use crate::reader::table_scan::opener::TemporaryTableScanOpener;
use crate::reader::tag_scan::opener::TemporaryTagScanOpener;
use crate::reader::{CheckFuture, CheckedCoordinatorRecordBatchStream};
use crate::writer::PointWriter;
use crate::{
    get_replica_all_info, get_vnode_all_info, status_response_to_result, Coordinator, QueryOption,
    SendableCoordinatorRecordBatchStream, VnodeManagerCmdType, VnodeSummarizerCmdType,
};

pub type CoordinatorRef = Arc<dyn Coordinator>;

const USAGE_SCHEMA: &str = "usage_schema";

#[derive(Clone)]
pub struct CoordService {
    node_id: u64,
    meta: MetaRef,
    config: Config,
    runtime: Arc<Runtime>,
    kv_inst: Option<EngineRef>,
    raft_writer: Arc<RaftWriter>,
    point_writer: Arc<PointWriter>,
    metrics: Arc<CoordServiceMetrics>,
    raft_manager: Arc<RaftNodesManager>,

    replica_selectioner: DynamicReplicaSelectionerRef,
}

#[derive(Debug)]
pub struct CoordServiceMetrics {
    coord_data_in: Metric<U64Counter>,
    coord_data_out: Metric<U64Counter>,
    coord_queries: Metric<U64Counter>,
    coord_writes: Metric<U64Counter>,

    sql_data_in: Metric<U64Counter>,
    sql_write_row: Metric<U64Counter>,
    sql_points_data_in: Metric<U64Counter>,
}

macro_rules! generate_coord_metrics_gets {
    ($IDENT: ident) => {
        impl CoordServiceMetrics {
            pub fn $IDENT(&self, tenant: &str, db: &str) -> U64Counter {
                self.$IDENT.recorder(Self::tenant_db_labels(tenant, db))
            }
        }
    };
}
generate_coord_metrics_gets!(coord_data_in);
generate_coord_metrics_gets!(coord_data_out);
generate_coord_metrics_gets!(coord_queries);
generate_coord_metrics_gets!(coord_writes);
generate_coord_metrics_gets!(sql_data_in);
generate_coord_metrics_gets!(sql_write_row);
generate_coord_metrics_gets!(sql_points_data_in);

impl CoordServiceMetrics {
    pub fn new(register: &MetricsRegister) -> Self {
        let coord_data_in = register.metric("coord_data_in", "tenant data in");
        let coord_data_out = register.metric("coord_data_out", "tenant data out");
        let coord_writes = register.metric("coord_writes", "");
        let coord_queries = register.metric("coord_queries", "");

        let sql_data_in = register.metric("sql_data_in", "Traffic written through sql");
        let sql_write_row = register.metric("sql_write_row", "sql write row");
        let sql_points_data_in = register.metric("sql_points_data_in", "sql points data in");

        Self {
            coord_data_in,
            coord_data_out,
            coord_writes,
            coord_queries,

            sql_data_in,
            sql_write_row,
            sql_points_data_in,
        }
    }

    pub fn tenant_db_labels<'a>(tenant: &'a str, db: &'a str) -> impl Into<Labels> + 'a {
        [("tenant", tenant), ("database", db)]
    }
}

impl CoordService {
    pub async fn new(
        runtime: Arc<Runtime>,
        kv_inst: Option<EngineRef>,
        meta: MetaRef,
        config: Config,
        metrics_register: Arc<MetricsRegister>,
    ) -> Arc<Self> {
        let node_id = config.node_basic.node_id;

        let (hh_sender, hh_receiver) = mpsc::channel(1024);
        let point_writer = Arc::new(PointWriter::new(
            node_id,
            config.query.write_timeout_ms,
            kv_inst.clone(),
            meta.clone(),
            hh_sender,
        ));

        let hh_manager = Arc::new(
            HintedOffManager::new(
                config.hinted_off.clone(),
                meta.clone(),
                point_writer.clone(),
            )
            .await,
        );
        tokio::spawn(HintedOffManager::write_handoff_job(hh_manager, hh_receiver));

        let raft_manager = Arc::new(RaftNodesManager::new(
            config.clone(),
            meta.clone(),
            kv_inst.clone(),
        ));
        raft_manager.start_all_raft_node().await.unwrap();

        let raft_writer = Arc::new(RaftWriter::new(
            meta.clone(),
            config.clone(),
            kv_inst.clone(),
            raft_manager.clone(),
        ));

        let coord = Arc::new(Self {
            runtime,
            kv_inst,
            node_id,
            raft_writer,
            point_writer,
            raft_manager,
            meta: meta.clone(),
            config: config.clone(),

            metrics: Arc::new(CoordServiceMetrics::new(metrics_register.as_ref())),
            replica_selectioner: Arc::new(DynamicReplicaSelectioner::new(meta)),
        });

        tokio::spawn(CoordService::db_ttl_service(coord.clone()));

        if config.node_basic.store_metrics {
            tokio::spawn(CoordService::metrics_service(
                coord.clone(),
                metrics_register,
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
        let start = tokio::time::Instant::now() + Duration::from_secs(10);
        let interval = Duration::from_secs(10);
        let mut intv = tokio::time::interval_at(start, interval);
        loop {
            intv.tick().await;
            let mut points_buffer = Vec::new();
            let mut reporter = LPReporter::new(&mut points_buffer);
            root_metrics_register.report(&mut reporter);

            for lines in points_buffer {
                if let Err(e) = coord
                    .write_lines(
                        DEFAULT_CATALOG,
                        USAGE_SCHEMA,
                        Precision::NS,
                        lines.iter().map(|l| l.to_line()).collect::<Vec<_>>(),
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
            if self.using_raft_replication() {
                self.raft_manager()
                    .destory_replica_group(&info.tenant, &info.database, repl_set.id)
                    .await?;
            } else {
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
                .await?
                .check_coord_queries()
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

    #[allow(clippy::type_complexity)]
    fn multi_write_vnodes<'a>(
        &'a self,
        tenant: &'a str,
        precision: Precision,
        info: ReplicationSet,
        points: Arc<Vec<u8>>,
        span_ctx: Option<&'a SpanContext>,
    ) -> CoordinatorResult<Vec<Pin<Box<dyn Future<Output = CoordinatorResult<()>> + Send + 'a>>>>
    {
        let mut requests: Vec<Pin<Box<dyn Future<Output = Result<(), CoordinatorError>> + Send>>> =
            Vec::new();
        for vnode in info.vnodes.iter() {
            let now = tokio::time::Instant::now();
            debug!(
                "Preparing write points on vnode {:?}, start at {:?}",
                vnode, now
            );
            if vnode.status == VnodeStatus::Copying {
                return Err(CoordinatorError::CommonError {
                    msg: "vnode is moving write forbidden ".to_string(),
                });
            }

            let request = self.point_writer.write_to_node(
                vnode.id,
                tenant,
                vnode.node_id,
                precision,
                points.clone(),
                SpanRecorder::new(span_ctx.child_span(format!(
                    "write to vnode {} on node {}",
                    vnode.id, vnode.node_id
                ))),
            );

            let request = Box::pin(request);
            requests.push(request);
        }

        Ok(requests)
    }

    async fn write_replica_by_raft(
        &self,
        tenant: &str,
        db_name: &str,
        data: Arc<Vec<u8>>,
        precision: Precision,
        replica: ReplicationSet,
        span_ctx: Option<&SpanContext>,
    ) -> CoordinatorResult<()> {
        self.raft_writer
            .write_to_replica(
                tenant,
                db_name,
                data,
                precision,
                &replica,
                SpanRecorder::new(span_ctx.child_span(format!(
                    "write to replica {} on node {}",
                    replica.id, self.node_id
                ))),
            )
            .await
    }

    async fn push_points_to_requests<'a>(
        &'a self,
        tenant: &'a str,
        db: &'a str,
        precision: Precision,
        info: ReplicationSet,
        points: Arc<Vec<u8>>,
        span_ctx: Option<&'a SpanContext>,
    ) -> CoordinatorResult<Vec<impl Future<Output = CoordinatorResult<()>> + Sized + 'a>> {
        {
            let _span_recorder = SpanRecorder::new(span_ctx.child_span("limit check"));

            let limiter = self.meta.limiter(tenant).await?;
            let write_size = points.len();

            limiter.check_coord_writes().await?;
            limiter.check_coord_data_in(write_size).await?;

            self.metrics.coord_writes(tenant, db).inc_one();
            self.metrics
                .coord_data_in(tenant, db)
                .inc(write_size as u64);
        }
        if info.vnodes.is_empty() {
            return Err(CoordinatorError::CommonError {
                msg: "no available vnode in replication set".to_string(),
            });
        }

        let mut requests: Vec<Pin<Box<dyn Future<Output = Result<(), CoordinatorError>> + Send>>> =
            Vec::new();
        if self.using_raft_replication() {
            let request = self.write_replica_by_raft(
                tenant,
                db,
                points.clone(),
                precision,
                info.clone(),
                span_ctx,
            );
            requests.push(Box::pin(request));
        } else {
            let mut tasks = self.multi_write_vnodes(tenant, precision, info, points, span_ctx)?;
            requests.append(&mut tasks);
        }

        Ok(requests)
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

    fn raft_manager(&self) -> Arc<RaftNodesManager> {
        self.raft_manager.clone()
    }

    fn using_raft_replication(&self) -> bool {
        self.config.using_raft_replication
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

    async fn exec_write_replica_points(
        &self,
        tenant: &str,
        db_name: &str,
        data: Arc<Vec<u8>>,
        precision: Precision,
        replica: ReplicationSet,
        span_ctx: Option<&SpanContext>,
    ) -> CoordinatorResult<()> {
        self.raft_writer
            .write_to_local_or_forward(data, tenant, db_name, precision, &replica, span_ctx)
            .await
    }

    async fn write_lines<'a>(
        &self,
        tenant: &str,
        db: &str,
        precision: Precision,
        lines: Vec<Line<'a>>,
        span_ctx: Option<&SpanContext>,
    ) -> CoordinatorResult<usize> {
        let mut write_bytes: usize = 0;
        let meta_client =
            self.meta
                .tenant_meta(tenant)
                .await
                .ok_or(CoordinatorError::TenantNotFound {
                    name: tenant.to_string(),
                })?;
        let mut map_lines: HashMap<ReplicationSetId, VnodeLines> = HashMap::new();
        let db_schema =
            meta_client
                .get_db_schema(db)?
                .ok_or_else(|| MetaError::DatabaseNotFound {
                    database: db.to_string(),
                })?;
        let db_precision = db_schema.config.precision_or_default();

        for line in lines {
            let ts = timestamp_convert(precision, *db_precision, line.timestamp).ok_or(
                CoordinatorError::CommonError {
                    msg: "timestamp overflow".to_string(),
                },
            )?;
            let info = meta_client
                .locate_replication_set_for_write(db, line.hash_id, ts)
                .await?;
            let lines_entry = map_lines.entry(info.id).or_insert(VnodeLines::new(info));
            lines_entry.add_line(line)
        }

        let mut requests = Vec::new();
        for lines in map_lines.into_values() {
            let batches =
                line_to_batches(&lines.lines).map_err(|e| CoordinatorError::CommonError {
                    msg: format!("line to batch error: {}", e),
                })?;
            let points = Arc::new(mutable_batches_to_point(db, batches));
            write_bytes += points.len();
            requests.extend(
                self.push_points_to_requests(tenant, db, precision, lines.info, points, span_ctx)
                    .await?,
            );
        }
        let now = tokio::time::Instant::now();
        for res in futures::future::join_all(requests).await {
            debug!(
                "Parallel write points on vnode over, start at: {:?}, elapsed: {} millis, result: {:?}",
                now,
                now.elapsed().as_millis(),
                res
            );
            res?
        }
        Ok(write_bytes)
    }

    async fn write_record_batch<'a>(
        &self,
        table_schema: TskvTableSchemaRef,
        record_batch: RecordBatch,
        span_ctx: Option<&SpanContext>,
    ) -> CoordinatorResult<usize> {
        let mut write_bytes: usize = 0;
        let mut precision = Precision::NS;
        let tenant = table_schema.tenant.as_str();
        let db = table_schema.db.as_str();
        let meta_client =
            self.meta
                .tenant_meta(tenant)
                .await
                .ok_or(CoordinatorError::TenantNotFound {
                    name: tenant.to_string(),
                })?;
        let db_schema =
            meta_client
                .get_db_schema(db)?
                .ok_or_else(|| MetaError::DatabaseNotFound {
                    database: db.to_string(),
                })?;
        let db_precision = db_schema.config.precision_or_default();

        let mut repl_idx: HashMap<ReplicationSet, Vec<u32>> = HashMap::new();
        let schema = record_batch.schema().fields.clone();
        let table_name = table_schema.name.as_str();
        let columns = record_batch.columns();
        for idx in 0..record_batch.num_rows() {
            let mut hasher = BkdrHasher::new();
            hasher.hash_with(table_name.as_bytes());
            let mut ts = i64::MAX;
            let mut has_ts = false;
            for (column, schema) in columns.iter().zip(schema.iter()) {
                let name = schema.name().as_str();
                let tskv_schema_column =
                    table_schema
                        .column(name)
                        .ok_or(CoordinatorError::CommonError {
                            msg: format!("column {} not found in table {}", name, table_name),
                        })?;
                if name == TIME_FIELD {
                    let precsion_and_value =
                        get_precision_and_value_from_arrow_column(column, idx)?;
                    precision = precsion_and_value.0;
                    ts = timestamp_convert(precision, *db_precision, precsion_and_value.1).ok_or(
                        CoordinatorError::CommonError {
                            msg: "timestamp overflow".to_string(),
                        },
                    )?;
                    has_ts = true;
                }
                if matches!(tskv_schema_column.column_type, ColumnType::Tag) {
                    let value = column
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .ok_or(CoordinatorError::CommonError {
                            msg: format!("column {} is not string", name),
                        })?
                        .value(idx);
                    hasher.hash_with(name.as_bytes());
                    hasher.hash_with(value.as_bytes());
                }
            }
            if !has_ts {
                return Err(CoordinatorError::CommonError {
                    msg: format!("column {} not found in table {}", TIME_FIELD, table_name),
                });
            }
            let hash = hasher.number();
            let info = meta_client
                .locate_replication_set_for_write(db, hash, ts)
                .await?;
            repl_idx.entry(info).or_default().push(idx as u32);
        }

        let mut requests = Vec::new();
        for (repl, idxs) in repl_idx {
            let indices = UInt32Array::from(idxs);
            let columns = record_batch
                .columns()
                .iter()
                .map(|column| {
                    take(column, &indices, None).map_err(|e| CoordinatorError::CommonError {
                        msg: format!("take column error: {}", e),
                    })
                })
                .collect::<Result<Vec<_>, _>>()?;
            let schema = record_batch.schema();
            let points = Arc::new(
                arrow_array_to_points(columns, schema, table_schema.clone(), indices.len())
                    .map_err(|e| CoordinatorError::CommonError {
                        msg: format!("arrow array to points error: {}", e),
                    })?,
            );
            write_bytes += points.len();
            requests.extend(
                self.push_points_to_requests(tenant, db, precision, repl, points, span_ctx)
                    .await?,
            );
        }
        let now = tokio::time::Instant::now();
        for res in futures::future::join_all(requests).await {
            debug!(
                "Parallel write points on vnode over, start at: {:?}, elapsed: {} millis, result: {:?}",
                now,
                now.elapsed().as_millis(),
                res
            );
            res?
        }
        Ok(write_bytes)
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
            VnodeManagerCmdType::AddRaftFollower(replica_id, node_id) => {
                let all_info = get_replica_all_info(self.meta.clone(), tenant, replica_id).await?;
                if all_info.replica_set.by_node_id(node_id).is_some() {
                    return Err(CoordinatorError::CommonError {
                        msg: format!("A Replication Already in {}", node_id),
                    });
                }

                (
                    AdminCommandRequest {
                        tenant: tenant.to_string(),
                        command: Some(AddRaftFollower(AddRaftFollowerRequest {
                            db_name: all_info.db_name,
                            replica_id: all_info.replica_set.id,
                            follower_nid: node_id,
                        })),
                    },
                    all_info.replica_set.leader_node_id,
                )
            }

            VnodeManagerCmdType::RemoveRaftNode(vnode_id) => {
                let all_info = get_vnode_all_info(self.meta.clone(), tenant, vnode_id).await?;
                let replica_id = all_info.repl_set_id;
                let replica = get_replica_all_info(self.meta.clone(), tenant, replica_id)
                    .await?
                    .replica_set;

                (
                    AdminCommandRequest {
                        tenant: tenant.to_string(),
                        command: Some(RemoveRaftNode(RemoveRaftNodeRequest {
                            vnode_id,
                            replica_id,
                            db_name: all_info.db_name,
                        })),
                    },
                    replica.leader_node_id,
                )
            }

            VnodeManagerCmdType::DestoryRaftGroup(replica_id) => {
                let all_info = get_replica_all_info(self.meta.clone(), tenant, replica_id).await?;

                (
                    AdminCommandRequest {
                        tenant: tenant.to_string(),
                        command: Some(DestoryRaftGroup(DestoryRaftGroupRequest {
                            replica_id,
                            db_name: all_info.db_name,
                        })),
                    },
                    all_info.replica_set.leader_node_id,
                )
            }

            VnodeManagerCmdType::Copy(vnode_id, node_id) => {
                let all_info = get_vnode_all_info(self.meta.clone(), tenant, vnode_id).await?;
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
                let all_info = get_vnode_all_info(self.meta.clone(), tenant, vnode_id).await?;
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
                let all_info = get_vnode_all_info(self.meta.clone(), tenant, vnode_id).await?;
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
                    let vnode = get_vnode_all_info(self.meta.clone(), tenant, *vnode_id).await?;
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

        self.exec_admin_command_on_node(req_node_id, grpc_req).await
    }

    async fn vnode_summarizer(
        &self,
        tenant: &str,
        cmd_type: VnodeSummarizerCmdType,
    ) -> CoordinatorResult<Vec<RecordBatch>> {
        match cmd_type {
            VnodeSummarizerCmdType::Checksum(replica_id) => {
                let replica = get_replica_all_info(self.meta.clone(), tenant, replica_id)
                    .await?
                    .replica_set;

                // Group vnode ids by node id.
                let mut node_vnode_ids_map: HashMap<u64, Vec<u32>> = HashMap::new();
                for vnode in replica.vnodes {
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

struct VnodeLines<'a> {
    pub lines: Vec<Line<'a>>,
    pub info: ReplicationSet,
}

impl<'a> VnodeLines<'a> {
    pub fn new(info: ReplicationSet) -> Self {
        Self {
            lines: vec![],
            info,
        }
    }

    pub fn add_line(&mut self, line: Line<'a>) {
        self.lines.push(line);
    }
}

fn get_precision_and_value_from_arrow_column(
    column: &ArrayRef,
    idx: usize,
) -> CoordinatorResult<(Precision, i64)> {
    match column.data_type() {
        DataType::Timestamp(unit, _) => match unit {
            TimeUnit::Second => Err(CoordinatorError::CommonError {
                msg: "time field not support second".to_string(),
            }),
            TimeUnit::Millisecond => {
                let value = column
                    .as_any()
                    .downcast_ref::<TimestampMillisecondArray>()
                    .ok_or(CoordinatorError::CommonError {
                        msg: "time field data type miss match: millisecond".to_string(),
                    })?
                    .value(idx);
                Ok((Precision::MS, value))
            }
            TimeUnit::Microsecond => {
                let value = column
                    .as_any()
                    .downcast_ref::<TimestampMicrosecondArray>()
                    .ok_or(CoordinatorError::CommonError {
                        msg: "time field data type miss match: microsecond".to_string(),
                    })?
                    .value(idx);
                Ok((Precision::US, value))
            }
            TimeUnit::Nanosecond => {
                let value = column
                    .as_any()
                    .downcast_ref::<TimestampNanosecondArray>()
                    .ok_or(CoordinatorError::CommonError {
                        msg: "time field data type miss match: nanosecond".to_string(),
                    })?
                    .value(idx);
                Ok((Precision::NS, value))
            }
        },
        DataType::Int64 => {
            let value = column
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or(CoordinatorError::CommonError {
                    msg: "time field data type miss match: int64".to_string(),
                })?
                .value(idx);
            Ok((Precision::NS, value))
        }
        _ => Err(CoordinatorError::CommonError {
            msg: "time field data type miss match".to_string(),
        }),
    }
}
