use std::convert::Infallible as StdInfallible;
use std::sync::Arc;
use std::time::Duration;

use config::meta::HeartBeatConfig;
use futures::TryFutureExt;
use metrics::metric_register::MetricsRegister;
use models::meta_data::NodeMetrics;
use models::node_info::NodeStatus;
use models::schema::database_schema::{DatabaseConfig, DatabaseOptions};
use models::schema::DEFAULT_DATABASE;
use openraft::SnapshotPolicy;
use protos::raft_service::raft_service_server::RaftServiceServer;
use replication::entry_store::HeedEntryStorage;
use replication::metrics::ReplicationMetrics;
use replication::multi_raft::MultiRaft;
use replication::network_grpc::RaftCBServer;
use replication::network_http::{EitherBody, RaftHttpAdmin, SyncSendError};
use replication::node_store::NodeStorage;
use replication::raft_node::RaftNode;
use replication::state_store::StateStorage;
use replication::{RaftNodeInfo, ReplicationConfig};
use tokio::sync::RwLock;
use tower::Service;
use tracing::{info, warn};
use warp::hyper;

use super::init::MetaInit;
use crate::error::{MetaError, MetaResult};
use crate::store::command::*;
use crate::store::key_path::KeyPath;
use crate::store::storage::StateMachine;

pub async fn start_raft_node(opt: config::meta::Opt) -> MetaResult<()> {
    info!("CnosDB meta config: {:?}", opt);
    let id = opt.global.node_id;
    let path = std::path::Path::new(&opt.global.data_path);
    let http_addr =
        models::utils::build_address(&opt.global.raft_node_host, opt.global.listen_port);

    let max_size = opt.cluster.lmdb_max_map_size;
    let state = StateStorage::open(path.join(format!("{}_state", id)), max_size)?;
    let entry = HeedEntryStorage::open(path.join(format!("{}_entry", id)), max_size)?;
    let engine = StateMachine::open(path.join(format!("{}_data", id)), max_size)?;

    let state = Arc::new(state);
    let engine = Arc::new(RwLock::new(engine));
    let entry = Arc::new(RwLock::new(entry));

    let info = RaftNodeInfo {
        group_id: 2222,
        address: http_addr.clone(),
    };

    let storage = NodeStorage::open(id, info.clone(), state, engine.clone(), entry).await?;
    let config = ReplicationConfig {
        cluster_name: opt.global.cluster_name.clone(),
        lmdb_max_map_size: opt.cluster.lmdb_max_map_size,
        grpc_enable_gzip: opt.global.grpc_enable_gzip,
        heartbeat_interval: opt.cluster.heartbeat_interval,
        raft_logs_to_keep: opt.cluster.raft_logs_to_keep,
        send_append_entries_timeout: opt.cluster.send_append_entries_timeout,
        install_snapshot_timeout: opt.cluster.install_snapshot_timeout,
        snapshot_policy: SnapshotPolicy::LogsSinceLast(opt.cluster.raft_logs_to_keep),
    };

    let mut db_opt = DatabaseOptions::default();
    db_opt.set_replica(opt.sys_config.system_database_replica);

    let mut usage_schema_config = DatabaseConfig::default();
    usage_schema_config.set_max_memcache_size(opt.sys_config.usage_schema_cache_size);

    let mut cluster_schema_config = DatabaseConfig::default();
    cluster_schema_config.set_max_memcache_size(opt.sys_config.cluster_schema_cache_size);

    let default_database = vec![
        (
            String::from(DEFAULT_DATABASE),
            DatabaseConfig::default(),
            db_opt.clone(),
        ),
        (
            String::from(models::schema::USAGE_SCHEMA),
            usage_schema_config,
            db_opt.clone(),
        ),
        (
            String::from(models::schema::CLUSTER_SCHEMA),
            cluster_schema_config,
            db_opt,
        ),
    ];
    let meta_init = MetaInit::new(
        opt.global.cluster_name.clone(),
        models::auth::user::ROOT.to_string(),
        models::auth::user::ROOT_PWD.to_string(),
        models::schema::DEFAULT_CATALOG.to_string(),
        default_database,
    );
    let node = RaftNode::new(id, info, Arc::new(storage), config)
        .await
        .unwrap();
    {
        let mut engine_w = engine.write().await;
        meta_init.init_meta(&mut engine_w).await;
    }

    let cluster_name = opt.global.cluster_name.clone();
    tokio::spawn(detect_node_heartbeat(
        node.clone(),
        engine.clone(),
        cluster_name,
        opt.heartbeat.clone(),
    ));

    let bind_addr = models::utils::build_address("0.0.0.0", opt.global.listen_port);
    tokio::spawn(start_warp_grpc_server(bind_addr, node, engine));

    Ok(())
}

async fn detect_node_heartbeat(
    node: RaftNode,
    storage: Arc<RwLock<StateMachine>>,
    cluster_name: String,
    heartbeat_config: HeartBeatConfig,
) {
    let mut interval = tokio::time::interval(Duration::from_secs(
        heartbeat_config.heartbeat_recheck_interval,
    ));

    let metrics_path = KeyPath::data_nodes_metrics(&cluster_name);
    loop {
        interval.tick().await;

        if let Ok(_leader) = node.raw_raft().ensure_linearizable().await {
            let opt_list = storage
                .read()
                .await
                .children_data::<NodeMetrics>(&metrics_path);

            if let Ok(list) = opt_list {
                let node_metrics_list: Vec<NodeMetrics> = list.into_values().collect();

                let time = models::utils::now_timestamp_secs();
                for node_metrics in node_metrics_list.iter() {
                    if time - heartbeat_config.heartbeat_expired_interval as i64 > node_metrics.time
                    {
                        let mut now_node_metrics = node_metrics.clone();
                        now_node_metrics.status = NodeStatus::Unreachable;
                        warn!(
                            "Data node '{}' report heartbeat late, maybe unreachable.",
                            node_metrics.id
                        );
                        let req =
                            WriteCommand::ReportNodeMetrics(cluster_name.clone(), now_node_metrics);

                        if let Ok(data) = serde_json::to_vec(&req) {
                            if node.raw_raft().client_write(data).await.is_err() {
                                tracing::error!("failed to change node status to unreachable");
                            }
                        }
                    }
                }
            }
        }
    }
}

// **************************** http and grpc server ************************************** //
async fn start_warp_grpc_server(
    addr: String,
    node: RaftNode,
    storage: Arc<RwLock<StateMachine>>,
) -> MetaResult<()> {
    let node = Arc::new(node);
    let raft_admin = RaftHttpAdmin::new(node.clone());
    let http_server = super::http::HttpServer {
        node: node.clone(),
        storage: storage.clone(),
        raft_admin: Arc::new(raft_admin),
    };

    let mut multi_raft = MultiRaft::new();

    let register = Arc::new(MetricsRegister::new([("address", addr.clone())]));
    let metrics = ReplicationMetrics::new(
        register,
        "cnosdb_meta",
        "cnosdb_meta",
        node.group_id(),
        node.raft_id(),
    );
    multi_raft.add_node(node, metrics);
    let nodes = Arc::new(RwLock::new(multi_raft));

    let addr = addr.parse().unwrap();
    hyper::Server::bind(&addr)
        .http1_max_buf_size(100 * 1024 * 1024)
        .serve(hyper::service::make_service_fn(move |_| {
            let mut http_service = warp::service(http_server.routes());
            let raft_service = RaftServiceServer::new(RaftCBServer::new(nodes.clone()));

            let mut grpc_service = tonic::transport::Server::builder()
                .add_service(raft_service)
                .into_service();

            futures::future::ok::<_, StdInfallible>(tower::service_fn(
                move |req: hyper::Request<hyper::Body>| {
                    if req.uri().path().starts_with("/raft_service.RaftService/") {
                        futures::future::Either::Right(
                            grpc_service
                                .call(req)
                                .map_ok(|res| res.map(EitherBody::Right))
                                .map_err(SyncSendError::from),
                        )
                    } else {
                        futures::future::Either::Left(
                            http_service
                                .call(req)
                                .map_ok(|res| res.map(EitherBody::Left))
                                .map_err(SyncSendError::from),
                        )
                    }
                },
            ))
        }))
        .await
        .map_err(|err| MetaError::CommonError {
            msg: err.to_string(),
        })?;

    Ok(())
}
