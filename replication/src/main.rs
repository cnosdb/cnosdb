#![allow(dead_code)]
#![allow(unused)]
#![feature(trait_upcasting)]

use std::any::Any;
use std::collections::{BTreeMap, BTreeSet};
use std::convert::Infallible as StdInfallible;
use std::net::SocketAddr;
use std::sync::Arc;

use actix_web::web;
use clap::Parser;
use futures::future::TryFutureExt;
use openraft::error::Infallible as OpenRaftInfallible;
use openraft::raft::{AppendEntriesRequest, InstallSnapshotRequest, VoteRequest};
use openraft::SnapshotPolicy;
use protos::raft_service::raft_service_server::RaftServiceServer;
use replication::apply_store::{ApplyStorage, ApplyStorageRef, HeedApplyStorage};
use replication::entry_store::{EntryStorageRef, HeedEntryStorage};
use replication::errors::{ReplicationError, ReplicationResult};
use replication::multi_raft::MultiRaft;
use replication::network_grpc::RaftCBServer;
use replication::network_http::{EitherBody, RaftHttpAdmin, SyncSendError};
use replication::node_store::NodeStorage;
use replication::raft_node::RaftNode;
use replication::state_store::StateStorage;
use replication::{RaftNodeId, RaftNodeInfo, Request, TypeConfig};
use tokio::sync::RwLock;
use tower::Service;
use trace::info;
use warp::{hyper, Filter};

#[derive(clap::Parser, Clone, Debug)]
#[clap(author, version, about, long_about = None)]
pub struct Opt {
    #[clap(long)]
    pub id: u64,

    #[clap(long)]
    pub http_addr: String,
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let options = Opt::parse();

    let subscriber = tracing_subscriber::FmtSubscriber::new();
    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");

    info!("service start option: {:?}", options);

    start_raft_node(options.id, options.http_addr)
        .await
        .unwrap();

    Ok(())
}

async fn start_raft_node(id: RaftNodeId, http_addr: String) -> ReplicationResult<()> {
    let path = format!("/tmp/cnosdb/{}", id);

    let state = StateStorage::open(format!("{}-state", path))?;
    let entry = HeedEntryStorage::open(format!("{}-entry", path))?;
    let engine = HeedApplyStorage::open(format!("{}-engine", path))?;

    let state = Arc::new(state);
    let entry: EntryStorageRef = Arc::new(entry);
    let engine: ApplyStorageRef = Arc::new(engine);

    let info = RaftNodeInfo {
        group_id: 2222,
        address: http_addr.clone(),
    };

    let storage = NodeStorage::open(id, info.clone(), state, engine.clone(), entry)?;
    let storage = Arc::new(storage);

    let hb: u64 = 1000;
    let config = openraft::Config {
        enable_tick: true,
        enable_elect: true,
        enable_heartbeat: true,
        heartbeat_interval: hb,
        election_timeout_min: 3 * hb,
        election_timeout_max: 5 * hb,
        install_snapshot_timeout: 300 * 1000,
        replication_lag_threshold: 300000,
        snapshot_policy: SnapshotPolicy::LogsSinceLast(3),
        max_in_snapshot_log_to_keep: 3,
        cluster_name: "raft_test".to_string(),
        ..Default::default()
    };
    let config = config.validate().unwrap();

    let node = RaftNode::new(id, info, config, storage, engine)
        .await
        .unwrap();

    start_warp_grpc_server(http_addr, node).await?;
    //start_actix_web_server(http_addr, node).await?;

    Ok(())
}

// **************************** http and grpc server ************************************** //
async fn start_warp_grpc_server(addr: String, node: RaftNode) -> ReplicationResult<()> {
    let node = Arc::new(node);
    let raft_admin = RaftHttpAdmin::new(node.clone());
    let http_server = HttpServer {
        node: node.clone(),
        raft_admin: Arc::new(raft_admin),
    };

    let mut multi_raft = MultiRaft::new();
    multi_raft.add_node(node);
    let nodes = Arc::new(RwLock::new(multi_raft));

    let addr = addr.parse().unwrap();
    hyper::Server::bind(&addr)
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
        .map_err(|err| ReplicationError::IOErrors {
            msg: err.to_string(),
        })?;

    Ok(())
}

struct HttpServer {
    node: Arc<RaftNode>,
    raft_admin: Arc<RaftHttpAdmin>,
}

//  let res: Result<String, warp::Rejection> = Ok(data);
//  warp::reply::Response::new(hyper::Body::from(data))
impl HttpServer {
    fn routes(
        &self,
    ) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
        self.raft_admin
            .routes()
            .or(self.read())
            .or(self.write())
            .or(self.trigger_snapshot())
            .or(self.trigger_purge_logs())
    }

    async fn start(&self, addr: String) {
        info!("http server start addr: {}", addr);

        let addr: SocketAddr = addr.parse().unwrap();
        warp::serve(self.routes()).run(addr).await;
    }

    fn with_raft_node(
        &self,
    ) -> impl Filter<Extract = (Arc<RaftNode>,), Error = StdInfallible> + Clone {
        let node = self.node.clone();

        warp::any().map(move || node.clone())
    }

    fn handle_rejection(err: warp::Rejection) -> Result<impl warp::Reply, StdInfallible> {
        let reason = format!("{:?}", err);

        Ok(warp::Reply::into_response(reason))
    }

    fn read(&self) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
        warp::path!("read")
            .and(warp::body::bytes())
            .and(self.with_raft_node())
            .and_then(|req: hyper::body::Bytes, node: Arc<RaftNode>| async move {
                let req: String = serde_json::from_slice(&req)
                    .map_err(ReplicationError::from)
                    .map_err(warp::reject::custom)?;

                let engine = node.apply_store();
                let engine = Arc::downcast::<HeedApplyStorage>(engine).unwrap();
                let rsp = engine
                    .get(&req)
                    .map_or_else(|err| Some(err.to_string()), |v| v)
                    .unwrap_or("not found value by key".to_string());

                let res: Result<String, warp::Rejection> = Ok(rsp);

                res
            })
    }

    fn write(&self) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
        warp::path!("write")
            .and(warp::body::bytes())
            .and(self.with_raft_node())
            .and_then(|req: hyper::body::Bytes, node: Arc<RaftNode>| async move {
                let rsp = node.raw_raft().client_write(req.to_vec()).await;
                let data = serde_json::to_string(&rsp)
                    .map_err(ReplicationError::from)
                    .map_err(warp::reject::custom)?;
                let res: Result<String, warp::Rejection> = Ok(data);

                res
            })
    }

    fn trigger_purge_logs(
        &self,
    ) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
        warp::path!("trigger_purge_logs")
            .and(warp::body::bytes())
            .and(self.with_raft_node())
            .and_then(|req: hyper::body::Bytes, node: Arc<RaftNode>| async move {
                let idx_id: u64 = serde_json::from_slice(&req)
                    .map_err(ReplicationError::from)
                    .map_err(warp::reject::custom)?;

                let rsp = node
                    .raw_raft()
                    .trigger()
                    .purge_log(idx_id)
                    .await
                    .map_or_else(|err| err.to_string(), |_| "Success".to_string());

                info!("------ trigger_purge_logs: {} - {}", idx_id, rsp);
                let res: Result<String, warp::Rejection> = Ok(rsp);
                res
            })
    }

    fn trigger_snapshot(
        &self,
    ) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
        warp::path!("trigger_snapshot")
            .and(warp::body::bytes())
            .and(self.with_raft_node())
            .and_then(|_req: hyper::body::Bytes, node: Arc<RaftNode>| async move {
                let rsp = node
                    .raw_raft()
                    .trigger()
                    .snapshot()
                    .await
                    .map_or_else(|err| err.to_string(), |_| "Success".to_string());

                info!("------ trigger_snapshot: {}", rsp);
                let res: Result<String, warp::Rejection> = Ok(rsp);
                res
            })
    }
}
