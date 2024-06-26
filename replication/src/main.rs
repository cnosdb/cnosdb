use std::convert::Infallible as StdInfallible;
use std::net::SocketAddr;
use std::sync::Arc;

use clap::Parser;
use futures::future::TryFutureExt;
use metrics::metric_register::MetricsRegister;
use openraft::SnapshotPolicy;
use protos::raft_service::raft_service_server::RaftServiceServer;
use replication::apply_store::HeedApplyStorage;
use replication::entry_store::HeedEntryStorage;
use replication::errors::{MsgInvalidSnafu, ReplicationResult};
use replication::metrics::ReplicationMetrics;
use replication::multi_raft::MultiRaft;
use replication::network_grpc::RaftCBServer;
use replication::network_http::{EitherBody, RaftHttpAdmin, SyncSendError};
use replication::node_store::NodeStorage;
use replication::raft_node::RaftNode;
use replication::state_store::StateStorage;
use replication::{RaftNodeId, RaftNodeInfo, ReplicationConfig};
use tokio::sync::RwLock;
use tower::Service;
use trace::{debug, info};
use tracing::error;
use warp::{hyper, Filter};

const TEST_DATA_DIR: &str = "/tmp/cnosdb/raft_test";
#[derive(clap::Parser, Clone, Debug)]
#[clap(author, version, about, long_about = None)]
pub struct Opt {
    #[clap(long)]
    pub id_port: u64,
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    tracing_subscriber::fmt()
        .with_file(true)
        .with_line_number(true)
        .with_max_level(tracing::Level::DEBUG)
        .init();

    debug!("this is a debug log");

    let options = Opt::parse();

    info!("service start option: {:?}", options);

    let data_dir = format!("{}/{}", TEST_DATA_DIR, options.id_port);
    let server = create_raft_node(&data_dir, options.id_port).await.unwrap();
    start_server(server).await.unwrap();

    Ok(())
}

fn raft_node_info(id: RaftNodeId) -> RaftNodeInfo {
    let http_addr = format!("127.0.0.1:{}", id);

    RaftNodeInfo {
        group_id: 2222,
        address: http_addr.clone(),
    }
}

async fn create_raft_node(dir: &str, id_port: RaftNodeId) -> ReplicationResult<RaftNodeServer> {
    let info = raft_node_info(id_port);
    let http_addr = info.address.clone();

    let max_size = 1024 * 1024 * 1024;
    let state = StateStorage::open(format!("{}/state", dir), max_size)?;
    let entry = HeedEntryStorage::open(format!("{}/entry", dir), max_size)?;
    let engine = HeedApplyStorage::open(format!("{}/engine", dir), max_size)?;

    let state = Arc::new(state);
    let entry = Arc::new(RwLock::new(entry));
    let engine = Arc::new(RwLock::new(engine));

    let storage = NodeStorage::open(id_port, info.clone(), state, engine.clone(), entry).await?;
    let storage = Arc::new(storage);

    let config = ReplicationConfig {
        cluster_name: "raft_test".to_string(),
        lmdb_max_map_size: 1024 * 1024 * 1024,
        grpc_enable_gzip: false,
        heartbeat_interval: 1000,
        raft_logs_to_keep: 200,
        send_append_entries_timeout: 3 * 1000,
        install_snapshot_timeout: 300 * 1000,
        //snapshot_policy: SnapshotPolicy::Never,
        snapshot_policy: SnapshotPolicy::LogsSinceLast(200),
    };
    let node = RaftNode::new(id_port, info, storage, config).await.unwrap();

    let node = Arc::new(node);
    let raft_admin = RaftHttpAdmin::new(node.clone());
    let node_server = RaftNodeServer {
        node,
        engine,
        http_addr,
        raft_admin: Arc::new(raft_admin),
    };

    Ok(node_server)
}

// **************************** http and grpc server ************************************** //
async fn start_server(node_server: RaftNodeServer) -> ReplicationResult<()> {
    let mut multi_raft = MultiRaft::new();

    let register = Arc::new(MetricsRegister::new([("node_id", "8888")]));
    let metrics = ReplicationMetrics::new(
        register,
        "test_t",
        "test_d",
        node_server.node.group_id(),
        node_server.node.raft_id(),
    );
    multi_raft.add_node(node_server.node.clone(), metrics);
    let nodes = Arc::new(RwLock::new(multi_raft));

    let addr = node_server.http_addr.parse().unwrap();
    hyper::Server::bind(&addr)
        .serve(hyper::service::make_service_fn(move |_| {
            let mut http_service = warp::service(node_server.routes());
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
        .unwrap();

    Ok(())
}

#[derive(Clone)]
struct RaftNodeServer {
    http_addr: String,
    node: Arc<RaftNode>,
    engine: Arc<RwLock<HeedApplyStorage>>,
    raft_admin: Arc<RaftHttpAdmin>,
}

//  let res: Result<String, warp::Rejection> = Ok(data);
//  warp::reply::Response::new(hyper::Body::from(data))
impl RaftNodeServer {
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

    async fn _start(&self, addr: String) {
        info!("http server start addr: {}", addr);

        let addr: SocketAddr = addr.parse().unwrap();
        warp::serve(self.routes()).run(addr).await;
    }

    fn with_engine(
        &self,
    ) -> impl Filter<Extract = (Arc<RwLock<HeedApplyStorage>>,), Error = StdInfallible> + Clone
    {
        let engine = self.engine.clone();

        warp::any().map(move || engine.clone())
    }

    fn with_raft_node(
        &self,
    ) -> impl Filter<Extract = (Arc<RaftNode>,), Error = StdInfallible> + Clone {
        let node = self.node.clone();

        warp::any().map(move || node.clone())
    }

    fn _handle_rejection(err: warp::Rejection) -> Result<impl warp::Reply, StdInfallible> {
        let reason = format!("{:?}", err);

        Ok(warp::Reply::into_response(reason))
    }

    fn read(&self) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
        warp::path!("read")
            .and(warp::body::bytes())
            .and(self.with_engine())
            .and_then(
                |req: hyper::body::Bytes, engine: Arc<RwLock<HeedApplyStorage>>| async move {
                    let req: String = serde_json::from_slice(&req)
                        .map_err(|e| MsgInvalidSnafu { msg: e.to_string() }.build())
                        .map_err(|e| {
                            error!("serde error: {:?}", e);
                            warp::reject::custom(e)
                        })?;

                    let rsp = engine
                        .read()
                        .await
                        .get(&req)
                        .map_or_else(|err| Some(err.to_string()), |v| v)
                        .unwrap_or("not found value by key".to_string());

                    let res: Result<String, warp::Rejection> = Ok(rsp);

                    res
                },
            )
    }

    fn write(&self) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
        warp::path!("write")
            .and(warp::body::bytes())
            .and(self.with_raft_node())
            .and_then(|req: hyper::body::Bytes, node: Arc<RaftNode>| async move {
                let rsp = node.raw_raft().client_write(req.to_vec()).await;
                let data = serde_json::to_string(&rsp)
                    .map_err(|e| MsgInvalidSnafu { msg: e.to_string() }.build())
                    .map_err(|e| {
                        error!("serde error: {:?}", e);
                        warp::reject::custom(e)
                    })?;
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
                    .map_err(|e| MsgInvalidSnafu { msg: e.to_string() }.build())
                    .map_err(|e| {
                        error!("serde error: {:?}", e);
                        warp::reject::custom(e)
                    })?;

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

//*****************************************************************************************//
//********************************** Replication Tests ************************************//
//*****************************************************************************************//
#[cfg(test)]
#[serial_test::serial]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use maplit::{btreemap, btreeset};
    use openraft::ServerState;
    use replication::raft_node::RaftNode;
    use replication::RaftNodeId;
    use tokio::runtime::Runtime;
    use tokio::task::JoinHandle;

    use crate::{create_raft_node, raft_node_info, start_server, RaftNodeServer};

    struct TestReplicationServer {
        pub node: Arc<RaftNode>,
        pub server: RaftNodeServer,
        pub handler: JoinHandle<()>,
    }

    impl Drop for TestReplicationServer {
        fn drop(&mut self) {
            self.handler.abort();
        }
    }

    fn create_runtime() -> Arc<Runtime> {
        let mut builder = tokio::runtime::Builder::new_multi_thread();
        builder.enable_all().max_blocking_threads(6);
        let runtime = builder.build().unwrap();

        Arc::new(runtime)
    }

    fn start_servers(
        rt: Arc<Runtime>,
        dir: &str,
        range: std::ops::RangeInclusive<RaftNodeId>,
    ) -> Vec<TestReplicationServer> {
        let mut servers = vec![];
        for id in range {
            let data_dir = format!("{}/{}", dir, id);
            let server = rt.block_on(create_raft_node(&data_dir, id)).unwrap();

            let server_clone = server.clone();
            let handler = rt.spawn(async move {
                start_server(server_clone).await.unwrap();
            });

            let node = server.node.clone();
            servers.push(TestReplicationServer {
                node,
                server,
                handler,
            })
        }

        servers
    }

    #[test]
    fn test_leader_down_and_select_new() {
        println!("----- begin test_leader_down_and_select_new -----");
        let _ = std::fs::remove_dir_all(crate::TEST_DATA_DIR);

        let rt = create_runtime();
        let dir = format!("{}/test_leader_down_and_select_new", crate::TEST_DATA_DIR);
        let servers = start_servers(rt.clone(), &dir, 8000..=8002);

        // init 3-nodes cluster
        let members = btreemap! {
            servers[0].node.raft_id()=>raft_node_info(servers[0].node.raft_id()),
            servers[1].node.raft_id()=>raft_node_info(servers[1].node.raft_id()),
            servers[2].node.raft_id()=>raft_node_info(servers[2].node.raft_id()),
        };
        rt.block_on(servers[0].node.raft_init(members)).unwrap();

        std::thread::sleep(Duration::from_secs(1));
        let metrics = servers[0].node.raft_metrics();
        assert_eq!(metrics.state, ServerState::Leader);

        // shutdown node-0
        servers[0].handler.abort();
        rt.block_on(servers[0].node.shutdown()).unwrap();
        std::thread::sleep(Duration::from_secs(1));
        let metrics = servers[0].node.raft_metrics();
        assert_eq!(metrics.state, ServerState::Shutdown);

        // check if the new leader has been completed
        std::thread::sleep(Duration::from_secs(10));
        let metrics = servers[1].node.raft_metrics();
        let leader_id = metrics.current_leader.unwrap_or_default();
        assert!(leader_id == 8001 || leader_id == 8002);

        let _ = std::fs::remove_dir_all(crate::TEST_DATA_DIR);
    }

    #[test]
    fn test_add_follower_node() {
        println!("----- begin test_add_follower_node -----");
        let _ = std::fs::remove_dir_all(crate::TEST_DATA_DIR);

        let rt = create_runtime();
        let dir = format!("{}/test_add_follower_node", crate::TEST_DATA_DIR);
        let servers = start_servers(rt.clone(), &dir, 8000..=8002);

        // start node-0 as cluster
        let members =
            btreemap! {servers[0].node.raft_id()=>raft_node_info(servers[0].node.raft_id())};
        rt.block_on(servers[0].node.raft_init(members)).unwrap();
        std::thread::sleep(Duration::from_secs(1));
        let metrics = servers[0].node.raft_metrics();
        assert_eq!(metrics.state, ServerState::Leader);

        // add node-1 to learner
        let id = servers[1].node.raft_id();
        rt.block_on(servers[0].node.raft_add_learner(id, raft_node_info(id)))
            .unwrap();

        // change node-1 as follower
        let members = btreeset! {
            servers[0].server.node.raft_id(),
            servers[1].server.node.raft_id()
        };
        rt.block_on(servers[0].node.raft_change_membership(members, true))
            .unwrap();

        // assert the cluster status
        std::thread::sleep(Duration::from_secs(3));
        let metrics = servers[0].node.raft_metrics();
        assert_eq!(metrics.state, ServerState::Leader);
        assert_eq!(metrics.current_leader.unwrap(), 8000);
        assert_eq!(metrics.replication.unwrap().len(), 2);

        let metrics = servers[1].node.raft_metrics();
        assert_eq!(metrics.state, ServerState::Follower);
        assert_eq!(metrics.current_leader.unwrap(), 8000);

        let _ = std::fs::remove_dir_all(crate::TEST_DATA_DIR);
    }

    #[test]
    fn test_remove_follower_node() {
        println!("----- begin test_remove_follower_node -----");
        let _ = std::fs::remove_dir_all(crate::TEST_DATA_DIR);

        let rt = create_runtime();
        let dir = format!("{}/test_remove_follower_node", crate::TEST_DATA_DIR);
        let servers = start_servers(rt.clone(), &dir, 8000..=8002);

        // start 3-nodes as a cluster
        let members = btreemap! {
            servers[0].node.raft_id()=>raft_node_info(servers[0].node.raft_id()),
            servers[1].node.raft_id()=>raft_node_info(servers[1].node.raft_id()),
            servers[2].node.raft_id()=>raft_node_info(servers[2].node.raft_id()),
        };
        rt.block_on(servers[0].node.raft_init(members)).unwrap();
        std::thread::sleep(Duration::from_secs(1));
        let metrics = servers[0].node.raft_metrics();
        assert_eq!(metrics.state, ServerState::Leader);

        // shutdown node-2
        servers[2].handler.abort();
        rt.block_on(servers[2].node.shutdown()).unwrap();
        std::thread::sleep(Duration::from_secs(1));
        let metrics = servers[2].node.raft_metrics();
        assert_eq!(metrics.state, ServerState::Shutdown);

        // remove node-2 from the cluster
        let members = btreeset! {
            servers[0].server.node.raft_id(),
            servers[1].server.node.raft_id()
        };
        rt.block_on(servers[0].node.raft_change_membership(members, false))
            .unwrap();

        // assert the cluster status
        let metrics = servers[0].node.raft_metrics();
        assert!(metrics.current_leader.unwrap() == 8000);
        assert_eq!(metrics.state, ServerState::Leader);
        assert_eq!(metrics.replication.unwrap().len(), 2);

        let _ = std::fs::remove_dir_all(crate::TEST_DATA_DIR);
    }

    #[test]
    fn test_2node_cluster_remove_follower_node() {
        println!("----- begin test_2node_cluster_remove_follower_node -----");
        let _ = std::fs::remove_dir_all(crate::TEST_DATA_DIR);

        let rt = create_runtime();
        let dir = format!("{}/test_2node_cluster_remove", crate::TEST_DATA_DIR);
        let servers = start_servers(rt.clone(), &dir, 8000..=8002);

        // start 2-nodes as a cluster
        let members = btreemap! {
            servers[0].node.raft_id()=>raft_node_info(servers[0].node.raft_id()),
            servers[1].node.raft_id()=>raft_node_info(servers[1].node.raft_id()),
        };
        rt.block_on(servers[0].node.raft_init(members)).unwrap();
        std::thread::sleep(Duration::from_secs(1));
        let metrics = servers[0].node.raft_metrics();
        assert_eq!(metrics.state, ServerState::Leader);

        // remove node-1 from the cluster
        let members = btreeset! {servers[0].server.node.raft_id()};
        rt.block_on(servers[0].node.raft_change_membership(members, false))
            .unwrap();

        // shutdown node-1
        servers[1].handler.abort();
        rt.block_on(servers[1].node.shutdown()).unwrap();
        std::thread::sleep(Duration::from_secs(1));
        let metrics = servers[1].node.raft_metrics();
        assert_eq!(metrics.state, ServerState::Shutdown);

        // assert the cluster status
        let metrics = servers[0].node.raft_metrics();
        assert!(metrics.current_leader.unwrap() == 8000);
        assert_eq!(metrics.state, ServerState::Leader);
        assert_eq!(metrics.replication.unwrap().len(), 1);

        let _ = std::fs::remove_dir_all(crate::TEST_DATA_DIR);
    }

    #[test]
    fn test_promote_follower_to_leader() {
        println!("----- begin test_promote_follower_to_leader -----");
        let _ = std::fs::remove_dir_all(crate::TEST_DATA_DIR);

        let rt = create_runtime();
        let dir = format!("{}/test_promote_follower_to_leader", crate::TEST_DATA_DIR);
        let servers = start_servers(rt.clone(), &dir, 8000..=8002);

        // start 3-nodes as a cluster
        let members = btreemap! {
            servers[0].node.raft_id()=>raft_node_info(servers[0].node.raft_id()),
            servers[1].node.raft_id()=>raft_node_info(servers[1].node.raft_id()),
            servers[2].node.raft_id()=>raft_node_info(servers[2].node.raft_id()),
        };
        rt.block_on(servers[0].node.raft_init(members)).unwrap();
        std::thread::sleep(Duration::from_secs(1));
        let metrics = servers[0].node.raft_metrics();
        assert_eq!(metrics.state, ServerState::Leader);

        // change node-2 as leader, node-0 && node-1 as learner
        let members = btreeset! {servers[2].server.node.raft_id()};
        rt.block_on(servers[0].node.raft_change_membership(members, true))
            .unwrap();
        std::thread::sleep(Duration::from_secs(1));

        let metrics = servers[0].node.raft_metrics();
        assert_eq!(metrics.state, ServerState::Learner);
        assert_eq!(metrics.current_leader.unwrap(), 8002);
        let metrics = servers[1].node.raft_metrics();
        assert_eq!(metrics.state, ServerState::Learner);
        assert_eq!(metrics.current_leader.unwrap(), 8002);
        let metrics = servers[2].node.raft_metrics();
        assert_eq!(metrics.state, ServerState::Leader);
        assert_eq!(metrics.current_leader.unwrap(), 8002);

        //change node-0 && node-1 as follower
        let members = btreeset! {
            servers[0].server.node.raft_id(),
            servers[1].server.node.raft_id(),
            servers[2].server.node.raft_id()
        };
        rt.block_on(servers[2].node.raft_change_membership(members, true))
            .unwrap();
        std::thread::sleep(Duration::from_secs(1));

        let metrics = servers[0].node.raft_metrics();
        assert_eq!(metrics.state, ServerState::Follower);
        assert_eq!(metrics.current_leader.unwrap(), 8002);
        let metrics = servers[1].node.raft_metrics();
        assert_eq!(metrics.state, ServerState::Follower);
        assert_eq!(metrics.current_leader.unwrap(), 8002);
        let metrics = servers[2].node.raft_metrics();
        assert_eq!(metrics.state, ServerState::Leader);
        assert_eq!(metrics.current_leader.unwrap(), 8002);

        let _ = std::fs::remove_dir_all(crate::TEST_DATA_DIR);
    }

    #[test]
    fn test_write_snapshot_add_node() {
        println!("----- begin test_write_snapshot_add_node -----");
        let _ = std::fs::remove_dir_all(crate::TEST_DATA_DIR);

        let rt = create_runtime();
        let dir = format!("{}/test_write_snapshot_add_node", crate::TEST_DATA_DIR);
        let servers = start_servers(rt.clone(), &dir, 8000..=8002);

        // start node-0 as cluster
        let members = btreemap! {
            servers[0].node.raft_id()=>raft_node_info(servers[0].node.raft_id()),
        };
        rt.block_on(servers[0].node.raft_init(members)).unwrap();
        std::thread::sleep(Duration::from_secs(1));
        let metrics = servers[0].node.raft_metrics();
        assert_eq!(metrics.state, ServerState::Leader);

        #[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
        struct RequestCommand {
            key: String,
            value: String,
        }
        let mut command = RequestCommand {
            key: "test_key".to_string(),
            value: "test_val".to_string(),
        };

        // write data 950 times
        for i in 50..1000 {
            command.value = format!("v_{}", i);
            let data = serde_json::to_string(&command).unwrap();
            rt.block_on(servers[0].node.raw_raft().client_write(data.into()))
                .unwrap();
        }

        // add node-1 as follower
        let id = servers[1].node.raft_id();
        rt.block_on(servers[0].node.raft_add_learner(id, raft_node_info(id)))
            .unwrap();
        println!("---- add learner complete");

        let members = btreeset! {
            servers[0].server.node.raft_id(),
            servers[1].server.node.raft_id()
        };
        rt.block_on(servers[0].node.raft_change_membership(members, true))
            .unwrap();
        println!("---- change to follower complete");

        {
            std::thread::sleep(Duration::from_secs(1));
            let engine = rt.block_on(servers[1].server.engine.read());
            assert_eq!(engine.get(&command.key).unwrap().unwrap(), "v_999");
        }

        // write data 1000 times
        for i in 1000..2000 {
            command.value = format!("v_{}", i);
            let data = serde_json::to_string(&command).unwrap();
            rt.block_on(servers[0].node.raw_raft().client_write(data.into()))
                .unwrap();
        }
        {
            std::thread::sleep(Duration::from_secs(1));
            let engine = rt.block_on(servers[0].server.engine.read());
            assert_eq!(engine.get(&command.key).unwrap().unwrap(), "v_1999");
            let engine = rt.block_on(servers[1].server.engine.read());
            assert_eq!(engine.get(&command.key).unwrap().unwrap(), "v_1999");
        }

        // remove node-1 from cluster
        let members = btreeset! {
            servers[0].server.node.raft_id(),
        };
        rt.block_on(servers[0].node.raft_change_membership(members, false))
            .unwrap();

        // write data 1000 times
        for i in 2000..3000 {
            command.value = format!("v_{}", i);
            let data = serde_json::to_string(&command).unwrap();
            rt.block_on(servers[0].node.raw_raft().client_write(data.into()))
                .unwrap();
        }
        {
            std::thread::sleep(Duration::from_secs(1));
            let engine = rt.block_on(servers[0].server.engine.read());
            assert_eq!(engine.get(&command.key).unwrap().unwrap(), "v_2999");
            let engine = rt.block_on(servers[1].server.engine.read());
            assert_eq!(engine.get(&command.key).unwrap().unwrap(), "v_1999");
        }

        // add node-1 as follower
        let id = servers[1].node.raft_id();
        rt.block_on(servers[0].node.raft_add_learner(id, raft_node_info(id)))
            .unwrap();
        let members = btreeset! {
            servers[0].server.node.raft_id(),
            servers[1].server.node.raft_id(),
        };
        rt.block_on(servers[0].node.raft_change_membership(members, true))
            .unwrap();
        {
            std::thread::sleep(Duration::from_secs(1));
            let engine = rt.block_on(servers[0].server.engine.read());
            assert_eq!(engine.get(&command.key).unwrap().unwrap(), "v_2999");
            let engine = rt.block_on(servers[1].server.engine.read());
            assert_eq!(engine.get(&command.key).unwrap().unwrap(), "v_2999");
        }

        // write data 1000 times
        for i in 3000..4000 {
            command.value = format!("v_{}", i);
            let data = serde_json::to_string(&command).unwrap();
            rt.block_on(servers[0].node.raw_raft().client_write(data.into()))
                .unwrap();
        }
        {
            std::thread::sleep(Duration::from_secs(1));
            let engine = rt.block_on(servers[0].server.engine.read());
            assert_eq!(engine.get(&command.key).unwrap().unwrap(), "v_3999");
            let engine = rt.block_on(servers[1].server.engine.read());
            assert_eq!(engine.get(&command.key).unwrap().unwrap(), "v_3999");
        }

        let _ = std::fs::remove_dir_all(crate::TEST_DATA_DIR);
    }

    #[test]
    fn test_write_snapshot_leader_down() {
        println!("----- begin test_write_snapshot -----");
        let _ = std::fs::remove_dir_all(crate::TEST_DATA_DIR);

        let rt = create_runtime();
        let dir = format!("{}/test_write_snapshot", crate::TEST_DATA_DIR);
        let servers = start_servers(rt.clone(), &dir, 8000..=8002);

        // start node-0 as cluster
        let members = btreemap! {
            servers[0].node.raft_id()=>raft_node_info(servers[0].node.raft_id()),
            servers[1].node.raft_id()=>raft_node_info(servers[1].node.raft_id()),
        };
        rt.block_on(servers[0].node.raft_init(members)).unwrap();
        std::thread::sleep(Duration::from_secs(1));
        let metrics = servers[0].node.raft_metrics();
        assert_eq!(metrics.state, ServerState::Leader);

        #[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
        struct RequestCommand {
            key: String,
            value: String,
        }
        let mut command = RequestCommand {
            key: "test_key".to_string(),
            value: "test_val".to_string(),
        };

        // write data 900 times
        for i in 100..1000 {
            command.value = format!("v_{}", i);
            let data = serde_json::to_string(&command).unwrap();
            rt.block_on(servers[0].node.raw_raft().client_write(data.into()))
                .unwrap();
        }

        // remove node-0 from the cluster
        let members = btreeset! {servers[1].server.node.raft_id()};
        rt.block_on(servers[0].node.raft_change_membership(members, false))
            .unwrap();

        // shutdown node-1
        servers[0].handler.abort();
        rt.block_on(servers[0].node.shutdown()).unwrap();
        std::thread::sleep(Duration::from_secs(1));
        let metrics = servers[0].node.raft_metrics();
        assert_eq!(metrics.state, ServerState::Shutdown);

        std::thread::sleep(Duration::from_secs(1));
        let metrics = servers[1].node.raft_metrics();
        assert_eq!(metrics.state, ServerState::Leader);

        // add node-1 as follower
        let id = servers[2].node.raft_id();
        rt.block_on(servers[1].node.raft_add_learner(id, raft_node_info(id)))
            .unwrap();
        println!("---- add learner complete");

        let members = btreeset! {
            servers[1].server.node.raft_id(),
            servers[2].server.node.raft_id()
        };
        rt.block_on(servers[1].node.raft_change_membership(members, true))
            .unwrap();
        println!("---- change to follower complete");

        // write data 1000 times
        for i in 1000..1300 {
            command.value = format!("v_{}", i);
            let data = serde_json::to_string(&command).unwrap();
            rt.block_on(servers[1].node.raw_raft().client_write(data.into()))
                .unwrap();
        }
        {
            std::thread::sleep(Duration::from_secs(1));
            let engine = rt.block_on(servers[1].server.engine.read());
            assert_eq!(engine.get(&command.key).unwrap().unwrap(), "v_1299");
            let engine = rt.block_on(servers[2].server.engine.read());
            assert_eq!(engine.get(&command.key).unwrap().unwrap(), "v_1299");
        }

        let _ = std::fs::remove_dir_all(crate::TEST_DATA_DIR);
    }

    #[test]
    fn test_write_snapshot_restart() {
        println!("----- begin test_write_snapshot_restart -----");
        let _ = std::fs::remove_dir_all(crate::TEST_DATA_DIR);

        let rt = create_runtime();
        let dir = format!("{}/test_write_snapshot_restart", crate::TEST_DATA_DIR);
        let servers = start_servers(rt.clone(), &dir, 8000..=8000);

        // start node-0 as cluster
        let members = btreemap! {
            servers[0].node.raft_id()=>raft_node_info(servers[0].node.raft_id()),
        };
        rt.block_on(servers[0].node.raft_init(members)).unwrap();
        std::thread::sleep(Duration::from_secs(1));
        let metrics = servers[0].node.raft_metrics();
        assert_eq!(metrics.state, ServerState::Leader);

        #[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
        struct RequestCommand {
            key: String,
            value: String,
        }
        let mut command = RequestCommand {
            key: "test_key".to_string(),
            value: "test_val".to_string(),
        };

        // write data 900 times
        for i in 900..1000 {
            command.value = format!("v_{}", i);
            let data = serde_json::to_string(&command).unwrap();
            rt.block_on(servers[0].node.raw_raft().client_write(data.into()))
                .unwrap();
        }

        {
            let engine = rt.block_on(servers[0].server.engine.read());
            assert_eq!(engine.get(&command.key).unwrap().unwrap(), "v_999");
        }

        servers[0].handler.abort();

        println!("----------------------*********-------------------------");
        println!("----------------------*********-------------------------");
        println!("----------------------*********-------------------------");

        std::thread::sleep(Duration::from_secs(1));
        let servers = start_servers(rt.clone(), &dir, 8000..=8000);
        // write data 900 times
        for i in 1000..1300 {
            command.value = format!("v_{}", i);
            let data = serde_json::to_string(&command).unwrap();
            rt.block_on(servers[0].node.raw_raft().client_write(data.into()))
                .unwrap();
        }

        {
            let engine = rt.block_on(servers[0].server.engine.read());
            assert_eq!(engine.get(&command.key).unwrap().unwrap(), "v_1299");
        }
    }

    #[test]
    fn continuous_operation_testing() {
        println!("----- begin continuous_operation_testing -----");
        let _ = std::fs::remove_dir_all(crate::TEST_DATA_DIR);

        let rt = create_runtime();
        let dir = format!("{}/continuous_operation_testing", crate::TEST_DATA_DIR);
        let servers = start_servers(rt.clone(), &dir, 8000..=8002);

        // start node-0 as a cluster
        let members = btreemap! {
            servers[0].node.raft_id()=>raft_node_info(servers[0].node.raft_id()),
        };
        rt.block_on(servers[0].node.raft_init(members)).unwrap();
        std::thread::sleep(Duration::from_secs(1));
        let metrics = servers[0].node.raft_metrics();
        assert_eq!(metrics.state, ServerState::Leader);

        // add node-1 as a learner
        let id = servers[1].node.raft_id();
        rt.block_on(servers[0].node.raft_add_learner(id, raft_node_info(id)))
            .unwrap();

        // change node-1 as leader, node-0 as learner
        let members = btreeset! {
            servers[1].server.node.raft_id(),
        };
        rt.block_on(servers[0].node.raft_change_membership(members, true))
            .unwrap();

        std::thread::sleep(Duration::from_secs(3));
        let metrics = servers[0].node.raft_metrics();
        assert_eq!(metrics.state, ServerState::Learner);
        assert_eq!(metrics.current_leader.unwrap(), 8001);

        let metrics = servers[1].node.raft_metrics();
        assert_eq!(metrics.state, ServerState::Leader);
        assert_eq!(metrics.current_leader.unwrap(), 8001);
        assert_eq!(metrics.replication.unwrap().len(), 2);

        // change node-0 as folower
        let members = btreeset! {
            servers[0].server.node.raft_id(),
            servers[1].server.node.raft_id(),
        };
        rt.block_on(servers[1].node.raft_change_membership(members, false))
            .unwrap();
        std::thread::sleep(Duration::from_secs(3));

        // remove node-0 from cluster
        let members = btreeset! {
            servers[1].server.node.raft_id(),
        };
        rt.block_on(servers[1].node.raft_change_membership(members, false))
            .unwrap();

        // shutdown node-0
        servers[0].handler.abort();
        rt.block_on(servers[0].node.shutdown()).unwrap();

        // assert the cluster status
        std::thread::sleep(Duration::from_secs(3));
        let metrics = servers[0].node.raft_metrics();
        assert_eq!(metrics.state, ServerState::Shutdown);
        let metrics = servers[1].node.raft_metrics();
        assert_eq!(metrics.state, ServerState::Leader);
        assert_eq!(metrics.current_leader.unwrap(), 8001);
        assert_eq!(metrics.replication.unwrap().len(), 1);

        let _ = std::fs::remove_dir_all(crate::TEST_DATA_DIR);
    }
}
