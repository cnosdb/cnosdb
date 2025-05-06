use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use clap::Parser;
use config::common::LogConfig;
use metrics::metric_register::MetricsRegister;
use openraft::SnapshotPolicy;
use protos::raft_service::raft_service_server::RaftServiceServer;
use replication::apply_store::HeedApplyStorage;
use replication::entry_store::HeedEntryStorage;
use replication::errors::ReplicationResult;
use replication::metrics::ReplicationMetrics;
use replication::multi_raft::MultiRaft;
use replication::network_grpc::RaftCBServer;
use replication::node_store::NodeStorage;
use replication::raft_node::RaftNode;
use replication::state_store::StateStorage;
use replication::{RaftNodeInfo, ReplicationConfig};
use tokio::net::TcpListener;
use tokio::sync::RwLock;
use trace::global_logging::init_global_logging;

const TEST_DIR: &str = "/tmp/test/replication/raft";
const TEST_HOST: &str = "127.0.0.1";
const TEST_CLUSTER_NAME: &str = "raft_test";
const TEST_GROUP_ID: u32 = 2222;
const TEST_MAX_SIZE: u64 = 1024 * 1024 * 1024;
const TEST_TENANT: &str = "test_t";
const TEST_DATABASE: &str = "test_d";

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let cli = Cli::parse();
    init_global_logging(
        &LogConfig {
            level: "debug".to_string(),
            path: cli.dir.join("log").to_string_lossy().to_string(),
            ..Default::default()
        },
        "replication",
    );
    trace::info!("Starting service with option: {:?}", cli);

    let server = cli.build_raft_node_server().await.unwrap();
    server.start_server().await;

    Ok(())
}

#[derive(Debug, Parser)]
#[command(name = "replication", version = version::workspace_version())]
pub struct Cli {
    #[arg(long)]
    pub node_id: u64,
    #[arg(short, long)]
    pub port: u16,
    #[arg(long, default_value = TEST_DIR)]
    pub dir: PathBuf,
}

impl Cli {
    async fn build_raft_node_server(&self) -> ReplicationResult<RaftNodeServer> {
        let data_dir = self.dir.join(self.node_id.to_string());
        let node_http_addr = format!("{TEST_HOST}:{}", self.port);

        let raft_node = RaftNodeInfo {
            group_id: TEST_GROUP_ID,
            address: node_http_addr.clone(),
        };

        let state = StateStorage::open(data_dir.join("state"), TEST_MAX_SIZE as usize)?;
        let state = Arc::new(state);

        let entry = HeedEntryStorage::open(data_dir.join("entry"), TEST_MAX_SIZE as usize)?;
        let entry = Arc::new(RwLock::new(entry));

        let engine = HeedApplyStorage::open(data_dir.join("engine"), TEST_MAX_SIZE as usize)?;
        let engine = Arc::new(RwLock::new(engine));

        let node_store = NodeStorage::open(
            self.node_id,
            raft_node.clone(),
            state,
            engine.clone(),
            entry,
        )
        .await?;
        let node_store = Arc::new(node_store);

        let config = ReplicationConfig {
            cluster_name: TEST_CLUSTER_NAME.to_string(),
            lmdb_max_map_size: TEST_MAX_SIZE,
            grpc_enable_gzip: false,
            heartbeat_interval: 1000,
            raft_logs_to_keep: 200,
            send_append_entries_timeout: 3 * 1000,
            install_snapshot_timeout: 300 * 1000,
            //snapshot_policy: SnapshotPolicy::Never,
            snapshot_policy: SnapshotPolicy::LogsSinceLast(200),
        };
        let raft_node = RaftNode::new(self.node_id, raft_node, node_store, config).await?;
        let raft_node = Arc::new(raft_node);
        let node_server = RaftNodeServer {
            listen_addr: node_http_addr.parse().unwrap(),
            raft_node,
            engine,
        };

        Ok(node_server)
    }
}

#[derive(Clone)]
struct RaftNodeServer {
    listen_addr: SocketAddr,
    raft_node: Arc<RaftNode>,
    engine: Arc<RwLock<HeedApplyStorage>>,
}

impl RaftNodeServer {
    async fn start_server(self) {
        let listener = TcpListener::bind(&self.listen_addr).await.unwrap();

        let metrics_register = Arc::new(MetricsRegister::new([(
            "node_id",
            self.raft_node.raft_id(),
        )]));
        let metrics = ReplicationMetrics::new(
            metrics_register,
            TEST_TENANT,
            TEST_DATABASE,
            self.raft_node.group_id(),
            self.raft_node.raft_id(),
        );

        let mut router = http_api::create_router(self.clone());
        {
            let mut multi_raft = MultiRaft::new();
            multi_raft.add_node(self.raft_node.clone(), metrics);
            let multi_raft = Arc::new(RwLock::new(multi_raft));

            let mut grpc_routes_builder = tonic::service::Routes::builder();
            grpc_routes_builder.add_service(RaftServiceServer::new(RaftCBServer::new(multi_raft)));
            let grpc_router = grpc_routes_builder.routes().into_axum_router();
            router = router.merge(grpc_router);
        };

        axum::serve(listener, router).await.unwrap();
    }
}

mod http_api {
    use axum::body::Bytes;
    use axum::extract::State;
    use axum::http::StatusCode;
    use axum::{routing, Router};
    use replication::errors::MsgInvalidSnafu;

    use super::RaftNodeServer;

    pub fn create_router(server: RaftNodeServer) -> Router {
        Router::new()
            .route("/read", routing::any(read))
            .route("/write", routing::any(write))
            .route("/trigger_purge_logs", routing::any(trigger_purge_logs))
            .route("/trigger_snapshot", routing::any(trigger_snapshot))
            .with_state(server)
    }

    async fn read(
        State(RaftNodeServer { engine, .. }): State<RaftNodeServer>,
        body: String,
    ) -> (StatusCode, String) {
        match engine.read().await.get(&body) {
            Ok(Some(data)) => (StatusCode::OK, data),
            // TODO(zipper): why return status 200 ?
            Ok(None) => (StatusCode::OK, "not found value by key".to_string()),
            Err(e) => (StatusCode::OK, e.to_string()),
        }
    }

    async fn write(
        State(RaftNodeServer { raft_node, .. }): State<RaftNodeServer>,
        body: Bytes,
    ) -> (StatusCode, String) {
        let app_data = body.to_vec();
        let ret = raft_node.raw_raft().client_write(app_data).await;
        match serde_json::to_string(&ret) {
            Ok(data) => (StatusCode::OK, data),
            Err(e) => {
                trace::error!("Failed to serialize response({ret:?}): {e}");
                let err = MsgInvalidSnafu { msg: e.to_string() }.build();
                // TODO(zipper): why return status 200 ?
                (StatusCode::OK, format!("serde error: {err:?}"))
            }
        }
    }

    async fn trigger_purge_logs(
        State(RaftNodeServer { raft_node, .. }): State<RaftNodeServer>,
        body: Bytes,
    ) -> (StatusCode, String) {
        let body_str = match std::str::from_utf8(&body) {
            Ok(s) => s,
            Err(_e) => return (StatusCode::BAD_REQUEST, "Bad UTF-8".to_string()),
        };
        let upto_log_idx = match body_str.parse::<u64>() {
            Ok(idx) => idx,
            Err(_e) => {
                return (StatusCode::BAD_REQUEST, "Invalid number".to_string());
            }
        };

        let ret = raft_node.raw_raft().trigger().purge_log(upto_log_idx).await;
        match ret {
            Ok(_) => {
                trace::info!("------ trigger_purge_logs: {upto_log_idx} - Success");
                (StatusCode::OK, "Success".to_string())
            }
            Err(e) => {
                // TODO(zipper): why return status 200 ?
                trace::info!("------ trigger_purge_logs: {upto_log_idx} - {e}");
                (StatusCode::OK, e.to_string())
            }
        }
    }

    async fn trigger_snapshot(
        State(RaftNodeServer { raft_node, .. }): State<RaftNodeServer>,
    ) -> (StatusCode, String) {
        let ret = raft_node.raw_raft().trigger().snapshot().await;
        match ret {
            Ok(_) => {
                trace::info!("------ trigger_snapshot: Success");
                (StatusCode::OK, "Success".to_string())
            }
            Err(e) => {
                // TODO(zipper): why return status 200 ?
                trace::info!("------ trigger_snapshot: {e}");
                (StatusCode::OK, e.to_string())
            }
        }
    }
}

#[cfg(test)]
#[serial_test::serial]
mod tests {
    use std::path::Path;
    use std::sync::Arc;
    use std::time::Duration;

    use maplit::{btreemap, btreeset};
    use openraft::ServerState;
    use replication::raft_node::RaftNode;
    use replication::{RaftNodeId, RaftNodeInfo};
    use tokio::runtime::Runtime;
    use tokio::task::JoinHandle;

    use crate::{Cli, RaftNodeServer};

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

    fn start_servers<P: AsRef<Path>>(
        rt: Arc<Runtime>,
        dir: P,
        range: std::ops::RangeInclusive<RaftNodeId>,
    ) -> Vec<TestReplicationServer> {
        let mut servers = vec![];
        let dir = dir.as_ref();
        for id in range {
            let cli = Cli {
                node_id: id,
                port: 10100 + id as u16,
                dir: dir.join(id.to_string()),
            };
            let server = rt.block_on(cli.build_raft_node_server()).unwrap();

            let server_clone = server.clone();
            let handler = rt.spawn(async move { server_clone.start_server().await });

            let node = server.raft_node.clone();
            servers.push(TestReplicationServer {
                node,
                server,
                handler,
            })
        }

        servers
    }

    fn raft_node_info(id: RaftNodeId) -> RaftNodeInfo {
        RaftNodeInfo {
            group_id: 2222,
            address: format!("127.0.0.1:{}", 10100 + id),
        }
    }

    #[test]
    fn test_leader_down_and_select_new() {
        println!("----- begin test_leader_down_and_select_new -----");
        let _ = std::fs::remove_dir_all(crate::TEST_DIR);

        let rt = create_runtime();
        let dir = format!("{}/test_leader_down_and_select_new", crate::TEST_DIR);
        let servers = start_servers(rt.clone(), &dir, 0..=2);

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

        let _ = std::fs::remove_dir_all(crate::TEST_DIR);
    }

    #[test]
    fn test_add_follower_node() {
        println!("----- begin test_add_follower_node -----");
        let _ = std::fs::remove_dir_all(crate::TEST_DIR);

        let rt = create_runtime();
        let dir = format!("{}/test_add_follower_node", crate::TEST_DIR);
        let servers = start_servers(rt.clone(), &dir, 0..=2);

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
            servers[0].server.raft_node.raft_id(),
            servers[1].server.raft_node.raft_id()
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

        let _ = std::fs::remove_dir_all(crate::TEST_DIR);
    }

    #[test]
    fn test_remove_follower_node() {
        println!("----- begin test_remove_follower_node -----");
        let _ = std::fs::remove_dir_all(crate::TEST_DIR);

        let rt = create_runtime();
        let dir = format!("{}/test_remove_follower_node", crate::TEST_DIR);
        let servers = start_servers(rt.clone(), &dir, 0..=2);

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
            servers[0].server.raft_node.raft_id(),
            servers[1].server.raft_node.raft_id()
        };
        rt.block_on(servers[0].node.raft_change_membership(members, false))
            .unwrap();

        // assert the cluster status
        let metrics = servers[0].node.raft_metrics();
        assert!(metrics.current_leader.unwrap() == 8000);
        assert_eq!(metrics.state, ServerState::Leader);
        assert_eq!(metrics.replication.unwrap().len(), 2);

        let _ = std::fs::remove_dir_all(crate::TEST_DIR);
    }

    #[test]
    fn test_2node_cluster_remove_follower_node() {
        println!("----- begin test_2node_cluster_remove_follower_node -----");
        let _ = std::fs::remove_dir_all(crate::TEST_DIR);

        let rt = create_runtime();
        let dir = format!("{}/test_2node_cluster_remove", crate::TEST_DIR);
        let servers = start_servers(rt.clone(), &dir, 0..=2);

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
        let members = btreeset! {servers[0].server.raft_node.raft_id()};
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

        let _ = std::fs::remove_dir_all(crate::TEST_DIR);
    }

    #[test]
    fn test_promote_follower_to_leader() {
        println!("----- begin test_promote_follower_to_leader -----");
        let _ = std::fs::remove_dir_all(crate::TEST_DIR);

        let rt = create_runtime();
        let dir = format!("{}/test_promote_follower_to_leader", crate::TEST_DIR);
        let servers = start_servers(rt.clone(), &dir, 0..=2);

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
        let members = btreeset! {servers[2].server.raft_node.raft_id()};
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
            servers[0].server.raft_node.raft_id(),
            servers[1].server.raft_node.raft_id(),
            servers[2].server.raft_node.raft_id()
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

        let _ = std::fs::remove_dir_all(crate::TEST_DIR);
    }

    #[test]
    fn test_write_snapshot_add_node() {
        println!("----- begin test_write_snapshot_add_node -----");
        let _ = std::fs::remove_dir_all(crate::TEST_DIR);

        let rt = create_runtime();
        let dir = format!("{}/test_write_snapshot_add_node", crate::TEST_DIR);
        let servers = start_servers(rt.clone(), &dir, 0..=2);

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
            servers[0].server.raft_node.raft_id(),
            servers[1].server.raft_node.raft_id()
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
            servers[0].server.raft_node.raft_id(),
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
            servers[0].server.raft_node.raft_id(),
            servers[1].server.raft_node.raft_id(),
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

        let _ = std::fs::remove_dir_all(crate::TEST_DIR);
    }

    #[test]
    fn test_write_snapshot_leader_down() {
        println!("----- begin test_write_snapshot -----");
        let _ = std::fs::remove_dir_all(crate::TEST_DIR);

        let rt = create_runtime();
        let dir = format!("{}/test_write_snapshot", crate::TEST_DIR);
        let servers = start_servers(rt.clone(), &dir, 0..=2);

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
        let members = btreeset! {servers[1].server.raft_node.raft_id()};
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
            servers[1].server.raft_node.raft_id(),
            servers[2].server.raft_node.raft_id()
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

        let _ = std::fs::remove_dir_all(crate::TEST_DIR);
    }

    #[test]
    fn test_write_snapshot_restart() {
        println!("----- begin test_write_snapshot_restart -----");
        let _ = std::fs::remove_dir_all(crate::TEST_DIR);

        let rt = create_runtime();
        let dir = format!("{}/test_write_snapshot_restart", crate::TEST_DIR);

        #[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
        struct RequestCommand {
            key: String,
            value: String,
        }
        let mut command = RequestCommand {
            key: "test_key".to_string(),
            value: "test_val".to_string(),
        };
        {
            let servers = start_servers(rt.clone(), &dir, 8000..=8000);

            // start node-0 as cluster
            let members = btreemap! {
                servers[0].node.raft_id()=>raft_node_info(servers[0].node.raft_id()),
            };
            rt.block_on(servers[0].node.raft_init(members)).unwrap();
            std::thread::sleep(Duration::from_secs(1));
            let metrics = servers[0].node.raft_metrics();
            assert_eq!(metrics.state, ServerState::Leader);

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
        }
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
        let _ = std::fs::remove_dir_all(crate::TEST_DIR);

        let rt = create_runtime();
        let dir = format!("{}/continuous_operation_testing", crate::TEST_DIR);
        let servers = start_servers(rt.clone(), &dir, 0..=2);

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
            servers[1].server.raft_node.raft_id(),
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
            servers[0].server.raft_node.raft_id(),
            servers[1].server.raft_node.raft_id(),
        };
        rt.block_on(servers[1].node.raft_change_membership(members, false))
            .unwrap();
        std::thread::sleep(Duration::from_secs(3));

        // remove node-0 from cluster
        let members = btreeset! {
            servers[1].server.raft_node.raft_id(),
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

        let _ = std::fs::remove_dir_all(crate::TEST_DIR);
    }
}
