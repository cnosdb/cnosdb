use std::sync::Arc;
use std::time::Duration;

use crate::service::api;
use crate::service::connection::Connections;
use crate::service::raft_api;
use crate::store::Restore;
use crate::store::Store;
use actix_web::middleware;
use actix_web::middleware::Logger;
use actix_web::web::Data;
use actix_web::App;
use actix_web::HttpServer;
use openraft::Config;
use openraft::Raft;
use openraft::SnapshotPolicy;
use store::command::*;
use store::state_machine::*;

pub mod client;
pub mod meta_client;
pub mod meta_client_mock;
pub mod service;
pub mod store;
pub mod tenant_manager;
pub mod user_manager;

pub type NodeId = u64;

pub struct MetaApp {
    pub id: NodeId,
    pub addr: String,
    pub raft: ExampleRaft,
    pub store: Arc<Store>,
    pub config: Arc<Config>,
}

openraft::declare_raft_types!(
    pub ExampleTypeConfig: D = WriteCommand, R = CommandResp, NodeId = NodeId
);

pub type ExampleRaft = Raft<ExampleTypeConfig, Connections, Arc<Store>>;

pub async fn start_raft_node(node_id: NodeId, http_addr: String) -> std::io::Result<()> {
    let mut config = Config::default().validate().unwrap();
    config.snapshot_policy = SnapshotPolicy::LogsSinceLast(500);
    config.max_applied_log_to_keep = 20000;
    config.install_snapshot_timeout = 400;

    let config = Arc::new(config);
    let es = Store::open_create(node_id);
    let mut store = Arc::new(es);

    store.restore().await;
    let network = Connections::new();
    let raft = Raft::new(node_id, config.clone(), network, store.clone());
    let app = Data::new(MetaApp {
        id: node_id,
        addr: http_addr.clone(),
        raft,
        store,
        config,
    });

    let server = HttpServer::new(move || {
        App::new()
            .wrap(Logger::default())
            .wrap(Logger::new("%a %{User-Agent}i"))
            .wrap(middleware::Compress::default())
            .app_data(app.clone())
            .service(raft_api::append)
            .service(raft_api::snapshot)
            .service(raft_api::vote)
            .service(raft_api::init)
            .service(raft_api::add_learner)
            .service(raft_api::change_membership)
            .service(raft_api::metrics)
            .service(api::write)
            .service(api::read)
            .service(api::read_all)
            .service(api::consistent_read)
    })
    .keep_alive(Duration::from_secs(5));

    let x = server.bind(http_addr)?;

    x.run().await
}
