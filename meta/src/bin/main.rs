#![allow(dead_code, unused_imports, unused_variables)]

use actix_web::middleware::Logger;
use actix_web::web::Data;
use actix_web::{middleware, App, HttpServer};
use clap::Parser;
use meta::service::connection::Connections;
use meta::service::{api, raft_api};
use meta::store::Store;
use meta::{start_raft_node, start_service, store, MetaApp};
use openraft::Raft;
use sled::Db;
use std::sync::Arc;
use std::time::Duration;
use trace::init_global_tracing;

#[derive(Parser, Clone, Debug)]
#[clap(author, version, about, long_about = None)]
pub struct Option {
    #[clap(long)]
    pub id: u64,

    #[clap(long)]
    pub http_addr: String,

    #[clap(long)]
    pub rpc_addr: String,

    /// The application specific name of this Raft cluster
    #[clap(
        long,
        env = "RAFT_SNAPSHOT_PATH",
        default_value = "/tmp/cnosdb/meta/snapshot"
    )]
    pub snapshot_path: String,

    #[clap(long, env = "RAFT_INSTANCE_PREFIX", default_value = "match")]
    pub instance_prefix: String,

    #[clap(
        long,
        env = "RAFT_JOURNAL_PATH",
        default_value = "/tmp/cnosdb/meta/journal"
    )]
    pub journal_path: String,

    #[clap(long, env = "RAFT_SNAPSHOT_PER_EVENTS", default_value = "500")]
    pub snapshot_per_events: u32,

    #[clap(long, env = "META_LOGS_PATH", default_value = "/tmp/cnosdb/meta/logs")]
    pub logs_path: String,

    #[clap(long, env = "META_LOGS_LEVEL", default_value = "info")]
    pub logs_level: String,
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let options = Option::parse();

    let logs_path = format!("{}/{}", options.logs_path, options.id);
    let _ = init_global_tracing(&logs_path, "meta_server.log", &config.logs_level);

    start_service(options).await
}

pub fn get_sled_db(config: &Option) -> Db {
    let db_path = format!(
        "{}/{}-{}.binlog",
        config.journal_path, config.instance_prefix, config.id
    );
    let db = sled::open(db_path.clone()).unwrap();
    tracing::debug!("get_sled_db: created log at: {:?}", db_path);
    db
}

pub async fn start_service(opt: Option) -> std::io::Result<()> {
    let mut config = RaftConfig::default().validate().unwrap();
    let config = Arc::new(config);
    let es = get_sled_db(&opt);
    let mut store = Arc::new(es);

    let network = Connections {};
    let raft = Raft::new(node_id, config.clone(), network, store.clone());
    let app = Data::new(MetaApp {
        id: node_id,
        addr: http_addr.clone(),
        rpc_addr: http_addr,
        raft: raft,
        store: store.clone(),
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
            .service(api::debug)
    })
    .keep_alive(Duration::from_secs(5));

    let x = server.bind(http_addr)?;

    x.run().await
}
