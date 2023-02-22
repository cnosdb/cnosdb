#![allow(
    dead_code,
    unused_imports,
    unused_variables,
    clippy::field_reassign_with_default
)]

use std::sync::Arc;
use std::time::Duration;

use actix_web::middleware::Logger;
use actix_web::web::Data;
use actix_web::{middleware, App, HttpServer};
use clap::Parser;
use meta::service::connection::Connections;
use meta::service::{api, raft_api};
use meta::store::config::Opt;
use meta::store::Store;
use meta::{store, MetaApp, RaftStore};
use openraft::{Config, Raft};
use sled::Db;
use trace::init_global_tracing;

#[derive(Debug, clap::Parser)]
struct Cli {
    /// configuration path
    #[clap(short, long, default_value = "./config.toml")]
    config: String,
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let cli = Cli::parse();
    let options = store::config::get_opt(cli.config);
    let logs_path = format!("{}/{}", options.logs_path, options.id);
    let _ = init_global_tracing(&logs_path, "meta_server.log", &options.logs_level);

    start_service(options).await
}

pub fn get_sled_db(config: &Opt) -> Db {
    let db_path = format!("{}/{}.binlog", config.journal_path, config.id);
    let db = sled::open(db_path.clone()).unwrap();
    tracing::info!("get_sled_db: created log at: {:?}", db_path);
    db
}

pub async fn start_service(opt: Opt) -> std::io::Result<()> {
    let mut config = Config::default();
    config.enable_tick = true;
    config.enable_elect = true;
    config.enable_heartbeat = true;
    config.heartbeat_interval = 1000;
    config.election_timeout_min = 3000;
    config.election_timeout_max = 4000;
    config.install_snapshot_timeout = 100000;
    config.cluster_name = "cnosdb".to_string();
    let config = config.validate().unwrap();

    let config = Arc::new(config);
    let meta_init = Arc::new(opt.meta_init.clone());
    let es = get_sled_db(&opt);
    let store = Arc::new(Store::new(es));

    let network = Connections::new();
    let raft = RaftStore::new(opt.id, config.clone(), network, store.clone());
    let app = Data::new(MetaApp {
        id: opt.id,
        http_addr: opt.http_addr.clone(),
        rpc_addr: opt.http_addr.clone(),
        raft,
        store,
        config,
        meta_init,
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
            .service(api::watch)
            .service(api::pprof_test)
    })
    .keep_alive(Duration::from_secs(5));

    let x = server.bind(opt.http_addr)?;

    x.run().await
}
