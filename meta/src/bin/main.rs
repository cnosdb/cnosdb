#![allow(dead_code, clippy::field_reassign_with_default)]

use std::sync::Arc;
use std::time::Duration;

use actix_web::middleware::Logger;
use actix_web::web::Data;
use actix_web::{middleware, App, HttpServer};
use clap::Parser;
use meta::service::connection::Connections;
use meta::service::{api, raft_api};
use meta::store::command::WriteCommand;
use meta::store::config::{HeartBeatConfig, Opt};
use meta::store::Store;
use meta::{store, MetaApp, RaftStore};
use models::meta_data::NodeMetrics;
use models::node_info::NodeStatus;
use models::utils::{build_address, now_timestamp_secs};
use once_cell::sync::Lazy;
use openraft::Config;
use parking_lot::Mutex;
use sled::Db;
use trace::{init_process_global_tracing, warn, WorkerGuard};

use crate::store::key_path::KeyPath;

static GLOBAL_META_LOG_GUARD: Lazy<Arc<Mutex<Option<Vec<WorkerGuard>>>>> =
    Lazy::new(|| Arc::new(Mutex::new(None)));

const DEFAULT_META_IP: &str = "0.0.0.0";

#[derive(Debug, Parser)]
struct Cli {
    /// configuration path
    #[arg(short, long, default_value = "./config.toml")]
    config: String,
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let cli = Cli::parse();
    let options = store::config::get_opt(cli.config);
    let logs_path = format!("{}/{}", options.log.path, options.id);
    init_process_global_tracing(
        &logs_path,
        &options.log.level,
        "meta_server.log",
        options.log.tokio_trace.as_ref(),
        &GLOBAL_META_LOG_GUARD,
    );
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

    let meta_ip = DEFAULT_META_IP.to_owned();
    let addr = build_address(opt.host.clone(), opt.port);
    let app = Data::new(MetaApp {
        id: opt.id,
        http_addr: addr.clone(),
        rpc_addr: addr,
        raft,
        store,
        config,
        meta_init,
    });

    tokio::spawn(detect_node_heartbeat(opt.heartbeat.clone(), app.clone()));

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
            .service(api::dump)
            .service(api::restore)
            .service(api::debug)
            .service(api::watch)
            .service(api::backtrace)
            .service(api::cpu_pprof)
    })
    .keep_alive(Duration::from_secs(5));

    let x = server.bind(build_address(meta_ip, opt.port))?;

    x.run().await
}

async fn detect_node_heartbeat(heartbeat_config: HeartBeatConfig, app: Data<MetaApp>) {
    let mut interval = tokio::time::interval(Duration::from_secs(
        heartbeat_config.heartbeat_recheck_interval,
    ));

    let metrics_path = KeyPath::data_nodes_metrics(&app.meta_init.cluster_name);
    loop {
        interval.tick().await;

        if let Ok(_leader) = app.raft.is_leader().await {
            let sm = app.store.state_machine.write().await;

            if let Ok(list) = sm.children_data::<NodeMetrics>(&metrics_path) {
                let node_metrics_list: Vec<NodeMetrics> = list.into_values().collect();

                let time = now_timestamp_secs();
                for node_metrics in node_metrics_list.iter() {
                    if time - heartbeat_config.heartbeat_expired_interval as i64 > node_metrics.time
                    {
                        let mut now_node_metrics = node_metrics.clone();
                        now_node_metrics.status = NodeStatus::Unreachable;
                        warn!(
                            "Data node '{}' report heartbeat late, maybe unreachable.",
                            node_metrics.id
                        );
                        let req = WriteCommand::ReportNodeMetrics(
                            app.meta_init.cluster_name.clone(),
                            now_node_metrics,
                        );

                        sm.process_write_command(&req);
                    }
                }
            }
        }
    }
}
