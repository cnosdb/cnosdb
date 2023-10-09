#![allow(dead_code, clippy::field_reassign_with_default)]

use std::sync::Arc;

use clap::Parser;
use meta::store::{self};
use once_cell::sync::Lazy;
use parking_lot::Mutex;
use trace::{init_process_global_tracing, WorkerGuard};

static GLOBAL_META_LOG_GUARD: Lazy<Arc<Mutex<Option<Vec<WorkerGuard>>>>> =
    Lazy::new(|| Arc::new(Mutex::new(None)));

#[derive(Debug, Parser)]
struct Cli {
    /// configuration path
    #[arg(short, long, default_value = "./config.toml")]
    config: String,
}

#[tokio::main]
async fn main() {
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

    meta::service::server::start_raft_node(options)
        .await
        .unwrap();
}
