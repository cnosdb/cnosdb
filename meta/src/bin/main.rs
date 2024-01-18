#![allow(dead_code, clippy::field_reassign_with_default)]

use clap::Parser;
use meta::signal;
use meta::store::{self};
use trace::global_logging::init_global_logging;

#[derive(Debug, Parser)]
struct Cli {
    /// configuration path
    #[arg(short, long)]
    config: Option<String>,
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    let mut options = store::config::get_opt(cli.config);
    options.log.path = format!("{}/{}", options.log.path, options.id);
    init_global_logging(&options.log, "meta_server.log");

    meta::service::server::start_raft_node(options)
        .await
        .unwrap();

    signal::block_waiting_ctrl_c();
}
