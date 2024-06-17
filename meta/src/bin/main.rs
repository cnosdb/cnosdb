#![allow(dead_code, clippy::field_reassign_with_default)]

use clap::Parser;
use config::VERSION;
use meta::signal;
use trace::global_logging::init_global_logging;

#[derive(Debug, Parser)]
#[command(name = "cnosdb-meta", version = & VERSION[..])]
struct Cli {
    /// configuration path
    #[arg(short, long)]
    config: Option<String>,
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    let mut options = config::meta::get_opt(cli.config);
    println!("-----------------------------------------------------------");
    println!("Start meta server with configuration:");
    println!("{}", options.to_string_pretty());
    println!("-----------------------------------------------------------");
    options.log.path = format!("{}/{}", options.log.path, options.id);
    init_global_logging(&options.log, "meta_server.log");

    meta::service::server::start_raft_node(options)
        .await
        .unwrap();

    signal::block_waiting_ctrl_c();
}
