#![allow(dead_code, clippy::field_reassign_with_default)]

use clap::{Args, Parser, Subcommand};
use config::VERSION;
use meta::signal;
use trace::global_logging::init_global_logging;

#[derive(Debug, Parser)]
#[command(name = "cnosdb-meta", version = & VERSION[..])]
struct Cli {
    #[command(subcommand)]
    subcmd: CliCommand,
}

#[derive(Debug, Subcommand)]
enum CliCommand {
    /// Run CnosDB meta server.
    Run(MetaConfig),
    /// Print default configurations.
    Config,
}
#[derive(Debug, Args)]
struct MetaConfig {
    #[arg(short, long)]
    config: Option<String>,
}
#[tokio::main]
async fn main() -> Result<(), std::io::Error> {
    let cli = Cli::parse();
    let run_args = match cli.subcmd {
        CliCommand::Run(run_args) => run_args,
        CliCommand::Config => {
            println!("{}", config::meta::Opt::default().to_string_pretty());
            return Ok(());
        }
    };
    let mut options = config::meta::get_opt(run_args.config);
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
    Ok(())
}
