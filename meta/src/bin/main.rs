#![allow(dead_code, clippy::field_reassign_with_default)]

use clap::{Parser, Subcommand};
use config::VERSION;
use meta::signal;
use trace::global_logging::init_global_logging;

#[derive(Debug, Parser)]
#[command(name = "cnosdb-meta", version = &VERSION[..])]
struct Cli {
    /// Subcommands
    #[command(subcommand)]
    command: Option<Commands>,

    /// Configuration file path
    #[arg(short, long)]
    config: Option<String>,
}

#[derive(Debug, Subcommand)]
enum Commands {
    /// Print the default configuration
    Config,
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    match cli.command {
        Some(Commands::Config) => {
            // Print default configuration
            let default_config = config::meta::Opt::default();
            println!("Default configuration:");
            println!("{}", default_config.to_string_pretty());
        }
        None => {
            // use --config to start cnosdb meta
            if let Some(config_path) = cli.config {
                let mut options = config::meta::get_opt(Some(config_path));
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
            } else {
                eprintln!("Please provide a config file or use the 'config' command.");
            }
        }
    }
}
