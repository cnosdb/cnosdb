use clap::{Parser, Subcommand};
use once_cell::sync::Lazy;
use query::instance::make_cnosdbms;
use std::{net::SocketAddr, sync::Arc};
use tokio::runtime::Runtime;
use trace::{info, init_global_tracing};
use tskv::TsKv;

mod http;
mod rpc;
pub mod server;
mod signal;

static VERSION: Lazy<String> = Lazy::new(|| {
    format!(
        "{}, revision {}",
        option_env!("CARGO_PKG_VERSION").unwrap_or("UNKNOWN"),
        option_env!("GIT_HASH").unwrap_or("UNKNOWN")
    )
});

// cli examples is here
// https://github.com/clap-rs/clap/blob/v3.1.3/examples/git-derive.rs
#[derive(Debug, clap::Parser)]
#[clap(name = "cnosdb")]
#[clap(version = & VERSION[..],
about = "cnosdb command line tools",
long_about = r#"cnosdb and command line tools
                        Examples:
                            # Run the cnosdb:
                            server run
                        "#
)]
struct Cli {
    /// gRPC address
    #[clap(
        short,
        long,
        global = true,
        env = "server_addr",
        default_value = "0.0.0.0:31006"
    )]
    grpc_host: String,

    #[clap(
        short,
        long,
        global = true,
        env = "server_http_addr",
        default_value = "0.0.0.0:31007"
    )]
    http_host: String,

    #[clap(short, long, global = true)]
    /// the number of cores on the system
    cpu: Option<usize>,

    #[clap(short, long, global = true)]
    /// the number of cores on the system
    memory: Option<usize>,

    #[clap(long, global = true, default_value = "./config/config.toml")]
    config: String,

    #[clap(subcommand)]
    subcmd: SubCommand,
}

#[derive(Debug, Subcommand)]
enum SubCommand {
    /// debug mode
    #[clap(arg_required_else_help = true)]
    Debug { debug: String },
    /// run cnosdb server
    #[clap(arg_required_else_help = true)]
    Run {},
    // /// run tskv
    // #[clap(arg_required_else_help = true)]
    // Tskv { debug: String },
    // /// run query
    // #[clap(arg_required_else_help = true)]
    // Query {},
}

use crate::http::http_service::HttpService;
use crate::rpc::grpc_service::GrpcService;
use mem_allocator::Jemalloc;

#[global_allocator]
static A: Jemalloc = Jemalloc;

/// To run cnosdb-cli:
///
/// ```bash
/// cargo run -- run --cpu 1 --memory 64 debug
/// ```
fn main() -> Result<(), std::io::Error> {
    signal::install_crash_handler();
    let cli = Cli::parse();
    let runtime = init_runtime(cli.cpu)?;
    let runtime = Arc::new(runtime);
    println!(
        "params: host:{}, http_host: {}, cpu:{:?}, memory:{:?}, config: {:?}, sub:{:?}",
        cli.grpc_host, cli.http_host, cli.cpu, cli.memory, cli.config, cli.subcmd
    );
    let global_config = config::get_config(cli.config.as_str());
    let mut _trace_guard = init_global_tracing(
        &global_config.log.path,
        "tsdb.log",
        &global_config.log.level,
    );

    let grpc_host = cli
        .grpc_host
        .parse::<SocketAddr>()
        .expect("Invalid grpc_host");
    let http_host = cli
        .http_host
        .parse::<SocketAddr>()
        .expect("Invalid http_host");

    runtime.clone().block_on(async move {
        match &cli.subcmd {
            SubCommand::Debug { debug: _ } => {
                todo!()
            }
            SubCommand::Run {} => {
                let tskv_options = tskv::Options::from(&global_config);
                let kv_inst = Arc::new(TsKv::open(tskv_options, runtime).await.unwrap());
                let dbms = Arc::new(make_cnosdbms(kv_inst.clone()).expect("make dbms"));
                let http_service =
                    Box::new(HttpService::new(dbms.clone(), kv_inst.clone(), http_host));
                let grpc_service =
                    Box::new(GrpcService::new(dbms.clone(), kv_inst.clone(), grpc_host));
                let mut server = server::Builder::default()
                    .add_service(http_service)
                    .add_service(grpc_service)
                    .build()
                    .expect("build server.");
                server.start().expect("server start.");
                signal::block_waiting_ctrl_c();
                server.stop(true).await;
            }
        }
    });
    Ok(())
}

fn init_runtime(cores: Option<usize>) -> Result<Runtime, std::io::Error> {
    use tokio::runtime::Builder;
    let kind = std::io::ErrorKind::Other;
    match cores {
        None => Runtime::new(),
        Some(cores) => {
            println!(
                "Setting core number to '{}' per command line request",
                cores
            );

            match cores {
                0 => {
                    let msg = format!("Invalid core number: '{}' must be greater than zero", cores);
                    Err(std::io::Error::new(kind, msg))
                }
                1 => Builder::new_current_thread().enable_all().build(),
                _ => Builder::new_multi_thread()
                    .enable_all()
                    .worker_threads(cores)
                    .build(),
            }
        }
    }
}
