#![allow(dead_code, unused_imports, unused_variables)]

use clap::{Parser, Subcommand};
use coordinator::hh_queue::HintedOffManager;
use coordinator::service::CoordService;
use coordinator::writer::PointWriter;
use meta::meta_client::{MetaClientRef, MetaRef, RemoteMetaManager};
use models::meta_data::NodeInfo;
use once_cell::sync::Lazy;
use query::instance::make_cnosdbms;
use std::{net::SocketAddr, sync::Arc};
use tokio::runtime::Runtime;
use trace::{info, init_global_tracing};
use tskv::TsKv;
mod flight_sql;
mod http;
mod report;
mod rpc;
pub mod server;
mod signal;
mod tcp;

static VERSION: Lazy<String> = Lazy::new(|| {
    format!(
        "{}, revision {}",
        option_env!("CARGO_PKG_VERSION").unwrap_or("UNKNOWN"),
        option_env!("GIT_HASH").unwrap_or("UNKNOWN")
    )
});

/// cli examples is here
/// https://github.com/clap-rs/clap/blob/v3.1.3/examples/git-derive.rs
#[derive(Debug, clap::Parser)]
#[clap(name = "cnosdb")]
#[clap(version = & VERSION[..],
about = "cnosdb command line tools",
long_about = r#"cnosdb and command line tools
                        Examples:
                            # Run the cnosdb:
                            cargo run -- run
                        "#
)]
struct Cli {
    #[clap(
        short,
        long,
        global = true,
        env = "server_tcp_addr",
        default_value = "0.0.0.0:31005"
    )]
    tcp_host: String,

    /// gRPC address
    #[clap(
        short,
        long,
        global = true,
        env = "server_addr",
        default_value = "0.0.0.0:31006"
    )]
    grpc_host: String,

    /// http address
    #[clap(
        short,
        long,
        global = true,
        env = "server_http_addr",
        default_value = "0.0.0.0:31007"
    )]
    http_host: String,

    #[clap(short, long, global = true, default_value_t = 4)]
    /// the number of cores on the system
    cpu: usize,

    #[clap(short, long, global = true, default_value_t = 16)]
    /// the number of memory on the system(GB)
    memory: usize,

    /// configuration path
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
    #[clap(arg_required_else_help = false)]
    Run {},
    // /// run tskv
    // #[clap(arg_required_else_help = true)]
    // Tskv { debug: String },
    // /// run query
    // #[clap(arg_required_else_help = true)]
    // Query {},
}

use crate::flight_sql::FlightSqlServiceAdapter;
use crate::http::http_service::HttpService;
use crate::report::ReportService;
use crate::rpc::grpc_service::GrpcService;
use crate::tcp::tcp_service::TcpService;
use mem_allocator::Jemalloc;
use metrics::{init_query_metrics_recorder, init_tskv_metrics_recorder};

#[global_allocator]
static A: Jemalloc = Jemalloc;

/// To run cnosdb-cli:
///
/// ```bash
/// cargo run -- run
/// ```
fn main() -> Result<(), std::io::Error> {
    signal::install_crash_handler();
    let cli = Cli::parse();
    let runtime = init_runtime(Some(cli.cpu))?;
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

    // let grpc_host = cli
    //     .grpc_host
    //     .parse::<SocketAddr>()
    //     .expect("Invalid grpc_host");
    // let http_host = cli
    //     .http_host
    //     .parse::<SocketAddr>()
    //     .expect("Invalid http_host");

    // let tcp_host = cli
    //     .tcp_host
    //     .parse::<SocketAddr>()
    //     .expect("Invalid http_host");

    let grpc_host = global_config
        .cluster
        .grpc_server
        .parse::<SocketAddr>()
        .expect("Invalid grpc_host");
    let flight_rpc_host = global_config
        .cluster
        .flight_rpc_server
        .parse::<SocketAddr>()
        .expect("Invalid flight_rpc_host");
    let http_host = global_config
        .cluster
        .http_server
        .parse::<SocketAddr>()
        .expect("Invalid http_host");

    let tcp_host = global_config
        .cluster
        .tcp_server
        .parse::<SocketAddr>()
        .expect("Invalid http_host");

    init_tskv_metrics_recorder();
    init_query_metrics_recorder();

    runtime.clone().block_on(async move {
        match &cli.subcmd {
            SubCommand::Debug { debug: _ } => {
                todo!()
            }
            SubCommand::Run {} => {
                let tskv_options = tskv::Options::from(&global_config);
                let query_options = tskv::Options::from(&global_config);
                let kv_inst = Arc::new(
                    TsKv::open(global_config.cluster.clone(), tskv_options, runtime)
                        .await
                        .unwrap(),
                );
                let coord_service = CoordService::new(
                    kv_inst.clone(),
                    global_config.cluster.clone(),
                    global_config.hintedoff.clone(),
                ).await;

                let dbms = Arc::new(
                    make_cnosdbms(kv_inst.clone(), coord_service.clone(), query_options)
                        .expect("make dbms"),
                );

                let tcp_service = Box::new(TcpService::new(
                    dbms.clone(),
                    coord_service.clone(),
                    tcp_host,
                ));

                let tls_config = global_config.security.tls_config;
                let http_service = Box::new(HttpService::new(
                    dbms.clone(),
                    kv_inst.clone(),
                    coord_service,
                    http_host,
                    tls_config.clone(),
                    global_config.query.query_sql_limit,
                    global_config.query.write_sql_limit,
                ));
                let grpc_service = Box::new(GrpcService::new(
                    dbms.clone(),
                    kv_inst.clone(),
                    grpc_host,
                    tls_config.clone(),
                ));
                let flight_sql_service = Box::new(FlightSqlServiceAdapter::new(
                    dbms.clone(),
                    flight_rpc_host,
                    tls_config.clone(),
                ));

                let report_service = Box::new(ReportService::new());

                let mut server_builder = server::Builder::default()
                    .add_service(http_service)
                    .add_service(grpc_service)
                    .add_service(tcp_service)
                    .add_service(flight_sql_service);

                if !global_config.reporting_disabled.unwrap_or(false) {
                    server_builder = server_builder.add_service(report_service);
                }

                let mut server = server_builder.build().expect("build server.");

                server.start().expect("server start.");
                signal::block_waiting_ctrl_c();
                server.stop(true).await;
                kv_inst.close().await;
                println!("CnosDB is stopped.");
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
        Some(cores) => match cores {
            0 => {
                let msg = format!("Invalid core number: '{}' must be greater than zero", cores);
                Err(std::io::Error::new(kind, msg))
            }
            _ => Builder::new_multi_thread()
                .enable_all()
                .worker_threads(cores)
                .build(),
        },
    }
}
