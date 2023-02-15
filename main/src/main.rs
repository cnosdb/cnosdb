#![allow(dead_code, unused_imports, unused_variables)]

use std::net::SocketAddr;
use std::sync::Arc;

use clap::{Parser, Subcommand};
use coordinator::hh_queue::HintedOffManager;
use coordinator::service::CoordService;
use coordinator::writer::PointWriter;
use mem_allocator::Jemalloc;
use meta::meta_manager::RemoteMetaManager;
use meta::{MetaClientRef, MetaRef};
use metrics::init_tskv_metrics_recorder;
use models::meta_data::NodeInfo;
use once_cell::sync::Lazy;
use query::instance::make_cnosdbms;
use tokio::runtime::Runtime;
use trace::{info, init_global_tracing};
use tskv::TsKv;

use crate::flight_sql::FlightSqlServiceAdapter;
use crate::http::http_service::{HttpService, ServerMode};
use crate::report::ReportService;
use crate::rpc::grpc_service::GrpcService;
use crate::tcp::tcp_service::TcpService;

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
    #[clap(short, long, global = true)]
    tcp_host: Option<String>,

    /// gRPC address
    #[clap(short, long, global = true)]
    grpc_host: Option<String>,

    /// http address
    #[clap(short, long, global = true)]
    http_host: Option<String>,

    #[clap(short, long, global = true, default_value_t = 4)]
    /// the number of cores on the system
    cpu: usize,

    #[clap(short, long, global = true, default_value_t = 16)]
    /// the number of memory on the system(GB)
    memory: usize,

    /// configuration path
    #[clap(long, global = true)]
    config: Option<String>,

    #[clap(subcommand)]
    subcmd: SubCommand,
}

#[derive(Debug, Subcommand)]
enum SubCommand {
    // #[clap(arg_required_else_help = true)]
    // Debug { debug: String },
    /// run cnosdb server
    #[clap(arg_required_else_help = false)]
    Run {},
    // /// run tskv
    #[clap(arg_required_else_help = true)]
    Tskv {},
    #[clap(arg_required_else_help = true)]
    Query {},
}

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
    let void_arg = "".to_string();
    println!(
        r#"----------
Start with cli arguments:
grpc_host = '{}'
http_host = '{}'
cpu = {}
memory = {}
config = '{}'
sub = {:?}"#,
        cli.grpc_host.as_ref().unwrap_or(&void_arg),
        cli.http_host.as_ref().unwrap_or(&void_arg),
        cli.cpu,
        cli.memory,
        cli.config.as_ref().unwrap_or(&void_arg),
        cli.subcmd
    );
    let global_config = if let Some(cfg_path) = cli.config {
        println!("----------\nStart with configuration:");
        config::get_config(cfg_path)
    } else {
        println!("----------\nStart with default configuration:");
        config::default_config()
    };
    println!("{}----------", global_config.to_string_pretty());

    let mut _trace_guard = init_global_tracing(
        &global_config.log.path,
        "tsdb.log",
        &global_config.log.level,
    );

    let http_host = cli
        .http_host
        .as_ref()
        .unwrap_or(&global_config.cluster.http_listen_addr)
        .parse::<SocketAddr>()
        .expect("Invalid http_host");
    let grpc_host = cli
        .grpc_host
        .as_ref()
        .unwrap_or(&global_config.cluster.grpc_listen_addr)
        .parse::<SocketAddr>()
        .expect("Invalid grpc_host");
    let tcp_host = cli
        .tcp_host
        .as_ref()
        .unwrap_or(&global_config.cluster.tcp_listen_addr)
        .parse::<SocketAddr>()
        .expect("Invalid http_host");

    let flight_rpc_host = global_config
        .cluster
        .flight_rpc_listen_addr
        .parse::<SocketAddr>()
        .expect("Invalid flight_rpc_host");

    init_tskv_metrics_recorder();

    runtime.clone().block_on(async move {
        let meta_manager: MetaRef = RemoteMetaManager::new(global_config.cluster.clone()).await;
        let options = tskv::Options::from(&global_config);
        let (mut server, tskv) = match &cli.subcmd {
            SubCommand::Tskv {} => {
                meta_manager.admin_meta().add_data_node().await.unwrap();
                let kv_inst = Arc::new(
                    TsKv::open(meta_manager.clone(), options.clone(), runtime)
                        .await
                        .unwrap(),
                );
                let coord_service = CoordService::new(
                    Some(kv_inst.clone()),
                    meta_manager,
                    global_config.cluster.clone(),
                    global_config.hintedoff.clone(),
                )
                .await;

                let dbms = Arc::new(
                    make_cnosdbms(coord_service.clone(), options.clone())
                        .await
                        .expect("make dbms"),
                );

                let tcp_service = Box::new(TcpService::new(coord_service.clone(), tcp_host));

                let tls_config = global_config.security.tls_config;
                let http_service = Box::new(HttpService::new(
                    dbms,
                    coord_service,
                    http_host,
                    tls_config.clone(),
                    global_config.query.query_sql_limit,
                    global_config.query.write_sql_limit,
                    ServerMode::Store,
                ));
                let grpc_service =
                    Box::new(GrpcService::new(kv_inst.clone(), grpc_host, tls_config));

                let report_service = Box::new(ReportService::new());
                let mut server_builder = server::Builder::default()
                    .add_service(http_service)
                    .add_service(grpc_service)
                    .add_service(tcp_service);

                if !global_config.reporting_disabled {
                    server_builder = server_builder.add_service(report_service);
                }

                (
                    server_builder.build().expect("build server."),
                    Some(kv_inst),
                )
            }
            SubCommand::Query {} => {
                let options = tskv::Options::from(&global_config);
                let coord_service = CoordService::new(
                    None,
                    meta_manager,
                    global_config.cluster.clone(),
                    global_config.hintedoff.clone(),
                )
                .await;

                let dbms = Arc::new(
                    make_cnosdbms(coord_service.clone(), options)
                        .await
                        .expect("make dbms"),
                );

                let tls_config = global_config.security.tls_config;
                let http_service = Box::new(HttpService::new(
                    dbms.clone(),
                    coord_service,
                    http_host,
                    tls_config.clone(),
                    global_config.query.query_sql_limit,
                    global_config.query.write_sql_limit,
                    ServerMode::Query,
                ));
                let flight_sql_service = Box::new(FlightSqlServiceAdapter::new(
                    dbms,
                    flight_rpc_host,
                    tls_config,
                ));

                let report_service = Box::new(ReportService::new());

                let mut server_builder = server::Builder::default()
                    .add_service(http_service)
                    .add_service(flight_sql_service);

                if !global_config.reporting_disabled {
                    server_builder = server_builder.add_service(report_service);
                }

                (server_builder.build().expect("build server."), None)
            }
            SubCommand::Run {} => {
                meta_manager.admin_meta().add_data_node().await.unwrap();
                let kv_inst = Arc::new(
                    TsKv::open(meta_manager.clone(), options.clone(), runtime)
                        .await
                        .unwrap(),
                );
                let coord_service = CoordService::new(
                    Some(kv_inst.clone()),
                    meta_manager,
                    global_config.cluster.clone(),
                    global_config.hintedoff.clone(),
                )
                .await;

                let dbms = Arc::new(
                    make_cnosdbms(coord_service.clone(), options.clone())
                        .await
                        .expect("make dbms"),
                );

                let tcp_service = Box::new(TcpService::new(coord_service.clone(), tcp_host));

                let tls_config = global_config.security.tls_config;
                let http_service = Box::new(HttpService::new(
                    dbms.clone(),
                    coord_service,
                    http_host,
                    tls_config.clone(),
                    global_config.query.query_sql_limit,
                    global_config.query.write_sql_limit,
                    ServerMode::Bundle,
                ));
                let grpc_service = Box::new(GrpcService::new(
                    kv_inst.clone(),
                    grpc_host,
                    tls_config.clone(),
                ));
                let flight_sql_service = Box::new(FlightSqlServiceAdapter::new(
                    dbms,
                    flight_rpc_host,
                    tls_config,
                ));

                let report_service = Box::new(ReportService::new());

                let mut server_builder = server::Builder::default()
                    .add_service(http_service)
                    .add_service(grpc_service)
                    .add_service(tcp_service)
                    .add_service(flight_sql_service);

                if !global_config.reporting_disabled {
                    server_builder = server_builder.add_service(report_service);
                }

                (
                    server_builder.build().expect("build server."),
                    Some(kv_inst),
                )
            }
        };
        server.start().expect("CnosDB server start.");
        signal::block_waiting_ctrl_c();
        server.stop(true).await;
        if let Some(kv_inst) = tskv {
            kv_inst.close().await;
        }
        println!("CnosDB is stopped.");
    });
    Ok(())
}

fn init_runtime(cores: Option<usize>) -> Result<Runtime, std::io::Error> {
    use tokio::runtime::Builder;
    match cores {
        None => Runtime::new(),
        Some(cores) => match cores {
            0 => Builder::new_multi_thread()
                .enable_all()
                .thread_stack_size(4 * 1024 * 1024)
                .build(),
            _ => Builder::new_multi_thread()
                .enable_all()
                .worker_threads(cores)
                .thread_stack_size(4 * 1024 * 1024)
                .build(),
        },
    }
}
