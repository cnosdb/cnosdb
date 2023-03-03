#![allow(dead_code, unused_imports, unused_variables)]

use std::sync::Arc;

use clap::{Parser, Subcommand};
use config::TokioTrace;
use mem_allocator::Jemalloc;
use memory_pool::GreedyMemoryPool;
use metrics::init_tskv_metrics_recorder;
use metrics::metric_register::MetricsRegister;
use models::meta_data::NodeInfo;
use once_cell::sync::Lazy;
use parking_lot::{Mutex, Once};
use tokio::runtime::Runtime;
use trace::{info, init_global_tracing, init_process_global_tracing, WorkerGuard};

use crate::report::ReportService;

mod flight_sql;
mod http;
mod meta_single;
mod report;
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

static GLOBAL_MAIN_LOG_GUARD: Lazy<Arc<Mutex<Option<Vec<WorkerGuard>>>>> =
    Lazy::new(|| Arc::new(Mutex::new(None)));

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
    /// run tskv
    #[clap(arg_required_else_help = true)]
    Tskv {},
    /// run query
    #[clap(arg_required_else_help = true)]
    Query {},
    #[clap(arg_required_else_help = true)]
    Singleton {},
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
    let config = parse_config(&cli);

    init_process_global_tracing(
        &config.log.path,
        &config.log.level,
        "tsdb.log",
        config.log.tokio_trace.as_ref(),
        &GLOBAL_MAIN_LOG_GUARD,
    );
    init_tskv_metrics_recorder();

    let runtime = Arc::new(init_runtime(Some(cli.cpu))?);
    let memory_size = cli.memory * 1024 * 1024 * 1024;
    let memory_pool = Arc::new(GreedyMemoryPool::new(memory_size));
    runtime.clone().block_on(async move {
        let builder = server::ServiceBuilder {
            config: config.clone(),
            runtime: runtime.clone(),
            memory_pool: memory_pool.clone(),
            metrics_register: Arc::new(MetricsRegister::new([(
                "node_id",
                config.cluster.node_id.to_string(),
            )])),
        };

        let mut server = server::Server::default();
        if !config.reporting_disabled {
            server.add_service(Box::new(ReportService::new()));
        }

        let storage = match &cli.subcmd {
            SubCommand::Tskv {} => builder.build_storage_server(&mut server).await,
            SubCommand::Query {} => builder.build_query_server(&mut server).await,
            SubCommand::Run {} => builder.build_query_storage(&mut server).await,
            SubCommand::Singleton {} => builder.build_singleton(&mut server).await,
        };

        server.start().expect("CnosDB server start.");
        signal::block_waiting_ctrl_c();
        server.stop(true).await;
        if let Some(tskv) = storage {
            tskv.close().await;
        }

        println!("CnosDB is stopped.");
    });
    Ok(())
}

fn parse_config(cli: &Cli) -> config::Config {
    let global_config = if let Some(cfg_path) = cli.config.clone() {
        println!("----------\nStart with configuration:");
        config::get_config(cfg_path)
    } else {
        println!("----------\nStart with default configuration:");
        config::default_config()
    };
    println!("{}----------", global_config.to_string_pretty());

    global_config
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
