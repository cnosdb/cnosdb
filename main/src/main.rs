#![allow(dead_code)]

use std::path::Path;
use std::sync::Arc;

use clap::{Args, Parser, Subcommand};
use memory_pool::GreedyMemoryPool;
use metrics::init_tskv_metrics_recorder;
use metrics::metric_register::MetricsRegister;
use once_cell::sync::Lazy;
use parking_lot::Mutex;
use tokio::runtime::Runtime;
use trace::{info, init_process_global_tracing, WorkerGuard};

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
/// <https://github.com/clap-rs/clap/blob/v3.1.3/examples/git-derive.rs>
#[derive(Debug, Parser)]
#[command(name = "cnosdb", version = & VERSION[..])]
#[command(about = "CnosDB command line tools")]
#[command(long_about = r#"CnosDB and command line tools
Examples:
    # Run the CnosDB:
    cnosdb run
    # Check configuration file:
    cnosdb check server-config ./config/config.toml"#)]
struct Cli {
    #[command(subcommand)]
    subcmd: CliCommand,
}

#[derive(Debug, Subcommand)]
enum CliCommand {
    /// Run CnosDB server.
    Run(RunArgs),
    /// Check configurations.
    Check {
        #[command(subcommand)]
        subcmd: CheckCommand,
    },
}

#[derive(Debug, Args)]
struct RunArgs {
    /// Number of CPUs on the system.
    #[arg(short, long, global = true, default_value_t = 4)]
    cpu: usize,

    /// Gigabytes(G) of memory on the system.
    #[arg(short, long, global = true, default_value_t = 16)]
    memory: usize,

    /// Path to configuration file.
    #[arg(long, global = true)]
    config: Option<String>,

    #[command(subcommand)]
    subcmd: Option<RunCommand>,
}

#[derive(Debug, Subcommand)]
enum RunCommand {
    /// Run storage engine (default).
    #[command()]
    Tskv {},
    /// Run query server.
    #[command(arg_required_else_help = true)]
    Query {},
    /// Run singleton server.
    #[command()]
    Singleton {},
}

#[derive(Debug, Subcommand)]
enum CheckCommand {
    /// Check server configurations.
    #[command(arg_required_else_help = false)]
    ServerConfig {
        /// Print warnings.
        #[arg(short, long)]
        show_warnings: bool,
        /// Path to configuration file.
        config: String,
    },
    // /// Check meta server configurations.
    // #[command(arg_required_else_help = false)]
    // MetaConfig {},
}

#[global_allocator]
static A: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

/// To run cnosdb-cli:
///
/// ```bash
/// cargo run -- run
/// ```
fn main() -> Result<(), std::io::Error> {
    signal::install_crash_handler();
    let cli = Cli::parse();
    let run_args = match cli.subcmd {
        CliCommand::Run(run_args) => run_args,
        CliCommand::Check { subcmd } => match subcmd {
            CheckCommand::ServerConfig {
                config,
                show_warnings,
            } => {
                config::check_config(config, show_warnings);
                return Ok(());
            }
        },
    };

    let config = parse_config(run_args.config.as_ref());

    init_process_global_tracing(
        &config.log.path,
        &config.log.level,
        "tsdb.log",
        config.log.tokio_trace.as_ref(),
        &GLOBAL_MAIN_LOG_GUARD,
    );
    init_tskv_metrics_recorder();

    let runtime = Arc::new(init_runtime(Some(run_args.cpu))?);
    let memory_size = run_args.memory * 1024 * 1024 * 1024;
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

        let storage = match &run_args.subcmd {
            None => builder.build_query_storage(&mut server).await,
            Some(RunCommand::Tskv {}) => builder.build_storage_server(&mut server).await,
            Some(RunCommand::Query {}) => builder.build_query_server(&mut server).await,
            Some(RunCommand::Singleton {}) => builder.build_singleton(&mut server).await,
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

fn parse_config(config_path: Option<impl AsRef<Path>>) -> config::Config {
    let global_config = if let Some(p) = config_path {
        println!("----------\nStart with configuration:");
        config::get_config(p).unwrap()
    } else {
        println!("----------\nStart with default configuration:");
        config::Config::default()
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
