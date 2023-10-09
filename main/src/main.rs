#![allow(dead_code)]

use std::fmt::Display;
use std::path::Path;
use std::str::FromStr;
use std::sync::Arc;

use clap::{command, Args, Parser, Subcommand, ValueEnum};
use config::Config;
use memory_pool::GreedyMemoryPool;
use metrics::init_tskv_metrics_recorder;
use metrics::metric_register::MetricsRegister;
use once_cell::sync::Lazy;
use parking_lot::Mutex;
use tokio::runtime::Runtime;
use trace::jaeger::jaeger_exporter;
use trace::log::{CombinationTraceCollector, LogTraceCollector};
use trace::{info, init_process_global_tracing, TraceExporter, WorkerGuard};
use trace_http::ctx::{SpanContextExtractor, TraceHeaderParser};

use crate::report::ReportService;

mod flight_sql;
mod http;
mod report;
mod rpc;
mod server;
mod signal;
mod spi;
mod tcp;
mod vector;

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
    /// Print default configurations.
    Config,
    /// Check the configuration file in the given path.
    Check {
        #[command(subcommand)]
        subcmd: CheckCommand,
    },
}

#[derive(Debug, Args)]
struct RunArgs {
    /// Number of CPUs on the system, the default value is 4
    #[arg(short, long, global = true)]
    cpu: Option<usize>,

    /// Gigabytes(G) of memory on the system, the default value is 16
    #[arg(short, long, global = true)]
    memory: Option<usize>,

    /// Path to configuration file.
    #[arg(long, global = true)]
    config: Option<String>,

    /// The deployment mode of CnosDB,
    #[arg(short = 'M', long, global = true, value_enum)]
    deployment_mode: Option<DeploymentMode>,
}

#[derive(Debug, Copy, Clone, ValueEnum)]
#[clap(rename_all = "snake_case")]
enum DeploymentMode {
    /// Default, Run query and tskv engines.
    QueryTskv,
    /// Only run the tskv engine.
    Tskv,
    /// Only run the query engine.
    Query,
    /// Stand-alone deployment.
    Singleton,
}

impl FromStr for DeploymentMode {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mode = match s.to_ascii_lowercase().as_str() {
            "query_tskv" => Self::QueryTskv,
            "tskv" => Self::Tskv,
            "query" => Self::Query,
            "singleton" => Self::Singleton,
            _ => {
                return Err(
                    "deployment must be one of [query_tskv, tskv, query, singleton]".to_string(),
                )
            }
        };
        Ok(mode)
    }
}

impl Display for DeploymentMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DeploymentMode::QueryTskv => write!(f, "query_tskv"),
            DeploymentMode::Tskv => write!(f, "tskv"),
            DeploymentMode::Query => write!(f, "query"),
            DeploymentMode::Singleton => write!(f, "singleton"),
        }
    }
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

#[cfg(unix)]
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
        CliCommand::Config => {
            println!("{}", Config::default().to_string_pretty());
            return Ok(());
        }
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

    let mut config = parse_config(run_args.config.as_ref());
    let deployment_mode =
        get_final_deployment_mode(run_args.deployment_mode, &config.deployment.mode)?;
    set_cli_args_to_config(&run_args, &mut config);

    init_process_global_tracing(
        &config.log.path,
        &config.log.level,
        "tsdb.log",
        config.log.tokio_trace.as_ref(),
        &GLOBAL_MAIN_LOG_GUARD,
    );
    init_tskv_metrics_recorder();

    let runtime = Arc::new(init_runtime(Some(config.deployment.cpu))?);
    let mem_bytes = run_args.memory.unwrap_or(config.deployment.memory) * 1024 * 1024 * 1024;
    let memory_pool = Arc::new(GreedyMemoryPool::new(mem_bytes));
    runtime.clone().block_on(async move {
        let builder = server::ServiceBuilder {
            cpu: config.deployment.cpu,
            config: config.clone(),
            runtime: runtime.clone(),
            memory_pool: memory_pool.clone(),
            metrics_register: Arc::new(MetricsRegister::new([(
                "node_id",
                config.node_basic.node_id.to_string(),
            )])),
            span_context_extractor: build_span_context_extractor(&config),
        };

        let mut server = server::Server::default();
        if !config.reporting_disabled {
            server.add_service(Box::new(ReportService::new()));
        }

        let storage = match deployment_mode {
            DeploymentMode::QueryTskv => builder.build_query_storage(&mut server).await,
            DeploymentMode::Tskv => builder.build_storage_server(&mut server).await,
            DeploymentMode::Query => builder.build_query_server(&mut server).await,
            DeploymentMode::Singleton => builder.build_singleton(&mut server).await,
        };

        info!("CnosDB server start as {} mode", deployment_mode);
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
/// Merge the deployment configs(mode) between CLI arguments and config file,
/// values in the CLI arguments (if any) has higher priority.
fn get_final_deployment_mode(
    arg_deployment_mode: Option<DeploymentMode>,
    config_deployment_mode: &str,
) -> Result<DeploymentMode, std::io::Error> {
    if let Some(mode) = arg_deployment_mode {
        Ok(mode)
    } else {
        match config_deployment_mode.parse::<DeploymentMode>() {
            Ok(mode) => Ok(mode),
            Err(e) => Err(std::io::Error::new(std::io::ErrorKind::Other, e)),
        }
    }
}

/// When the command line and the configuration file specify a setting at the same time,
/// the command line has higher priority
fn set_cli_args_to_config(args: &RunArgs, config: &mut Config) {
    if let Some(mode) = args.deployment_mode {
        config.deployment.mode = mode.to_string();
    }

    if let Some(m) = args.memory {
        config.deployment.memory = m;
    }

    if let Some(c) = args.cpu {
        config.deployment.cpu = c;
    }
}

fn build_span_context_extractor(config: &Config) -> Arc<SpanContextExtractor> {
    let mut res: Vec<Arc<dyn TraceExporter>> = Vec::new();
    let mode = &config.deployment.mode;
    let node_id = config.node_basic.node_id;
    let service_name = format!("cnosdb_{mode}_{node_id}");

    if let Some(trace_log_collector_config) = &config.trace.log {
        info!(
            "Log trace collector created, path: {}",
            trace_log_collector_config.path.display()
        );
        res.push(Arc::new(LogTraceCollector::new(trace_log_collector_config)))
    }

    if let Some(trace_config) = &config.trace.jaeger {
        let exporter =
            jaeger_exporter(trace_config, service_name).expect("build jaeger trace exporter");
        info!("Jaeger trace exporter created");
        res.push(exporter);
    }

    // TODO HttpCollector
    let collector: Option<Arc<dyn TraceExporter>> = if res.is_empty() {
        None
    } else if res.len() == 1 {
        res.pop()
    } else {
        Some(Arc::new(CombinationTraceCollector::new(res)))
    };

    let parser = TraceHeaderParser::new(config.trace.auto_generate_span);

    Arc::new(SpanContextExtractor::new(parser, collector))
}
