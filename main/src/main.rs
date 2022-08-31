use std::{net::SocketAddr, sync::Arc};

use clap::{Parser, Subcommand};
use futures::join;
use once_cell::sync::Lazy;
use tokio::runtime::Runtime;

use async_channel as channel;

use protos::kv_service::tskv_service_server::TskvServiceServer;
use trace::init_default_global_tracing;
use tskv::TsKv;

mod http;
mod rpc;

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
        default_value = "127.0.0.1:31006"
    )]
    host: String,

    #[clap(
        global = true,
        env = "server_http_addr",
        default_value = "127.0.0.1:31007"
    )]
    http_host: String,

    #[clap(short, long, global = true)]
    /// the number of cores on the system
    cpu: Option<usize>,

    #[clap(short, long, global = true)]
    /// the number of cores on the system
    memory: Option<usize>,

    #[clap(global = true, default_value = "./config/config.toml")]
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
    /// run tskv
    #[clap(arg_required_else_help = true)]
    Tskv { debug: String },
    /// run query
    #[clap(arg_required_else_help = true)]
    Query {},
}

/// To run cnosdb-cli:
///
/// ```bash
/// cargo run -- tskv --cpu 1 --memory 64 debug
/// ```
fn main() -> Result<(), std::io::Error> {
    init_default_global_tracing("tskv_log", "tskv.log", "debug");
    install_crash_handler();
    let cli = Cli::parse();
    let runtime = init_runtime(cli.cpu)?;
    println!(
        "params: host:{}, http_host: {}, cpu:{:?}, memory:{:?}, config: {:?}, sub:{:?}",
        cli.host, cli.http_host, cli.cpu, cli.memory, cli.config, cli.subcmd
    );
    let global_config = config::get_config(cli.config.as_str());
    // TODO check global_config
    runtime.block_on(async move {
        match &cli.subcmd {
            SubCommand::Debug { debug } => {
                println!("Debug {}", debug);
            }
            SubCommand::Run {} => {}
            SubCommand::Tskv { debug } => {
                println!("TSKV {}", debug);

                let grpc_host = cli.host.parse::<SocketAddr>().expect("Invalid grpc_host");
                let http_host = cli
                    .http_host
                    .parse::<SocketAddr>()
                    .expect("Invalid http_host");

                //let (sender, receiver) = mpsc::unbounded_channel();
                let (sender, receiver) = channel::unbounded();

                let tskv_options = tskv::Options::from(&global_config);

                let tskv = Arc::new(TsKv::open(tskv_options).await.unwrap());

                for _ in 0..1 {
                    TsKv::start(tskv.clone(), receiver.clone());
                }

                let db =
                    Arc::new(server::instance::make_cnosdbms(tskv).expect("Failed to build dbms."));

                let tskv_grpc_service = TskvServiceServer::new(rpc::tskv::TskvServiceImpl {
                    sender: sender.clone(),
                });
                let mut grpc_builder = tonic::transport::server::Server::builder();
                let grpc_router = grpc_builder.add_service(tskv_grpc_service);
                let grpc = tokio::spawn(async move {
                    if let Err(e) = grpc_router.serve(grpc_host).await {
                        eprintln!("{}", e);
                        std::process::exit(1)
                    }
                });

                let http = tokio::spawn(async move {
                    if let Err(e) = http::serve(http_host, db, sender).await {
                        eprintln!("{}", e);
                        std::process::exit(1)
                    }
                });

                let (grpc_ret, http_ret) = join!(grpc, http);
                grpc_ret.unwrap();
                http_ret.unwrap();
            }
            SubCommand::Query {} => todo!(),
        }
    });
    Ok(())
}

fn install_crash_handler() {
    unsafe {
        // handle segfaults
        set_signal_handler(libc::SIGSEGV, signal_handler);
        // handle stack overflow and unsupported CPUs
        set_signal_handler(libc::SIGILL, signal_handler);
        // handle invalid memory access
        set_signal_handler(libc::SIGBUS, signal_handler);
    }
}

unsafe extern "C" fn signal_handler(sig: i32) {
    use std::process::abort;

    use backtrace::Backtrace;
    let name = std::thread::current()
        .name()
        .map(|n| format!(" for thread \"{}\"", n))
        .unwrap_or_else(|| "".to_owned());
    eprintln!(
        "Signal {}, Stack trace{}\n{:?}",
        sig,
        name,
        Backtrace::new()
    );
    abort();
}

// based on https://github.com/adjivas/sig/blob/master/src/lib.rs#L34-L52
unsafe fn set_signal_handler(signal: libc::c_int, handler: unsafe extern "C" fn(libc::c_int)) {
    use libc::{sigaction, sigfillset, sighandler_t};
    let mut sigset = std::mem::zeroed();
    if sigfillset(&mut sigset) != -1 {
        let mut action: sigaction = std::mem::zeroed();
        action.sa_mask = sigset;
        action.sa_sigaction = handler as sighandler_t;
        sigaction(signal, &action, std::ptr::null_mut());
    }
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
