use std::net::SocketAddr;

use clap::{Parser, Subcommand};
use once_cell::sync::Lazy;
use protos::kv_service::tskv_service_server::TskvServiceServer;
use tokio::{runtime::Runtime, sync::mpsc};

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

    #[clap(short, long, global = true)]
    /// the number of cores on the system
    cpu: Option<usize>,

    #[clap(short, long, global = true)]
    /// the number of cores on the system
    memory: Option<usize>,

    #[clap(global = true, default_value = "../config/config.toml")]
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
    install_crash_handler();
    let cli = Cli::parse();
    let runtime = init_runtime(cli.cpu)?;
    println!(
        "params: host:{}, cpu:{:?}, memory:{:?}, config: {:?}, sub:{:?}",
        cli.host, cli.cpu, cli.memory, cli.config, cli.subcmd
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

                let host = cli.host.parse::<SocketAddr>().expect("Invalid host");

                let (sender, receiver) = mpsc::unbounded_channel();

                let tskv_options = tskv::Options::from(global_config);
                let tskv = tskv::TsKv::open(tskv_options, global_config.tsfamily_num)
                    .await
                    .unwrap();
                tskv::TsKv::start(tskv, receiver);

                let tskv_impl = rpc::tskv::TskvServiceImpl { sender };

                let tskv_service = TskvServiceServer::new(tskv_impl);

                let mut builder = tonic::transport::server::Server::builder();
                let router = builder.add_service(tskv_service);

                if let Err(e) = router.serve(host).await {
                    eprintln!("{}", e);
                    std::process::exit(1)
                }
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
