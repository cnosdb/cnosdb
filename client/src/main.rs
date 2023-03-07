use std::env;
use std::net::IpAddr;
use std::path::Path;

use clap::Parser;
use client::ctx::{SessionConfig, SessionContext};
use client::print_format::PrintFormat;
use client::print_options::PrintOptions;
use client::{exec, CNOSDB_CLI_VERSION};
use datafusion::error::Result;

#[derive(Debug, Parser, PartialEq)]
#[clap(author, version, about, long_about= None)]
struct Args {
    #[clap(
        short,
        long,
        help = "CnosDB server http api host",
        default_value = "0.0.0.0",
        validator(is_valid_host)
    )]
    host: String,

    #[clap(
        short = 'P',
        long,
        help = "CnosDB server http api port",
        default_value = "31001",
        validator(is_valid_port)
    )]
    port: usize,

    #[clap(
        short,
        long,
        help = "The user name used to connect to the CnosDB",
        default_value = "root"
    )]
    user: String,

    #[clap(short, long, help = "Password used to connect to the CnosDB")]
    password: Option<String>,

    #[clap(
        short,
        long,
        help = "Default database to connect to the CnosDB",
        default_value = "public"
    )]
    database: String,

    #[clap(
        short,
        long,
        help = "Default tenant to connect to the CnosDB",
        default_value = "cnosdb"
    )]
    tenant: String,

    #[clap(
        long,
        help = "Number of partitions for query execution. Increasing partitions can increase concurrency.",
        validator(is_valid_target_partitions)
    )]
    target_partitions: Option<usize>,

    #[clap(
        long,
        help = "Path to your data, default to current directory",
        validator(is_valid_data_dir)
    )]
    data_path: Option<String>,

    // #[clap(
    //     long,
    //     help = "The batch size of each query, or use CnosDB default",
    //     validator(is_valid_batch_size)
    // )]
    // batch_size: Option<usize>,
    #[clap(
        short,
        long,
        multiple_values = true,
        help = "Execute commands from file(s), then exit",
        validator(is_valid_file)
    )]
    file: Vec<String>,

    #[clap(
        long,
        multiple_values = true,
        help = "Run the provided files on startup instead of ~/.cnosdbrc",
        validator(is_valid_file),
        conflicts_with = "file"
    )]
    rc: Option<Vec<String>>,

    #[clap(long, arg_enum, default_value_t = PrintFormat::Table)]
    format: PrintFormat,

    #[clap(
        short,
        long,
        help = "Reduce printing other than the results and work quietly"
    )]
    quiet: bool,
}

#[tokio::main]
pub async fn main() -> Result<()> {
    env_logger::init();
    let args = Args::parse();

    if !args.quiet {
        println!("CnosDB CLI v{}", CNOSDB_CLI_VERSION);
        println!("Input arguments: {:?}", args);
    }

    if let Some(ref path) = args.data_path {
        let p = Path::new(path);
        env::set_current_dir(p).unwrap();
    };

    let session_config = SessionConfig::from_env()
        .with_host(args.host)
        .with_port(args.port)
        .with_user(args.user)
        .with_password(args.password)
        .with_tenant(args.tenant)
        .with_database(args.database)
        .with_target_partitions(args.target_partitions)
        .with_result_format(args.format);

    let mut ctx = SessionContext::new(session_config);

    let mut print_options = PrintOptions {
        format: args.format,
        quiet: args.quiet,
    };

    let files = args.file;
    let rc = match args.rc {
        Some(file) => file,
        None => {
            let mut files = Vec::new();
            let home = dirs::home_dir();
            if let Some(p) = home {
                let home_rc = p.join(".cnosdbrc");
                if home_rc.exists() {
                    files.push(home_rc.into_os_string().into_string().unwrap());
                }
            }
            files
        }
    };
    if !files.is_empty() {
        exec::exec_from_files(files, &mut ctx, &print_options).await
    } else {
        if !rc.is_empty() {
            exec::exec_from_files(rc, &mut ctx, &print_options).await
        }
        exec::exec_from_repl(&mut ctx, &mut print_options).await;
    }

    Ok(())
}

fn is_valid_host(address: &str) -> std::result::Result<(), String> {
    address
        .parse::<IpAddr>()
        .map(|_| ())
        .map_err(|e| e.to_string())
}

fn is_valid_file(dir: &str) -> std::result::Result<(), String> {
    if Path::new(dir).is_file() {
        Ok(())
    } else {
        Err(format!("Invalid file '{}'", dir))
    }
}

fn is_valid_data_dir(dir: &str) -> std::result::Result<(), String> {
    if Path::new(dir).is_dir() {
        Ok(())
    } else {
        Err(format!("Invalid data directory '{}'", dir))
    }
}

fn is_valid_target_partitions(size: &str) -> std::result::Result<(), String> {
    match size.parse::<usize>() {
        Ok(size) if size > 0 => Ok(()),
        _ => Err(format!("Invalid target partitions '{}'", size)),
    }
}

fn is_valid_port(size: &str) -> std::result::Result<(), String> {
    match size.parse::<usize>() {
        Ok(size) if size > 0 => Ok(()),
        _ => Err(format!("Invalid port range '{}'", size)),
    }
}
