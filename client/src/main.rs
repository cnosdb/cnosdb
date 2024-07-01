use std::path::{Path, PathBuf};
use std::{env, fs};

use clap::builder::PossibleValuesParser;
use clap::{value_parser, Args, Parser, Subcommand};
use client::ctx::{SessionConfig, SessionContext};
use client::print_format::PrintFormat;
use client::print_options::PrintOptions;
use client::{exec, CNOSDB_CLI_VERSION};
use config::VERSION;
use http_protocol::encoding::Encoding;

#[derive(Debug, Clone, Parser, PartialEq)]
#[command(name = "cnosdb-cli", author, version = & VERSION[..], about, long_about= None)]
struct CliArgs {
    #[command(subcommand)]
    subcommand: Option<CliCommand>,

    /// Host of CnosDB server
    #[arg(
        short = 'H', long,
        default_value = "localhost",
        value_parser = try_parse_host,
    )]
    host: String,

    /// Port of CnosDB server HTTP API
    #[arg(
        short = 'P', long,
        default_value = "8902",
        value_parser = value_parser!(u16).range(0..=65535),
    )]
    port: u16,

    /// Username to connect to CnosDB server
    #[arg(short, long, default_value = "root")]
    user: String,

    /// Use password to connect to CnosDB server
    #[arg(short, long, default_value = "false")]
    password: bool,

    /// Rsa private key path for key pair authentication used to connect to the CnosDB
    #[arg(long)]
    private_key_path: Option<String>,

    /// Default database to connect to the CnosDB.
    #[arg(short, long, default_value = "public")]
    database: String,

    /// Default tenant to connect to the CnosDB.
    #[arg(short, long, default_value = "cnosdb")]
    tenant: String,

    /// The precision of the unix timestamps, will be used as the url param 'precision'
    #[arg(long, value_parser = PossibleValuesParser::new(["ns", "us", "ms"]))]
    precision: Option<String>,

    /// Number of partitions for query execution. Increasing partitions can increase concurrency
    #[arg(long, value_parser = try_parse_target_partitions)]
    target_partitions: Option<usize>,

    /// Optionally, specify the micro batch stream trigger interval. e.g. once, 1m, 10s
    #[arg(short, long)]
    stream_trigger_interval: Option<String>,

    /// Path to your data, default to current directory
    #[arg(long, value_parser = try_parse_data_dir)]
    data_path: Option<String>,

    /// HTTP response encoding. Support deflate, gzip, br, zstd
    #[arg(long,  value_parser = try_parse_encoding)]
    receive_data_encoding: Option<Encoding>,

    /// HTTP request encoding. Support deflate, gzip, br, zstd
    #[arg(long,  value_parser = try_parse_encoding)]
    send_data_encoding: Option<Encoding>,

    /// Execute commands from file(s), then exit
    #[arg(
        short, long,
        num_args = 0..,
        value_parser = try_parse_file,
    )]
    file: Vec<String>,

    /// Run the provided files on startup instead of ~/.cnosdbrc
    #[arg(
        long,
        num_args = 0..,
        value_parser = try_parse_file,
        conflicts_with = "file",
    )]
    rc: Option<Vec<String>>,

    #[arg(long, value_enum, default_value_t = PrintFormat::Table)]
    format: PrintFormat,

    /// Reduce printing other than the results and work quietly
    #[arg(short, long)]
    quiet: bool,

    /// Write line protocol from file
    #[arg(short = 'W', long, value_name = "FILE")]
    write_line_protocol: Option<PathBuf>,

    /// Use HTTPS connection
    #[arg(name = "ssl", long)]
    use_ssl: bool,

    /// Allow unsafe HTTPS connections
    #[arg(name = "unsafe-ssl", long)]
    use_unsafe_ssl: bool,

    /// Use the specified certificate file to verify the connection peer.
    /// The certificate(s) must be in PEM format
    #[arg(long, value_name = "FILE")]
    cacert: Vec<String>,

    /// Proxy URL, for HTTP or HTTPS requests
    #[arg(long, value_name = "URL")]
    proxy_url: Option<String>,

    /// Enable chunk mode, and CnosDB server uses http streaming output
    #[arg(long, default_value = "false")]
    chunked: bool,

    /// Stop when an error is encounter
    #[arg(long, default_value = "false")]
    error_stop: bool,

    /// Enable client command
    #[arg(long, default_value = "false")]
    process_cli_command: bool,
}

impl CliArgs {
    fn to_session_config(&self) -> SessionConfig {
        SessionConfig::from_env()
            .with_host(self.host.clone())
            .with_port(self.port)
            .with_user(self.user.clone())
            .with_tenant(self.tenant.clone())
            .with_database(self.database.clone())
            .with_target_partitions(self.target_partitions)
            .with_stream_trigger_interval(self.stream_trigger_interval.clone())
            .with_accept_encoding(self.receive_data_encoding)
            .with_content_encoding(self.send_data_encoding)
            .with_result_format(self.format)
            .with_precision(self.precision.clone())
            .with_ssl(self.use_ssl)
            .with_unsafe_ssl(self.use_unsafe_ssl)
            .with_ca_certs(self.cacert.clone())
            .with_proxy_url(self.proxy_url.clone())
            .with_chunked(self.chunked)
            .with_process_cli_command(self.process_cli_command)
            .with_error_stop(self.error_stop)
    }
}

#[derive(Debug, Clone, Subcommand, PartialOrd, PartialEq)]
enum CliCommand {
    DumpDDL(DumpDDL),

    RestoreDumpDDL(RestoreDumpDDL),
}

/// Dump ddl to files, Support multi tenants
#[derive(Debug, Clone, Args, PartialOrd, PartialEq)]
struct DumpDDL {
    /// Dump tenants
    #[arg(short, long)]
    tenant: Vec<Option<String>>,
}

/// Restore database from files
#[derive(Debug, Clone, Args, PartialOrd, PartialEq)]
struct RestoreDumpDDL {
    /// Tenant wanna restore
    #[arg(short, long)]
    tenant: Option<String>,

    /// Restore files
    #[arg()]
    files: Vec<String>,
}

#[tokio::main]
pub async fn main() -> Result<(), anyhow::Error> {
    env_logger::init();
    let args = CliArgs::parse();

    if !args.quiet && args.subcommand.is_none() {
        println!("CnosDB CLI v{}", CNOSDB_CLI_VERSION);
        println!("Input arguments: {:?}", args);
    }

    let password = if args.password {
        Some(rpassword::prompt_password("password: ").unwrap())
    } else {
        None
    };

    if let Some(ref path) = args.data_path {
        let p = Path::new(path);
        env::set_current_dir(p).unwrap();
    };

    let private_key = args.private_key_path.as_ref().map(|p| {
        let p = Path::new(p);
        fs::read_to_string(p).expect("Read private key file.")
    });

    let session_config = args
        .to_session_config()
        .with_password(password)
        .with_private_key(private_key);

    let mut ctx = SessionContext::new(session_config);
    if let Some(ref path) = args.write_line_protocol {
        ctx.write(path).await?;
        return Ok(());
    }

    let print_options = PrintOptions {
        format: args.format,
        quiet: args.quiet,
    };

    match args.subcommand {
        Some(CliCommand::DumpDDL(d)) => {
            let res = ctx.dump(d.tenant).await?;
            println!("{}", res);
            return Ok(());
        }
        Some(CliCommand::RestoreDumpDDL(r)) => {
            ctx.get_mut_session_config().process_cli_command = true;
            if let Some(t) = r.tenant {
                ctx.set_tenant(t)
            }
            let files = r.files;
            return exec::exec_from_files(files, &mut ctx, &print_options).await;
        }
        None => {}
    }

    let files = args.file.clone();
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
        exec::exec_from_files(files, &mut ctx, &print_options).await?;
    } else {
        if !rc.is_empty() {
            exec::exec_from_files(rc, &mut ctx, &print_options).await?;
        }
        exec::exec_from_repl(&mut ctx, &print_options).await;
    }

    Ok(())
}

fn try_parse_host(address: &str) -> std::result::Result<String, String> {
    if address.trim().is_empty() {
        return Err("host cannot be empty".to_string());
    }
    Ok(address.to_string())
}

fn try_parse_file(dir: &str) -> std::result::Result<String, String> {
    if Path::new(&dir).is_file() {
        Ok(dir.to_string())
    } else {
        Err("file must be a file".to_string())
    }
}

fn try_parse_data_dir(dir: &str) -> std::result::Result<String, String> {
    if Path::new(&dir).is_dir() {
        Ok(dir.to_string())
    } else {
        Err("data-dir must be a directory".to_string())
    }
}

fn try_parse_target_partitions(size: &str) -> std::result::Result<usize, String> {
    match size.parse::<usize>() {
        Ok(s) if s > 0 => Ok(s),
        _ => Err(format!("target-partitions is not in 1..={}", usize::MAX)),
    }
}

fn try_parse_encoding(encoding: &str) -> std::result::Result<Encoding, String> {
    match Encoding::from_str_opt(encoding) {
        Some(encoding) => Ok(encoding),
        _ => Err(format!("encoding not support: {}", encoding)),
    }
}
