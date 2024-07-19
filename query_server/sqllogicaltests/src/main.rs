use std::error::Error;
use std::path::{Path, PathBuf};
use std::process::exit;

use clap::Parser;

use crate::instance::{CreateOptions, SqlClientOptions};

mod db_request;
mod error;
mod instance;
mod os;
mod utils;

const TEST_DIRECTORY: &str = "query_server/sqllogicaltests/cases";

const CNOSDB_FLIGHT_HOST_ENV: &str = "CNOSDB_FLIGHT_HOST";
const CNOSDB_FLIGHT_PORT_ENV: &str = "CNOSDB_FLIGHT_PORT";
const CNOSDB_HTTP_HOST_ENV: &str = "CNOSDB_HTTP_HOST";
const CNOSDB_HTTP_PORT_ENV: &str = "CNOSDB_HTTP_PORT";
const CNOSDB_WORK_DIR_ENV: &str = "CNOSDB_WORK_DIR";

const CNOSDB_FLIGHT_HOST_DEFAULT: &str = "localhost";
const CNOSDB_FLIGHT_PORT_DEFAULT: u16 = 8904;
const CNOSDB_HTTP_HOST_DEFAULT: &str = "localhost";
const CNOSDB_HTTP_PORT_DEFAULT: u16 = 8902;

const CNOSDB_USERNAME_DEFAULT: &str = "root";
const CNOSDB_PASSWORD_DEFAULT: &str = "";
const CNOSDB_TENANT_DEFAULT: &str = "cnosdb";
const CNOSDB_DB_DEFAULT: &str = "public";
const CNOSDB_TARGET_PARTITIONS_DEFAULT: usize = 8;

#[tokio::main]
pub async fn main() -> Result<(), Box<dyn Error>> {
    let options = Options::new();

    if options.mode != "singleton" && options.mode != "cluster" {
        eprintln!("Unsupported mode: {}", options.mode);
        exit(1);
    }

    let db_options = SqlClientOptions {
        flight_host: options.flight_host.clone(),
        flight_port: options.flight_port,
        http_host: options.http_host.clone(),
        http_port: options.http_port,
        username: CNOSDB_USERNAME_DEFAULT.into(),
        password: CNOSDB_PASSWORD_DEFAULT.into(),
        tenant: CNOSDB_TENANT_DEFAULT.into(),
        db: CNOSDB_DB_DEFAULT.into(),
        pwd: options.pwd.clone(),
        target_partitions: CNOSDB_TARGET_PARTITIONS_DEFAULT,
        timeout: None,
        precision: None,
        chunked: None,
    };

    let create_options = CreateOptions {
        shard_num: options.shard_count,
        replication_num: options.replica_count,
    };

    println!("{options:?}");
    println!("{db_options:?}");

    for (path, relative_path) in read_test_files(&options) {
        if !relative_path.to_string_lossy().ends_with("slt") {
            continue;
        }
        if let Some(file_stem) = path.file_stem() {
            let file_stem = file_stem.to_string_lossy();
            if file_stem.ends_with("__WINDOWS") && !cfg!(target_family = "windows") {
                continue;
            }
            if file_stem.ends_with("__UNIX") && !cfg!(target_family = "unix") {
                continue;
            }
        }
        if options.complete_mode {
            os::run_complete_file(
                options.mode.clone(),
                &path,
                relative_path,
                db_options.clone(),
                create_options.clone(),
            )
            .await?;
        } else {
            os::run_test_file(
                options.mode.clone(),
                &path,
                relative_path,
                db_options.clone(),
                create_options.clone(),
            )
            .await?;
        }
    }

    Ok(())
}

pub(crate) fn read_test_files<'a>(
    options: &'a Options,
) -> Box<dyn Iterator<Item = (PathBuf, PathBuf)> + 'a> {
    Box::new(
        read_dir_recursive(TEST_DIRECTORY)
            .map(|path| {
                (
                    path.clone(),
                    PathBuf::from(path.to_string_lossy().strip_prefix(TEST_DIRECTORY).unwrap()),
                )
            })
            .filter(|(_, relative_path)| options.check_test_file(relative_path)),
    )
}

pub(crate) fn read_dir_recursive<P: AsRef<Path>>(path: P) -> Box<dyn Iterator<Item = PathBuf>> {
    Box::new(
        std::fs::read_dir(path)
            .expect("Readable directory")
            .map(|path| path.expect("Readable entry").path())
            .flat_map(|path| {
                if path.is_dir() {
                    read_dir_recursive(path)
                } else {
                    Box::new(std::iter::once(path))
                }
            }),
    )
}

/// Parsed command line options
#[derive(Debug)]
struct Options {
    // regex like
    /// arguments passed to the program which are treated as
    /// cargo test filter (substring match on filenames)
    filters: Vec<String>,

    /// Auto complete mode to fill out expected results
    complete_mode: bool,

    /// Number of shards
    shard_count: u32,

    /// Number of replication sets
    replica_count: u32,

    mode: String,

    flight_host: String,
    flight_port: u16,
    http_host: String,
    http_port: u16,
    pwd: String,
}

#[derive(Debug, Parser)]
struct CliOptions {
    #[arg()]
    filters: Vec<String>,

    #[arg(long = "shard", default_value = "1")]
    shard_count: u32,

    #[arg(long = "replica", default_value = "1")]
    replica_count: u32,

    #[arg(long = "complete", default_value = "false")]
    complete_mode: bool,

    #[arg(short = 'M', long = "mode", default_value = "singleton")]
    mode: String,
}

impl Options {
    fn new() -> Self {
        let args = CliOptions::parse();

        let flight_host = std::env::var(CNOSDB_FLIGHT_HOST_ENV)
            .unwrap_or_else(|_| CNOSDB_FLIGHT_HOST_DEFAULT.into());
        let flight_port = std::env::var(CNOSDB_FLIGHT_PORT_ENV)
            .map_or(CNOSDB_FLIGHT_PORT_DEFAULT, |e| {
                e.parse::<u16>().expect("Parse CNOSDB_FLIGHT_PORT")
            });

        let http_host =
            std::env::var(CNOSDB_HTTP_HOST_ENV).unwrap_or_else(|_| CNOSDB_HTTP_HOST_DEFAULT.into());
        let http_port = std::env::var(CNOSDB_HTTP_PORT_ENV).map_or(CNOSDB_HTTP_PORT_DEFAULT, |e| {
            e.parse::<u16>().expect("Parse CNOSDB_HTTP_PORT")
        });

        let mut default_home = std::env::var("HOME").unwrap();
        default_home.push_str("/cnosdb");
        let pwd = std::env::var(CNOSDB_WORK_DIR_ENV).unwrap_or(default_home);
        // treat args after the first as filters to run (substring matching)

        Self {
            filters: args.filters,
            complete_mode: args.complete_mode,
            shard_count: args.shard_count,
            replica_count: args.replica_count,
            mode: args.mode,
            flight_host,
            flight_port,
            http_host,
            http_port,
            pwd,
        }
    }

    fn check_test_file(&self, relative_path: &Path) -> bool {
        if self.filters.is_empty() {
            return true;
        }

        // otherwise check if any filter matches
        self.filters
            .iter()
            .any(|filter| relative_path.to_string_lossy().contains(filter))
    }
}
