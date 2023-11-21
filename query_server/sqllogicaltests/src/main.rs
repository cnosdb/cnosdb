use std::error::Error;
use std::path::{Path, PathBuf};

use crate::instance::SqlClientOptions;

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

    let db_options = SqlClientOptions {
        flight_host: options.flight_host.clone(),
        flight_port: options.flight_port,
        http_host: options.http_host.clone(),
        http_port: options.http_port,
        username: CNOSDB_USERNAME_DEFAULT.into(),
        password: CNOSDB_PASSWORD_DEFAULT.into(),
        tenant: CNOSDB_TENANT_DEFAULT.into(),
        db: CNOSDB_DB_DEFAULT.into(),
        target_partitions: CNOSDB_TARGET_PARTITIONS_DEFAULT,
        timeout: None,
        precision: None,
        chunked: None,
    };

    println!("{options:?}");
    println!("{db_options:?}");

    for (path, relative_path) in read_test_files(&options) {
        if !relative_path.to_string_lossy().ends_with("slt") {
            continue;
        }
        if options.complete_mode {
            os::run_complete_file(&path, relative_path, db_options.clone()).await?;
        } else {
            os::run_test_file(&path, relative_path, db_options.clone()).await?;
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

    flight_host: String,
    flight_port: u16,
    http_host: String,
    http_port: u16,
}

impl Options {
    fn new() -> Self {
        let args: Vec<_> = std::env::args().collect();

        let complete_mode = args.iter().any(|a| a == "--complete");
        let flight_host =
            std::env::var(CNOSDB_FLIGHT_HOST_ENV).unwrap_or(CNOSDB_FLIGHT_HOST_DEFAULT.into());
        let flight_port = std::env::var(CNOSDB_FLIGHT_PORT_ENV)
            .map_or(CNOSDB_FLIGHT_PORT_DEFAULT, |e| {
                e.parse::<u16>().expect("Parse CNOSDB_FLIGHT_PORT")
            });

        let http_host =
            std::env::var(CNOSDB_HTTP_HOST_ENV).unwrap_or(CNOSDB_HTTP_HOST_DEFAULT.into());
        let http_port = std::env::var(CNOSDB_HTTP_PORT_ENV).map_or(CNOSDB_HTTP_PORT_DEFAULT, |e| {
            e.parse::<u16>().expect("Parse CNOSDB_HTTP_PORT")
        });

        // treat args after the first as filters to run (substring matching)
        let filters = if !args.is_empty() {
            args.into_iter()
                .skip(1)
                // ignore command line arguments like `--complete`
                .filter(|arg| !arg.as_str().starts_with("--"))
                .collect::<Vec<_>>()
        } else {
            vec![]
        };

        Self {
            filters,
            complete_mode,
            flight_host,
            flight_port,
            http_host,
            http_port,
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
