use std::env::VarError;
use std::error::Error;
use std::path::{Path, PathBuf};

use clap::{Parser, ValueEnum};

use crate::instance::CnosdbClient;

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
const CNOSDB_DATABASE_DEFAULT: &str = "public";
const CNOSDB_TARGET_PARTITIONS_DEFAULT: usize = 8;

#[tokio::main]
pub async fn main() -> Result<(), Box<dyn Error>> {
    let cli_opts = CliOptions::parse();

    let options = Options::from_cli_options(cli_opts);
    let client = CnosdbClient::new(&options);

    println!("======= Options ========\n{options:?}");
    println!("======== Client ========\n{client:?}");
    println!("========================");

    for (path, relative_path) in test_files(TEST_DIRECTORY, &options) {
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
        if options.cli.complete_cases {
            os::update_test_file(
                options.cli.cnosdb_deploy_mode.to_string(),
                &path,
                relative_path,
                client.clone(),
            )
            .await?;
        } else {
            os::run_test_file(
                options.cli.cnosdb_deploy_mode.to_string(),
                &path,
                relative_path,
                client.clone(),
            )
            .await?;
        }
    }

    Ok(())
}

pub struct TestFilesIterator<'a> {
    base_path: PathBuf,
    options: &'a Options,
    inner: Box<dyn Iterator<Item = PathBuf>>,
}

impl Iterator for TestFilesIterator<'_> {
    type Item = (PathBuf, PathBuf);

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(path) = self.inner.next() {
            let relative_path = match path.strip_prefix(&self.base_path) {
                Ok(rel_path) => rel_path.to_path_buf(),
                Err(_) => {
                    // If the path is not a valid relative path, skip it.
                    return self.next();
                }
            };
            if self.options.check_test_file(&relative_path) {
                Some((path, relative_path))
            } else {
                self.next()
            }
        } else {
            None
        }
    }
}

pub fn test_files<P: AsRef<Path>>(path: P, options: &Options) -> TestFilesIterator<'_> {
    let base_path = path.as_ref().to_path_buf();
    TestFilesIterator {
        base_path,
        options,
        inner: read_dir_recursive(path),
    }
}

pub fn read_dir_recursive<P: AsRef<Path>>(path: P) -> Box<dyn Iterator<Item = PathBuf>> {
    let path = path.as_ref();
    let mut dir_entries: Vec<PathBuf> = match std::fs::read_dir(path) {
        Ok(read_dir) => read_dir.into_iter().flatten().map(|e| e.path()).collect(),
        Err(e) => panic!("Readable directory '{}' failed: {e}", path.display()),
    };
    // Sort paths by file_name asc, files first, then directories.
    dir_entries.sort_by(|a, b| match (a.is_dir(), b.is_dir()) {
        (true, true) => a.cmp(b),
        (true, false) => std::cmp::Ordering::Greater,
        (false, true) => std::cmp::Ordering::Less,
        (false, false) => a.cmp(b),
    });
    Box::new(dir_entries.into_iter().flat_map(|path| {
        if path.is_dir() {
            read_dir_recursive(path)
        } else {
            Box::new(std::iter::once(path))
        }
    }))
}

fn get_env_var(key: &str) -> Option<String> {
    match std::env::var(key) {
        Ok(v) => Some(v),
        Err(e) => match e {
            VarError::NotPresent => None,
            VarError::NotUnicode(os_str) => {
                panic!(
                    "Environment variable '{key}' is not unicode ('{}')",
                    os_str.to_string_lossy()
                );
            }
        },
    }
}

fn get_env_var_or(key: &str, default_val: &str) -> String {
    get_env_var(key).unwrap_or_else(|| default_val.to_string())
}

fn get_env_var_t_or<T>(key: &str, default_val: T) -> T
where
    T: Copy + std::fmt::Display + std::str::FromStr,
    <T as std::str::FromStr>::Err: std::fmt::Display,
{
    match get_env_var(key) {
        Some(v_str) => match v_str.parse::<T>() {
            Ok(v) => v,
            Err(e) => {
                panic!("Environment variable '{key}'='{v_str}' is not a valid ({e})");
            }
        },
        None => default_val,
    }
}

#[derive(Debug, Clone, Parser)]
#[command(version = version::workspace_version(), )]
struct CliOptions {
    /// Filters of test files to run.
    /// e.g. 'test_database.slt' '.*test_database.slt' '.*test_database.*'
    #[arg()]
    filters: Vec<String>,

    /// Add external option when executing CREATE_DATABASE statement: shard <number>
    #[arg(long = "shard", default_value = "1")]
    create_database_with_shard: u32,

    /// Add external option when executing CREATE_DATABASE statement: replica <number>
    #[arg(long = "replica", default_value = "1")]
    create_database_with_replica: u32,

    /// Complete case files to fill out expected results with the output produced by database.
    #[arg(long = "complete", default_value = "false")]
    complete_cases: bool,

    /// The mode of the cnosdb deployment: "singleton" or "cluster"
    #[arg(short = 'M', long = "mode", default_value = "singleton")]
    cnosdb_deploy_mode: CnosdbDeployMode,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, ValueEnum)]
enum CnosdbDeployMode {
    Singleton,
    Cluster,
}

impl std::fmt::Display for CnosdbDeployMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CnosdbDeployMode::Singleton => write!(f, "singleton"),
            CnosdbDeployMode::Cluster => write!(f, "cluster"),
        }
    }
}

/// Parsed command line options
#[derive(Debug, Clone)]
pub struct Options {
    cli: CliOptions,

    /// Rewritten regex filters.
    filters_regex: Vec<regex::Regex>,
    /// Rewritten filters that use 'String::contain()' to check.
    filters_string: Vec<String>,

    flight_host: String,
    flight_port: u16,
    http_host: String,
    http_port: u16,
    work_directory: PathBuf,
}

impl Options {
    fn from_cli_options(cli_opts: CliOptions) -> Self {
        let mut filters_regex = Vec::new();
        let mut filters_string = Vec::new();
        for ex in cli_opts.filters.iter() {
            match regex::Regex::new(ex) {
                Ok(regex) => filters_regex.push(regex),
                Err(_) => {
                    filters_string.push(ex.clone());
                }
            };
        }

        let flight_host = get_env_var_or(CNOSDB_FLIGHT_HOST_ENV, CNOSDB_FLIGHT_HOST_DEFAULT);
        let flight_port = get_env_var_t_or(CNOSDB_FLIGHT_PORT_ENV, CNOSDB_FLIGHT_PORT_DEFAULT);
        let http_host = get_env_var_or(CNOSDB_HTTP_HOST_ENV, CNOSDB_HTTP_HOST_DEFAULT);
        let http_port = get_env_var_t_or(CNOSDB_HTTP_PORT_ENV, CNOSDB_HTTP_PORT_DEFAULT);
        let work_directory = get_env_var(CNOSDB_WORK_DIR_ENV)
            .map(PathBuf::from)
            .unwrap_or_else(|| match std::env::current_dir() {
                Ok(path) => path,
                Err(e) => {
                    panic!("Failed to get current directory: {e}");
                }
            });

        Self {
            cli: cli_opts,

            filters_regex,
            filters_string,

            flight_host,
            flight_port,
            http_host,
            http_port,
            work_directory,
        }
    }

    fn check_test_file(&self, relative_path: &Path) -> bool {
        if self.cli.filters.is_empty() {
            return true;
        }

        let path = relative_path.to_string_lossy();
        self.filters_regex
            .iter()
            .any(|ex| ex.is_match(path.as_ref()))
            || self.filters_string.iter().any(|ex| path.contains(ex))
    }
}

#[cfg(test)]
mod test {
    use std::path::PathBuf;

    use crate::{
        read_dir_recursive, test_files, CliOptions, CnosdbDeployMode, Options,
        CNOSDB_FLIGHT_HOST_DEFAULT, CNOSDB_FLIGHT_PORT_DEFAULT, CNOSDB_HTTP_HOST_DEFAULT,
        CNOSDB_HTTP_PORT_DEFAULT,
    };

    #[test]
    fn test_read_dir_recursive() {
        let dir = PathBuf::from("/tmp/test/sqllogicaltests/test_read_dir_recursive");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let file_1 = dir.join("1.txt"); // 1.txt
        let file_2 = dir.join("2.txt"); // 1.txt
        let subdir_a = dir.join("a"); // a/
        let file_a_1 = subdir_a.join("a_1.txt"); // a/a_1.txt
        let file_a_2 = subdir_a.join("a_2.txt"); // a/a_2.txt
        let subdir_a_a = subdir_a.join("a_a"); // a/a_a/
        let file_a_a_1 = subdir_a_a.join("a_a_1.txt"); // a/a_a/a_a_1.txt
        let subdir_b = dir.join("b"); // b/
        let subdir_b_a = subdir_b.join("b_a"); // b/b_a/
        let file_b_a_1 = subdir_b_a.join("b_a_1.txt"); // b/b_a/b_a_1.txt
        let file_b_a_2 = subdir_b_a.join("b_a_2.txt"); // b/b_a/b_a_2.txt
        let subdir_b_b = subdir_b.join("b_b"); // b/b_b/
        let file_b_b_1 = subdir_b_b.join("b_b_1.txt"); // b/b_b/b_b_1.txt

        std::fs::create_dir_all(&subdir_a).unwrap();
        std::fs::create_dir_all(&subdir_a_a).unwrap();
        std::fs::create_dir_all(&subdir_b).unwrap();
        std::fs::create_dir_all(&subdir_b_a).unwrap();
        std::fs::create_dir_all(&subdir_b_b).unwrap();
        std::fs::write(&file_a_2, "a_2").unwrap();
        std::fs::write(&file_a_1, "a_1").unwrap();
        std::fs::write(&file_2, "2").unwrap();
        std::fs::write(&file_1, "1").unwrap();
        std::fs::write(&file_b_b_1, "b_b_1").unwrap();
        std::fs::write(&file_b_a_2, "b_a_2").unwrap();
        std::fs::write(&file_b_a_1, "b_a_1").unwrap();
        std::fs::write(&file_a_a_1, "a_a_1").unwrap();

        let mut iter = read_dir_recursive(&dir);
        assert_eq!(iter.next(), Some(file_1.clone()));
        assert_eq!(iter.next(), Some(file_2.clone()));
        assert_eq!(iter.next(), Some(file_a_1.clone()));
        assert_eq!(iter.next(), Some(file_a_2.clone()));
        assert_eq!(iter.next(), Some(file_a_a_1.clone()));
        assert_eq!(iter.next(), Some(file_b_a_1.clone()));
        assert_eq!(iter.next(), Some(file_b_a_2.clone()));
        assert_eq!(iter.next(), Some(file_b_b_1.clone()));
        assert_eq!(iter.next(), None);

        let options = Options {
            cli: CliOptions {
                filters: vec![],
                create_database_with_shard: 1,
                create_database_with_replica: 1,
                complete_cases: false,
                cnosdb_deploy_mode: CnosdbDeployMode::Singleton,
            },
            filters_regex: vec![],
            filters_string: vec![],
            flight_host: CNOSDB_FLIGHT_HOST_DEFAULT.to_string(),
            flight_port: CNOSDB_FLIGHT_PORT_DEFAULT,
            http_host: CNOSDB_HTTP_HOST_DEFAULT.to_string(),
            http_port: CNOSDB_HTTP_PORT_DEFAULT,
            work_directory: dir.clone(),
        };
        let mut iter = test_files(&dir, &options);
        #[rustfmt::skip]
        {
            assert_eq!(iter.next(), Some((file_1.clone(), file_1.strip_prefix(&dir).unwrap().to_path_buf())));
            assert_eq!(iter.next(), Some((file_2.clone(), file_2.strip_prefix(&dir).unwrap().to_path_buf())));
            assert_eq!(iter.next(), Some((file_a_1.clone(), file_a_1.strip_prefix(&dir).unwrap().to_path_buf())));
            assert_eq!(iter.next(), Some((file_a_2.clone(), file_a_2.strip_prefix(&dir).unwrap().to_path_buf())));
            assert_eq!(iter.next(), Some((file_a_a_1.clone(), file_a_a_1.strip_prefix(&dir).unwrap().to_path_buf())));
            assert_eq!(iter.next(), Some((file_b_a_1.clone(), file_b_a_1.strip_prefix(&dir).unwrap().to_path_buf())));
            assert_eq!(iter.next(), Some((file_b_a_2.clone(), file_b_a_2.strip_prefix(&dir).unwrap().to_path_buf())));
            assert_eq!(iter.next(), Some((file_b_b_1.clone(), file_b_b_1.strip_prefix(&dir).unwrap().to_path_buf())));
            assert_eq!(iter.next(), None);
        }; // this semicolon is required to use #[rustfmt::skip].
    }
}
