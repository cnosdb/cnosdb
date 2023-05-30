use std::path::Path;

use anyhow::anyhow;
use datafusion::arrow::record_batch::RecordBatch;
use http_protocol::header::{ACCEPT, PRIVATE_KEY};
use http_protocol::http_client::HttpClient;
use http_protocol::parameter::{SqlParam, WriteParam};
use http_protocol::status_code::OK;

use crate::config::ConfigOptions;
use crate::print_format::PrintFormat;
use crate::{ExitCode, Result};

pub const DEFAULT_USER: &str = "cnosdb";
pub const DEFAULT_PASSWORD: &str = "";
pub const DEFAULT_DATABASE: &str = "public";
pub const DEFAULT_PRECISION: &str = "NS";
pub const DEFAULT_USE_SSL: bool = false;
pub const DEFAULT_USE_UNSAFE_SSL: bool = false;

pub const API_V1_SQL_PATH: &str = "/api/v1/sql";
pub const API_V1_WRITE_PATH: &str = "/api/v1/write";

pub struct SessionConfig {
    pub user_info: UserInfo,
    pub connection_info: ConnectionInfo,
    pub tenant: String,
    pub database: String,
    pub precision: String,
    pub target_partitions: Option<usize>,
    pub stream_trigger_interval: Option<String>,
    pub fmt: PrintFormat,
    pub config_options: ConfigOptions,
    pub use_ssl: bool,
    pub use_unsafe_ssl: bool,
}

impl SessionConfig {
    /// Create an execution config with config options read from the environment
    pub fn from_env() -> Self {
        let config_options = ConfigOptions::from_env();

        Self {
            user_info: Default::default(),
            connection_info: Default::default(),
            tenant: DEFAULT_USER.to_string(),
            database: DEFAULT_DATABASE.to_string(),
            precision: DEFAULT_PRECISION.to_string(),
            target_partitions: None,
            stream_trigger_interval: None,
            config_options,
            fmt: PrintFormat::Csv,
            use_ssl: DEFAULT_USE_SSL,
            use_unsafe_ssl: DEFAULT_USE_UNSAFE_SSL,
        }
    }

    pub fn mut_config_options(&mut self) -> &mut ConfigOptions {
        &mut self.config_options
    }

    pub fn with_user(mut self, user: String) -> Self {
        self.user_info.user = user;
        self
    }

    pub fn with_password(mut self, password: Option<String>) -> Self {
        self.user_info.password = password;
        self
    }

    pub fn with_private_key(mut self, private_key: Option<String>) -> Self {
        self.user_info.private_key = private_key;
        self
    }

    pub fn with_tenant(mut self, tenant: String) -> Self {
        self.tenant = tenant;
        self
    }

    pub fn with_database(mut self, database: String) -> Self {
        self.database = database;
        self
    }

    pub fn with_target_partitions(mut self, target_partitions: Option<usize>) -> Self {
        self.target_partitions = target_partitions;
        self
    }

    pub fn with_stream_trigger_interval(mut self, stream_trigger_interval: Option<String>) -> Self {
        self.stream_trigger_interval = stream_trigger_interval;
        self
    }

    pub fn with_host(mut self, host: String) -> Self {
        self.connection_info.host = host;

        self
    }

    pub fn with_port(mut self, port: u16) -> Self {
        self.connection_info.port = port;

        self
    }

    pub fn with_ca_certs(mut self, ca_cert_files: Vec<String>) -> Self {
        self.connection_info.ca_cert_files = ca_cert_files;

        self
    }

    pub fn with_result_format(mut self, fmt: PrintFormat) -> Self {
        self.fmt = fmt;

        self
    }

    pub fn with_precision(mut self, precision: Option<String>) -> Self {
        if let Some(precision) = precision {
            self.precision = precision;
        }

        self
    }

    pub fn with_ssl(mut self, use_ssl: bool) -> Self {
        self.use_ssl = use_ssl;

        self
    }

    pub fn with_unsafe_ssl(mut self, use_unsafe_ssl: bool) -> Self {
        self.use_unsafe_ssl = use_unsafe_ssl;

        self
    }
}

pub struct UserInfo {
    pub user: String,
    pub password: Option<String>,
    pub private_key: Option<String>,
}

impl Default for UserInfo {
    fn default() -> Self {
        Self {
            user: DEFAULT_USER.to_string(),
            password: None,
            private_key: None,
        }
    }
}

#[derive(Default)]
pub struct ConnectionInfo {
    pub host: String,
    pub port: u16,

    pub ca_cert_files: Vec<String>,
}

pub struct SessionContext {
    session_config: SessionConfig,

    http_client: HttpClient,
}

impl SessionContext {
    pub fn new(session_config: SessionConfig) -> Self {
        let c = &session_config.connection_info;
        let http_client = HttpClient::new(
            &c.host,
            c.port,
            session_config.use_ssl,
            session_config.use_unsafe_ssl,
            &c.ca_cert_files,
        )
        .unwrap_or_else(|e| {
            eprintln!("ERROR: Failed to build http client: {}", e);
            std::process::exit(ExitCode::HttpClientInitFailed as i32);
        });

        Self {
            session_config,
            http_client,
        }
    }

    pub fn set_database(&mut self, name: &str) {
        self.session_config.database = name.to_string();
    }

    pub fn get_database(&self) -> &str {
        self.session_config.database.as_str()
    }

    pub async fn sql(&self, sql: String) -> Result<ResultSet> {
        let user_info = &self.session_config.user_info;

        let tenant = self.session_config.tenant.clone();
        let db = self.session_config.database.clone();
        let target_partitions = self.session_config.target_partitions;
        let stream_trigger_interval = self.session_config.stream_trigger_interval.clone();
        let param = SqlParam {
            tenant: Some(tenant),
            db: Some(db),
            chunked: None,
            target_partitions,
            stream_trigger_interval,
        };

        // let param = &[("db", &self.session_config.database)];
        let mut builder = self
            .http_client
            .post(API_V1_SQL_PATH)
            .basic_auth::<&str, &str>(&user_info.user, user_info.password.as_deref())
            .header(ACCEPT, self.session_config.fmt.get_http_content_type());

        builder = if let Some(key) = &user_info.private_key {
            let key = base64::encode(key);
            builder.header(PRIVATE_KEY, key)
        } else {
            builder
        };

        let resp = builder.query(&param).body(sql).send().await?;

        match resp.status() {
            OK => {
                let body = resp.bytes().await?;

                Ok(ResultSet::Bytes((body.to_vec(), 0)))
            }
            _ => {
                let status = resp.status().to_string();
                let body = resp.text().await?;

                Err(anyhow!("{status}, details: {body}"))
            }
        }
    }

    pub async fn write_line_protocol_file(&self, path: impl AsRef<Path>) -> Result<ResultSet> {
        let body = tokio::fs::read(path).await?;

        let user_info = &self.session_config.user_info;

        let tenant = self.session_config.tenant.clone();
        let db = self.session_config.database.clone();
        let precision = self.session_config.precision.clone();

        let param = WriteParam {
            precision: Some(precision),
            tenant: Some(tenant),
            db: Some(db),
        };

        let resp = self
            .http_client
            .post(API_V1_WRITE_PATH)
            .basic_auth::<&str, &str>(&user_info.user, user_info.password.as_deref())
            .query(&param)
            .body(body)
            .send()
            .await?;

        match resp.status() {
            OK => {
                let body = resp.bytes().await?;

                Ok(ResultSet::Bytes((body.to_vec(), 0)))
            }
            _ => {
                let body = resp.text().await?;

                Ok(ResultSet::Bytes((body.into(), 0)))
            }
        }
    }

    pub async fn write(&self, path: impl AsRef<Path>) -> Result<()> {
        if path.as_ref().is_file() {
            self.write_line_protocol_file(path).await?;
            return Ok(());
        }
        for dir_entry in walkdir::WalkDir::new(path)
            .sort_by_file_name()
            .into_iter()
            .collect::<std::result::Result<Vec<_>, _>>()?
            .into_iter()
            .filter(|d| d.path().is_file())
        {
            self.write_line_protocol_file(dir_entry.path()).await?;
        }
        Ok(())
    }
}

pub enum ResultSet {
    RecordBatches(Vec<RecordBatch>),
    // (data, row_number)
    Bytes((Vec<u8>, usize)),
}

impl ResultSet {
    pub fn print_fmt(&self, print: &PrintFormat) -> Result<()> {
        match self {
            Self::RecordBatches(batches) => {
                print.print_batches(batches)?;
            }
            Self::Bytes((r, _)) => {
                let str = String::from_utf8(r.to_owned())?;
                if !str.is_empty() {
                    println!("{}", str);
                }
            }
        }

        Ok(())
    }

    pub fn row_count(&self) -> usize {
        match self {
            Self::RecordBatches(batches) => batches.iter().map(|b| b.num_rows()).sum(),
            Self::Bytes((_, row_count)) => *row_count,
        }
    }
}
