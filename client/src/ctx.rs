use std::io::BufRead;
use std::path::Path;

use anyhow::anyhow;
use base64::prelude::{Engine, BASE64_STANDARD};
use bytes::Bytes;
use datafusion::arrow::record_batch::RecordBatch;
use http_protocol::encoding::Encoding;
use http_protocol::header::{ACCEPT, PRIVATE_KEY};
use http_protocol::http_client::HttpClient;
use http_protocol::parameter::{DumpParam, SqlParam, WriteParam};
use http_protocol::status_code::OK;
use reqwest::header::{HeaderMap, ACCEPT_ENCODING, CONTENT_ENCODING};
use reqwest::Response;
use tokio::sync::mpsc;

use crate::config::ConfigOptions;
use crate::print_format::PrintFormat;
use crate::{progress_bar, ExitCode, Result};

pub const DEFAULT_USER: &str = "cnosdb";
pub const DEFAULT_PASSWORD: &str = "";
pub const DEFAULT_DATABASE: &str = "public";
pub const DEFAULT_PRECISION: &str = "NS";
pub const DEFAULT_USE_SSL: bool = false;
pub const DEFAULT_USE_UNSAFE_SSL: bool = false;
pub const DEFAULT_CHUNKED: bool = false;
pub const DEFAULT_PROCESS_CLI_COMMAND: bool = false;
pub const DEFAULT_ERROR_STOP: bool = false;

pub const API_V1_SQL_PATH: &str = "/api/v1/sql";
pub const API_V1_WRITE_PATH: &str = "/api/v1/write";
pub const API_V1_DUMP_SQL_DDL_PATH: &str = "/api/v1/dump/sql/ddl";

pub struct SessionConfig {
    pub user_info: UserInfo,
    pub connection_info: ConnectionInfo,
    pub tenant: String,
    pub database: String,
    pub precision: String,
    pub target_partitions: Option<usize>,
    pub stream_trigger_interval: Option<String>,
    pub accept_encoding: Option<Encoding>,
    pub content_encoding: Option<Encoding>,
    pub fmt: PrintFormat,
    pub config_options: ConfigOptions,
    pub use_ssl: bool,
    pub use_unsafe_ssl: bool,
    pub chunked: bool,
    pub process_cli_command: bool,
    pub error_stop: bool,
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
            accept_encoding: None,
            content_encoding: None,
            config_options,
            fmt: PrintFormat::Csv,
            use_ssl: DEFAULT_USE_SSL,
            use_unsafe_ssl: DEFAULT_USE_UNSAFE_SSL,
            chunked: DEFAULT_CHUNKED,
            process_cli_command: DEFAULT_PROCESS_CLI_COMMAND,
            error_stop: DEFAULT_ERROR_STOP,
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

    pub fn with_accept_encoding(mut self, accept_encoding: Option<Encoding>) -> Self {
        self.accept_encoding = accept_encoding;
        self
    }

    pub fn with_content_encoding(mut self, content_encoding: Option<Encoding>) -> Self {
        self.content_encoding = content_encoding;
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

    pub fn with_proxy_url(mut self, proxy_url: Option<String>) -> Self {
        self.connection_info.proxy_url = proxy_url;

        self
    }

    pub fn with_proxy_basic_auth(mut self, proxy_basic_auth: Option<(String, String)>) -> Self {
        self.connection_info.proxy_basic_auth = proxy_basic_auth;

        self
    }

    pub fn with_proxy_custom_auth(mut self, proxy_custom_auth: Option<String>) -> Self {
        self.connection_info.proxy_custom_auth = proxy_custom_auth;

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

    pub fn with_chunked(mut self, chunked: bool) -> Self {
        self.chunked = chunked;

        self
    }

    pub fn with_process_cli_command(mut self, process_cli_command: bool) -> Self {
        self.process_cli_command = process_cli_command;

        self
    }

    pub fn with_error_stop(mut self, error_stop: bool) -> Self {
        self.error_stop = error_stop;

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

    pub proxy_url: Option<String>,
    pub proxy_basic_auth: Option<(String, String)>,
    pub proxy_custom_auth: Option<String>,
}

pub struct SessionContext {
    session_config: SessionConfig,

    http_client: HttpClient,
}

impl SessionContext {
    pub fn new(session_config: SessionConfig) -> Self {
        let c = &session_config.connection_info;
        let http_client = HttpClient::with_proxy_settings(
            &c.host,
            c.port,
            session_config.use_ssl,
            session_config.use_unsafe_ssl,
            &c.ca_cert_files,
            &c.proxy_url,
            &c.proxy_basic_auth,
            &c.proxy_custom_auth,
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

    pub fn set_tenant(&mut self, tenant: String) {
        self.session_config.tenant = tenant
    }

    pub fn get_database(&self) -> &str {
        self.session_config.database.as_str()
    }

    pub fn get_session_config(&self) -> &SessionConfig {
        &self.session_config
    }

    pub fn get_mut_session_config(&mut self) -> &mut SessionConfig {
        &mut self.session_config
    }

    pub async fn sql(&self, sql: String) -> Result<Response> {
        let mut sql = sql.into_bytes();
        let user_info = &self.session_config.user_info;

        let tenant = self.session_config.tenant.clone();
        let db = self.session_config.database.clone();
        let target_partitions = self.session_config.target_partitions;
        let stream_trigger_interval = self.session_config.stream_trigger_interval.clone();
        let chunked = self.session_config.chunked;
        let param = SqlParam {
            tenant: Some(tenant),
            db: Some(db),
            chunked: Some(chunked),
            target_partitions,
            stream_trigger_interval,
        };

        // let param = &[("db", &self.session_config.database)];
        let mut builder = self
            .http_client
            .post(API_V1_SQL_PATH)
            .basic_auth::<&str, &str>(&user_info.user, user_info.password.as_deref())
            .header(ACCEPT, self.session_config.fmt.get_http_content_type());

        if let Some(encoding) = self.session_config.accept_encoding {
            builder = builder.header(ACCEPT_ENCODING, encoding.to_header_value());
        }

        if let Some(encoding) = self.session_config.content_encoding {
            builder = builder.header(CONTENT_ENCODING, encoding.to_header_value());
            sql = encoding.encode(sql)?;
        }

        builder = if let Some(key) = &user_info.private_key {
            let key = BASE64_STANDARD.encode(key);
            builder.header(PRIVATE_KEY, key)
        } else {
            builder
        };

        let resp = builder.query(&param).body(sql).send().await?;
        if resp.status().is_client_error() || resp.status().is_server_error() {
            let error_msg = format!("{}, details: {}", resp.status(), resp.text().await?);
            return Err(anyhow::Error::msg(error_msg));
        }
        Ok(resp)
    }

    pub async fn parse_response(resp: Response) -> Result<ResultSet> {
        match resp.status() {
            OK => {
                let header = resp.headers().clone();
                let bytes = resp.bytes().await?;
                Self::decode_body(bytes, &header).await
            }
            _ => {
                let status = resp.status().to_string();
                let body = resp.text().await?;

                Err(anyhow!("{status}, details: {body}"))
            }
        }
    }

    pub async fn decode_body(body: Bytes, header: &HeaderMap) -> Result<ResultSet> {
        let body = if let Some(content_encoding) = header.get(CONTENT_ENCODING) {
            let encoding_str = content_encoding.to_str()?;
            let encoding = match Encoding::from_str_opt(encoding_str) {
                Some(encoding) => Ok(encoding),
                None => Err(anyhow!("encoding not support: {}", encoding_str)),
            }?;
            encoding.decode(body)?
        } else {
            body
        }
        .to_vec();

        Ok(ResultSet::Bytes((body, 0)))
    }

    pub async fn write_line_protocol_file(&self, path: impl AsRef<Path>) -> Result<ResultSet> {
        let file = std::fs::File::open(path)?;
        let size = file.metadata()?.len();
        let mut reader = std::io::BufReader::new(file);
        let pb = progress_bar::new_with_size(size);

        let (tx, mut rx) = mpsc::channel::<String>(10);

        let producer = tokio::spawn(async move {
            let mut buffer = String::new();
            let mut line_count = 0;
            while let Ok(size) = reader.read_line(&mut buffer) {
                if size == 0 {
                    break;
                }
                line_count += 1;
                if line_count % 10000 == 0 && !buffer.is_empty() {
                    tx.send(buffer.clone()).await.unwrap();
                    buffer.clear();
                }
            }

            if !buffer.is_empty() {
                tx.send(buffer).await.unwrap();
            }
            anyhow::Ok(())
        });

        let consumer = async move {
            while let Some(data) = rx.recv().await {
                let data = data.into_bytes();
                pb.inc(data.len() as u64);
                self.write_line_protocol(data).await?;
            }
            anyhow::Ok(())
        };

        let (producer_results, consumer_results) = tokio::join!(producer, consumer);
        let _ = producer_results?;
        consumer_results?;

        Ok(ResultSet::Bytes(("".into(), 0)))
    }

    pub async fn write_line_protocol(&self, mut body: Vec<u8>) -> Result<ResultSet> {
        let user_info = &self.session_config.user_info;

        let tenant = self.session_config.tenant.clone();
        let db = self.session_config.database.clone();
        let precision = self.session_config.precision.clone();

        let param = WriteParam {
            precision: Some(precision),
            tenant: Some(tenant),
            db: Some(db),
        };

        let mut builder = self
            .http_client
            .post(API_V1_WRITE_PATH)
            .basic_auth::<&str, &str>(&user_info.user, user_info.password.as_deref())
            .query(&param);

        if let Some(encoding) = self.session_config.content_encoding {
            builder = builder.header(CONTENT_ENCODING, encoding.to_header_value());
            body = encoding.encode(body)?;
        }

        let resp = builder.body(body).send().await?;

        match resp.status() {
            OK => {
                let body = resp.bytes().await?;

                Ok(ResultSet::Bytes((body.to_vec(), 0)))
            }
            code => {
                let body = resp.text().await?;
                Err(anyhow!("{}, body: {}", code, body))
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

    pub async fn dump(&self, tenants: Vec<Option<String>>) -> Result<String> {
        let user_info = &self.session_config.user_info;

        let mut res = String::new();
        let tenants = if tenants.is_empty() {
            vec![None]
        } else {
            tenants
        };

        for tenant in tenants {
            let param = DumpParam { tenant };
            let mut builder = self
                .http_client
                .get(API_V1_DUMP_SQL_DDL_PATH)
                .basic_auth::<&str, &str>(&user_info.user, user_info.password.as_deref());
            builder = if let Some(key) = &user_info.private_key {
                let key = BASE64_STANDARD.encode(key);
                builder.header(PRIVATE_KEY, key)
            } else {
                builder
            }
            .query(&param);

            let resp = builder.send().await?;

            res += &resp.text().await?;
        }
        Ok(res)
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
