use datafusion::arrow::record_batch::RecordBatch;
use http_protocol::header::ACCEPT;
use http_protocol::http_client::HttpClient;
use http_protocol::parameter::{SqlParam, WriteParam};
use http_protocol::status_code::OK;

use crate::config::ConfigOptions;
use crate::print_format::PrintFormat;

pub const DEFAULT_USER: &str = "cnosdb";
pub const DEFAULT_PASSWORD: &str = "";
pub const DEFAULT_DATABASE: &str = "public";

pub const API_V1_SQL_PATH: &str = "/api/v1/sql";
pub const API_V1_WRITE_PATH: &str = "/api/v1/write";

pub struct SessionConfig {
    pub user_info: UserInfo,
    pub connection_info: ConnectionInfo,
    pub tenant: String,
    pub database: String,
    pub target_partitions: Option<usize>,
    pub fmt: PrintFormat,
    pub config_options: ConfigOptions,
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
            target_partitions: None,
            config_options,
            fmt: PrintFormat::Csv,
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

    pub fn with_host(mut self, host: String) -> Self {
        self.connection_info.host = host;

        self
    }

    pub fn with_port(mut self, port: usize) -> Self {
        self.connection_info.port = port;

        self
    }

    pub fn with_tls(mut self, tls: TLSConfig) -> Self {
        self.connection_info.tls_config = Some(tls);

        self
    }

    pub fn with_result_format(mut self, fmt: PrintFormat) -> Self {
        self.fmt = fmt;

        self
    }
}

pub struct UserInfo {
    pub user: String,
    pub password: Option<String>,
}

impl Default for UserInfo {
    fn default() -> Self {
        Self {
            user: DEFAULT_USER.to_string(),
            password: None,
        }
    }
}

#[derive(Default)]
pub struct ConnectionInfo {
    pub host: String,
    pub port: usize,

    pub tls_config: Option<TLSConfig>,
}

pub struct TLSConfig {
    pub client_cert_file: String,
    pub client_key_file: String,
}

pub struct SessionContext {
    session_config: SessionConfig,

    http_client: HttpClient,
}

impl SessionContext {
    pub fn new(session_config: SessionConfig) -> Self {
        let c = &session_config.connection_info;
        let http_client = HttpClient::from_addr(c.host.clone(), c.port);

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

    pub async fn sql(&self, sql: String) -> Result<ResultSet, String> {
        let user_info = &self.session_config.user_info;

        let tenant = self.session_config.tenant.clone();
        let db = self.session_config.database.clone();
        let target_partitions = self.session_config.target_partitions;
        let param = SqlParam {
            tenant: Some(tenant),
            db: Some(db),
            chunked: None,
            target_partitions,
        };

        // let param = &[("db", &self.session_config.database)];

        let resp = self
            .http_client
            .post(API_V1_SQL_PATH)
            .basic_auth::<&str, &str>(&user_info.user, user_info.password.as_deref())
            .header(ACCEPT, self.session_config.fmt.get_http_content_type())
            .query(&param)
            .body(sql)
            .send()
            .await
            .map_err(|e| e.to_string())?;

        match resp.status() {
            OK => {
                let body = resp.bytes().await.map_err(|e| format!("{}", e))?;

                Ok(ResultSet::Bytes((body.to_vec(), 0)))
            }
            _ => {
                let status = resp.status().to_string();
                let body = resp.text().await.map_err(|e| format!("{}", e))?;

                Err(format!("{}, details: {}", status, body))
            }
        }
    }

    pub async fn write(&self, path: &str) -> Result<ResultSet, String> {
        let body = tokio::fs::read(path).await.map_err(|e| e.to_string())?;

        let user_info = &self.session_config.user_info;

        let param = WriteParam {
            tenant: None,
            db: self.session_config.database.clone(),
        };

        // let param = &[("db", &self.session_config.database)];

        let resp = self
            .http_client
            .post(API_V1_WRITE_PATH)
            .basic_auth::<&str, &str>(&user_info.user, user_info.password.as_deref())
            .query(&param)
            .body(body)
            .send()
            .await
            .map_err(|e| e.to_string())?;

        match resp.status() {
            OK => {
                let body = resp.bytes().await.map_err(|e| format!("{}", e))?;

                Ok(ResultSet::Bytes((body.to_vec(), 0)))
            }
            _ => {
                let body = resp.text().await.map_err(|e| format!("{}", e))?;

                Err(body)
            }
        }
    }
}

pub enum ResultSet {
    RecordBatches(Vec<RecordBatch>),
    // (data, row_number)
    Bytes((Vec<u8>, usize)),
}

impl ResultSet {
    pub fn print_fmt(&self, print: &PrintFormat) -> Result<(), String> {
        match self {
            Self::RecordBatches(batches) => {
                print.print_batches(batches).map_err(|e| e.to_string())?;
            }
            Self::Bytes((r, _)) => {
                let str = String::from_utf8(r.to_owned()).map_err(|e| e.to_string())?;
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
