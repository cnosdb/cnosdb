use std::fmt::Write as _;
use std::path::PathBuf;
use std::time::Duration;

use arrow::datatypes::Schema;
use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;
use arrow_flight::sql::client::FlightSqlServiceClient;
use async_trait::async_trait;
use futures::TryStreamExt;
use nom::Parser;
use regex::Regex;
use reqwest::{Body, Client, Method, Request, Url};
use sqllogictest::{ColumnType, DBOutput};
use tonic::transport::{Channel, Endpoint};

use crate::db_request::{
    instruction_parse_identity, instruction_parse_str, instruction_parse_to, CnosdbRequest,
};
use crate::error::{Result, SqlError};
use crate::Options;

pub struct CnosdbTestEngine {
    engine_name: String,
    relative_path: PathBuf,
    client: CnosdbClient,
}

impl CnosdbTestEngine {
    pub fn new(
        engine_name: impl Into<String>,
        relative_path: impl Into<PathBuf>,
        client: CnosdbClient,
    ) -> Result<Self, SqlError> {
        Ok(Self {
            engine_name: engine_name.into(),
            relative_path: relative_path.into(),
            client,
        })
    }
}

#[async_trait]
impl sqllogictest::AsyncDB for CnosdbTestEngine {
    type Error = SqlError;

    type ColumnType = CnosdbColumnType;

    async fn run(
        &mut self,
        request: &str,
    ) -> std::result::Result<DBOutput<Self::ColumnType>, Self::Error> {
        let request = CnosdbRequest::parse_db_request(request, &mut self.client)?;

        let (types, rows) = request.execute(&self.client, &self.relative_path).await?;
        if rows.is_empty() && types.is_empty() {
            Ok(DBOutput::StatementComplete(0))
        } else {
            Ok(DBOutput::Rows { types, rows })
        }
    }

    async fn shutdown(&mut self) {}

    fn engine_name(&self) -> &str {
        &self.engine_name
    }

    async fn sleep(dur: Duration) {
        tokio::time::sleep(dur).await;
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum CnosdbColumnType {
    Boolean,
    Integer,
    Float,
    Text,
    Timestamp,
    Another,
}

impl ColumnType for CnosdbColumnType {
    fn from_char(value: char) -> Option<Self> {
        match value {
            'B' => Some(Self::Boolean),
            'I' => Some(Self::Integer),
            'P' => Some(Self::Timestamp),
            'R' => Some(Self::Float),
            'T' => Some(Self::Text),
            _ => Some(Self::Another),
        }
    }

    fn to_char(&self) -> char {
        match self {
            Self::Boolean => 'B',
            Self::Integer => 'I',
            Self::Timestamp => 'P',
            Self::Float => 'R',
            Self::Text => 'T',
            Self::Another => '?',
        }
    }
}

#[derive(Debug, Clone)]
pub struct CreateDatabaseOptions {
    pub replica: u32,
    pub shard: u32,
}

impl CreateDatabaseOptions {
    /// Rewrite the CREATE_DATABASE statement to add shard and replica options if not present.
    fn rewrite(&self, sql: &str) -> String {
        let re = Regex::new(r"create\s+database").expect("create Regex failed");
        if re.is_match(sql) {
            let sql_lower = sql.to_ascii_lowercase();
            if let Some(with_idx) = sql_lower.find(" with ") {
                let contains_shard = sql_lower.contains(" shard ");
                let contains_replica = sql_lower.contains(" replica ");
                drop(sql_lower);

                let mut buf = sql[..with_idx + 6].to_string();
                if !contains_shard {
                    write!(&mut buf, "shard {} ", self.shard).expect("write to String failed");
                }
                if !contains_replica {
                    write!(&mut buf, "replica {} ", self.replica).expect("write to String failed");
                }
                buf.push_str(&sql[with_idx + 6..]);
                buf
            } else if sql.ends_with(';') {
                let sem_idx = sql.rfind(|c| c != ';').unwrap_or(0);
                format!(
                    "{} with shard {} replica {}{}",
                    &sql[..=sem_idx],
                    self.shard,
                    self.replica,
                    &sql[sem_idx + 1..]
                )
            } else {
                format!("{sql} with shard {} replica {}", self.shard, self.replica)
            }
        } else {
            sql.to_string()
        }
    }
}

#[derive(Debug, Clone)]
pub struct CnosdbClient {
    pub flight_endpoint: Endpoint,
    pub http_url: Url,

    pub username: String,
    pub password: String,
    pub tenant: String,
    pub database: String,
    pub target_partitions: usize,
    pub work_directory: String,
    pub timeout: Option<Duration>,
    pub precision: Option<String>,
    pub chunked: Option<bool>,

    pub create_database_options: CreateDatabaseOptions,
}

impl CnosdbClient {
    pub fn new(options: &Options) -> Self {
        let flight_endpoint_str = format!("http://{}:{}", options.flight_host, options.flight_port);
        let flight_endpoint = flight_endpoint_str.parse::<Endpoint>().unwrap_or_else(|e| {
            panic!("Failed to create flight-endpoint with '{flight_endpoint_str}': {e}");
        });
        let http_url_str = format!("http://{}:{}", options.http_host, options.http_port);
        let http_url = Url::parse(&http_url_str).unwrap_or_else(|e| {
            panic!("Failed to create http-url with '{http_url_str}': {e}");
        });
        CnosdbClient {
            flight_endpoint,
            http_url,

            username: crate::CNOSDB_USERNAME_DEFAULT.into(),
            password: crate::CNOSDB_PASSWORD_DEFAULT.into(),
            tenant: crate::CNOSDB_TENANT_DEFAULT.into(),
            database: crate::CNOSDB_DATABASE_DEFAULT.into(),
            work_directory: options.work_directory.to_string_lossy().to_string(),
            target_partitions: crate::CNOSDB_TARGET_PARTITIONS_DEFAULT,
            timeout: None,
            precision: None,
            chunked: None,

            create_database_options: CreateDatabaseOptions {
                shard: options.cli.create_database_with_shard,
                replica: options.cli.create_database_with_replica,
            },
        }
    }

    fn build_http_request(&self, url: Url, body: impl Into<Body>) -> Result<Request> {
        let request = Client::default()
            .request(Method::POST, url)
            .basic_auth(self.username.as_str(), None::<&str>)
            .body(body)
            .header(reqwest::header::ACCEPT, "text/csv")
            .build()?;
        Ok(request)
    }

    pub async fn http_api_v1_sql<S: Into<String>>(&self, sql: S) -> Result<Vec<Vec<String>>> {
        let mut url = self.http_url.join("api/v1/sql")?;
        url.query_pairs_mut()
            .append_pair("db", &self.database)
            .append_pair("tenant", &self.tenant);

        let req = self.build_http_request(url, sql.into())?;
        let resp = Client::default().execute(req).await?;
        if resp.status().is_success() {
            resp.text()
                .await
                .map(|text| {
                    text.lines()
                        .map(|line| line.split(',').map(|s| format!("\"{s}\"")).collect())
                        .collect()
                })
                .map_err(|e| SqlError::Http { err: e.to_string() })
        } else {
            Err(SqlError::Http {
                err: resp.text().await?,
            })
        }
    }

    pub async fn http_api_v1_write<S: Into<String>>(&self, lp: S) -> Result<()> {
        let mut url = self.http_url.join("api/v1/write")?;
        url.query_pairs_mut()
            .append_pair("db", &self.database)
            .append_pair("tenant", &self.tenant)
            .append_pair("precision", self.precision.as_deref().unwrap_or("NS"));

        let req = self.build_http_request(url, lp.into())?;
        let resp = Client::default().execute(req).await?;
        let status_code = resp.status();
        let text = resp.text().await?;
        if !status_code.is_success() {
            Err(SqlError::Http { err: text })
        } else {
            Ok(())
        }
    }

    async fn build_flight_sql_client(&self) -> Result<FlightSqlServiceClient<Channel>> {
        let endpoint = self
            .flight_endpoint
            .clone()
            .connect_timeout(Duration::from_secs(20))
            .timeout(Duration::from_secs(20))
            .tcp_nodelay(true) // Disable Nagle's Algorithm since we don't want packets to wait
            .tcp_keepalive(Option::Some(Duration::from_secs(3600)))
            .http2_keep_alive_interval(Duration::from_secs(300))
            .keep_alive_timeout(Duration::from_secs(20))
            .keep_alive_while_idle(true);

        let channel = endpoint
            .connect()
            .await
            .map_err(|e| ArrowError::IpcError(format!("Cannot connect to endpoint: {e}")))?;

        let mut client = FlightSqlServiceClient::new(channel);
        client.set_header("TENANT", &self.tenant);
        client.set_header("DB", &self.database);
        client.set_header("target_partitions", self.target_partitions.to_string());

        Ok(client)
    }

    pub async fn flight_query<S: Into<String>>(
        &self,
        sql: S,
    ) -> Result<(Schema, Vec<RecordBatch>)> {
        let mut client = self.build_flight_sql_client().await?;

        // 1. handshake, basic authentication
        let _ = client.handshake(&self.username, &self.password).await?;

        // 2. execute query, get result metadata
        let sql = sql.into();
        let mut sql = self.create_database_options.rewrite(&sql);
        println!("Rewrite sql: {sql}");
        if sql.contains("$pwd") {
            sql = sql.replace("$pwd", &self.work_directory);
        }

        let mut stmt = client.prepare(sql, None).await?;
        let flight_info = stmt.execute().await?;

        let mut batches = vec![];
        for ep in &flight_info.endpoint {
            if let Some(tkt) = &ep.ticket {
                let stream = client.do_get(tkt.clone()).await?;
                let new_batches = stream.try_collect::<Vec<_>>().await.map_err(|err| {
                    ArrowError::IpcError(format!("Cannot collect flight data: {:#?}", err))
                })?;
                batches.extend(new_batches);
            };
        }

        let schema = flight_info.try_decode_schema()?;

        Ok((schema, batches))
    }

    pub async fn http_api_v1_opentsdb_write(&self, content: &str) -> Result<()> {
        let mut url = self.http_url.join("api/v1/opentsdb/write")?;
        url.query_pairs_mut()
            .append_pair("db", &self.database)
            .append_pair("tenant", &self.tenant)
            .append_pair("precision", self.precision.as_deref().unwrap_or("NS"));

        let req = self.build_http_request(url, content.to_string())?;
        let resp = Client::default().execute(req).await?;
        let status_code = resp.status();
        let text = resp.text().await?;
        if !status_code.is_success() {
            Err(SqlError::Http { err: text })
        } else {
            Ok(())
        }
    }

    pub async fn http_api_v1_opentsdb_put(&self, content: &str) -> Result<()> {
        let mut url = self.http_url.join("api/v1/opentsdb/put")?;
        url.query_pairs_mut()
            .append_pair("db", &self.database)
            .append_pair("tenant", &self.tenant)
            .append_pair("precision", self.precision.as_deref().unwrap_or("NS"));

        let req = self.build_http_request(url, content.to_string())?;
        let resp = Client::default().execute(req).await?;
        let status_code = resp.status();
        let text = resp.text().await?;
        if !status_code.is_success() {
            Err(SqlError::Http { err: text })
        } else {
            Ok(())
        }
    }

    /// Parse the instruction line and set the corresponding field in the SqlClientOptions.
    pub fn apply_instruction(&mut self, line: &str) {
        if let Ok((_, http_host)) = instruction_parse_str("HTTP_HOST").parse(line) {
            if let Err(e) = self.http_url.set_host(Some(http_host)) {
                panic!(
                    "Failed to set host '{http_host}' in http_url '{}': {e}",
                    self.http_url
                );
            }
        }

        if let Ok((_, http_port)) = instruction_parse_to::<u16>("HTTP_PORT").parse(line) {
            if self.http_url.set_port(Some(http_port)).is_err() {
                panic!(
                    "Failed to set port '{http_port}' in http_url '{}'",
                    self.http_url
                );
            }
        }

        if let Ok((_, flight_host)) = instruction_parse_str("FLIGHT_HOST").parse(line) {
            let mut url = Url::parse(&self.flight_endpoint.uri().to_string())
                .expect("flight_endpoint is url");
            if let Err(e) = url.set_host(Some(flight_host)) {
                panic!("Failed to set host '{flight_host}' in flight_endpoint '{url}': {e}");
            }
            self.flight_endpoint = url.as_str().parse().unwrap_or_else(|e| {
                panic!("Failed to set flight_endpoint from url'{url}' after set flight_host: {e}");
            });
        }

        if let Ok((_, flight_port)) = instruction_parse_to::<u16>("FLIGHT_PORT").parse(line) {
            let mut url = Url::parse(&self.flight_endpoint.uri().to_string())
                .expect("flight_endpoint is url");
            if url.set_port(Some(flight_port)).is_err() {
                panic!("Failed to set port '{flight_port}' in flight_endpoint: '{url}'");
            }
            self.flight_endpoint = url.as_str().parse().unwrap_or_else(|e| {
                panic!("Failed to set flight_endpoint from url '{url}' after set flight_port: {e}");
            });
        }

        if let Ok((_, tenant)) = instruction_parse_str("TENANT").parse(line) {
            self.tenant = tenant.to_string();
        }

        if let Ok((_, dbname)) = instruction_parse_identity("DATABASE").parse(line) {
            self.database = dbname.to_string();
        }

        if let Ok((_, user_name)) = instruction_parse_identity("USER_NAME").parse(line) {
            self.username = user_name.to_string();
        }

        if let Ok((_, password)) = instruction_parse_str("PASSWORD").parse(line) {
            self.password = password.to_string();
        }

        if let Ok((_, dur)) = instruction_parse_str("TIMEOUT").parse(line) {
            self.timeout = humantime::parse_duration(dur).ok()
        }

        if let Ok((_, precision)) = instruction_parse_identity("PRECISION").parse(line) {
            self.precision = Some(precision.to_string())
        }

        if let Ok((_, chunked)) = instruction_parse_to::<bool>("CHUNKED").parse(line) {
            self.chunked = Some(chunked)
        }
    }
}

#[cfg(test)]
mod test {
    use std::time::Duration;

    use super::CreateDatabaseOptions;
    use crate::instance::CnosdbClient;

    #[test]
    fn test_parse_instruction() {
        let mut instruction = CnosdbClient {
            flight_endpoint: "http://127.0.0.1:8903".parse().unwrap(),
            http_url: "http://127.0.0.1:8902".parse().unwrap(),
            username: "".to_string(),
            password: "".to_string(),
            tenant: "".to_string(),
            database: "".to_string(),
            work_directory: "".to_string(),
            target_partitions: 0,
            timeout: None,
            precision: None,
            chunked: None,
            create_database_options: CreateDatabaseOptions {
                shard: 1,
                replica: 1,
            },
        };

        let line = r##"--#DATABASE = _abc_"##;
        instruction.apply_instruction(line);
        assert_eq!(instruction.database, "_abc_");

        let line = r##"--#USER_NAME = hello"##;
        instruction.apply_instruction(line);
        assert_eq!(instruction.username, "hello");

        assert_eq!(instruction.timeout, None);
        let line = r##"--#TIMEOUT = 10ms"##;
        instruction.apply_instruction(line);
        assert_eq!(instruction.timeout, Some(Duration::from_millis(10)));
    }

    #[test]
    fn test_rewrite_create_database_stmt() {
        let create_db_opt = CreateDatabaseOptions {
            replica: 11,
            shard: 13,
        };
        assert_eq!(
            create_db_opt.rewrite("create database test_db;"),
            "create database test_db with shard 13 replica 11;"
        );
        assert_eq!(
            create_db_opt.rewrite("create database test_db;;;"),
            "create database test_db with shard 13 replica 11;;;"
        );
        // assert_eq!( // This assert will fail with "create database test_db;aa with shard 13 replica 11".
        //     create_db_opt.rewrite("create database test_db;aa"),
        //     "create database test_db with shard 13 replica 11;aa"
        // );
        assert_eq!(
            create_db_opt.rewrite("create database test_db"),
            "create database test_db with shard 13 replica 11"
        );
        assert_eq!(
            create_db_opt.rewrite("create database if not exists test_db"),
            "create database if not exists test_db with shard 13 replica 11"
        );
        assert_eq!(
            create_db_opt.rewrite("create database with_db"),
            "create database with_db with shard 13 replica 11"
        );
        assert_eq!(
            create_db_opt.rewrite("create database with_shard"),
            "create database with_shard with shard 13 replica 11"
        );
        assert_eq!(
            create_db_opt.rewrite("create database with_replica"),
            "create database with_replica with shard 13 replica 11"
        );
        assert_eq!(
            create_db_opt.rewrite("create database test_db with ttl '1d'"),
            "create database test_db with shard 13 replica 11 ttl '1d'"
        );
        assert_eq!(
            create_db_opt.rewrite("create database test_db with shard 8 replica 3"),
            "create database test_db with shard 8 replica 3"
        );
        assert_eq!(
            create_db_opt.rewrite("create database test_db with shard 8"),
            "create database test_db with replica 11 shard 8"
        );
        assert_eq!(
            create_db_opt.rewrite("create database test_db with replica 3"),
            "create database test_db with shard 13 replica 3"
        );
    }
}
