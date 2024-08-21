use std::path::PathBuf;
use std::process::{Command, ExitStatus};
use std::time::Duration;

use arrow::datatypes::Schema;
use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;
use arrow_flight::sql::client::FlightSqlServiceClient;
use arrow_flight::utils::flight_data_to_batches;
use async_trait::async_trait;
use futures::TryStreamExt;
use regex::Regex;
use reqwest::{Body, Client, Method, Request, Url};
use sqllogictest::{ColumnType, DBOutput};
use tonic::transport::{Channel, Endpoint};

use crate::db_request::{
    instruction_parse_identity, instruction_parse_str, instruction_parse_to, DBRequest,
};
use crate::error::{Result, SqlError};

pub struct CnosDBClient {
    engine_name: String,
    relative_path: PathBuf,
    options: SqlClientOptions,
    create_options: CreateOptions,
}

impl CnosDBClient {
    pub fn new(
        engine_name: impl Into<String>,
        relative_path: impl Into<PathBuf>,
        options: SqlClientOptions,
        create_options: CreateOptions,
    ) -> Result<Self, SqlError> {
        Ok(Self {
            engine_name: engine_name.into(),
            relative_path: relative_path.into(),
            options,
            create_options,
        })
    }
}

#[async_trait]
impl sqllogictest::AsyncDB for CnosDBClient {
    type Error = SqlError;

    type ColumnType = CnosDBColumnType;

    async fn run(
        &mut self,
        request: &str,
    ) -> std::result::Result<DBOutput<Self::ColumnType>, Self::Error> {
        let request = DBRequest::parse_db_request(request, &mut self.options)?;

        let (types, rows) = request
            .execute(&self.options, &self.create_options, &self.relative_path)
            .await?;

        if rows.is_empty() && types.is_empty() {
            Ok(DBOutput::StatementComplete(0))
        } else {
            Ok(DBOutput::Rows { types, rows })
        }
    }

    fn engine_name(&self) -> &str {
        &self.engine_name
    }

    async fn sleep(dur: Duration) {
        tokio::time::sleep(dur).await;
    }

    async fn run_command(command: Command) -> std::io::Result<ExitStatus> {
        tokio::process::Command::from(command).status().await
    }
}

pub fn construct_write_url(options: &SqlClientOptions) -> Result<Url> {
    let SqlClientOptions {
        http_host,
        http_port,
        tenant,
        db,
        precision,
        ..
    } = options;
    let url = Url::parse(&format!("http://{}:{}", http_host, http_port))?;
    let mut url = url.join("api/v1/write")?;
    let mut http_query = String::new();

    http_query.push_str("db=");
    http_query.push_str(db.as_str());

    http_query.push_str("&tenant=");
    http_query.push_str(tenant.as_str());

    http_query.push_str("&precision=");
    http_query.push_str(precision.as_ref().map(|s| s.as_str()).unwrap_or("NS"));
    url.set_query(Some(http_query.as_str()));

    Ok(url)
}

pub fn construct_sql_url(options: &SqlClientOptions) -> Result<Url> {
    let SqlClientOptions {
        http_host,
        http_port,
        tenant,
        db,
        ..
    } = options;
    let url = Url::parse(&format!("http://{}:{}", http_host, http_port))?;
    let mut url = url.join("api/v1/sql")?;
    let mut http_query = String::new();

    http_query.push_str("db=");
    http_query.push_str(db.as_str());

    http_query.push_str("&tenant=");
    http_query.push_str(tenant.as_str());
    url.set_query(Some(http_query.as_str()));

    Ok(url)
}

fn construct_opentsdb_write_url(option: &SqlClientOptions) -> Result<Url> {
    let SqlClientOptions {
        http_host,
        http_port,
        tenant,
        db,
        precision,
        ..
    } = option;

    let url = Url::parse(&format!("http://{}:{}", http_host, http_port))?;
    let mut url = url.join("api/v1/opentsdb/write")?;

    let mut http_query = String::new();

    http_query.push_str("db=");
    http_query.push_str(db.as_str());

    http_query.push_str("&tenant=");
    http_query.push_str(tenant.as_str());

    http_query.push_str("&precision=");
    http_query.push_str(precision.as_ref().map(|e| e.as_str()).unwrap_or("NS"));

    url.set_query(Some(http_query.as_str()));
    Ok(url)
}

fn construct_opentsdb_json_url(option: &SqlClientOptions) -> Result<Url> {
    let SqlClientOptions {
        http_host,
        http_port,
        tenant,
        db,
        precision,
        ..
    } = option;
    let url = Url::parse(&format!("http://{}:{}", http_host, http_port))?;
    let mut url = url.join("api/v1/opentsdb/put").unwrap();

    let mut http_query = String::new();
    http_query.push_str("db=");
    http_query.push_str(db);

    http_query.push_str("&tenant=");
    http_query.push_str(tenant);

    http_query.push_str("&precision=");
    http_query.push_str(precision.as_ref().map(|e| e.as_str()).unwrap_or("NS"));

    url.set_query(Some(http_query.as_str()));
    Ok(url)
}

fn build_http_request(
    option: &SqlClientOptions,
    url: Url,
    body: impl Into<Body>,
) -> Result<Request> {
    let request = Client::default()
        .request(Method::POST, url)
        .basic_auth::<&str, &str>(option.username.as_str(), None)
        .body(body)
        .header(reqwest::header::ACCEPT, "text/csv")
        .build()?;
    Ok(request)
}

pub async fn run_query(
    options: &SqlClientOptions,
    create_option: &CreateOptions,
    sql: impl Into<String>,
) -> Result<(Schema, Vec<RecordBatch>)> {
    let SqlClientOptions {
        flight_host,
        flight_port,
        username,
        password,
        tenant,
        db,
        target_partitions,
        ..
    } = options;

    let channel = flight_channel(flight_host, *flight_port).await?;

    let mut client = FlightSqlServiceClient::new(channel);
    client.set_header("TENANT", tenant);
    client.set_header("DB", db);
    client.set_header("target_partitions", &target_partitions.to_string());

    // 1. handshake, basic authentication
    let _ = client.handshake(username, password).await?;

    // 2. execute query, get result metadata
    let mut sql = Into::<String>::into(sql);
    let re = Regex::new(r"create\s+database").unwrap();
    if re.is_match(&sql) {
        let with = sql.to_ascii_lowercase().find("with");
        if with.is_some() {
            let with = with.unwrap();
            if !sql.to_ascii_lowercase().contains("shard") {
                sql.insert_str(
                    with + 4,
                    format!(" shard {}", create_option.shard_num).as_str(),
                );
            }

            if !sql.to_ascii_lowercase().contains("replica") {
                sql.insert_str(
                    with + 4,
                    format!(" replica {}", create_option.replication_num).as_str(),
                );
            }
        } else {
            sql.insert_str(
                sql.len() - 1,
                format!(
                    " with shard {} replica {}",
                    create_option.shard_num, create_option.replication_num
                )
                .as_str(),
            );
        }
    }

    if sql.contains("$pwd") {
        sql = sql.replace("$pwd", options.pwd.as_str());
    }

    let mut stmt = client.prepare(sql, None).await?;
    let flight_info = stmt.execute().await?;

    let mut batches = vec![];
    for ep in &flight_info.endpoint {
        if let Some(tkt) = &ep.ticket {
            let stream = client.do_get(tkt.clone()).await?;
            let flight_data = stream.try_collect::<Vec<_>>().await.map_err(|err| {
                ArrowError::IoError(format!("Cannot collect flight data: {:#?}", err))
            })?;
            batches.extend(flight_data_to_batches(&flight_data)?);
        };
    }

    let schema = flight_info.try_decode_schema()?;

    Ok((schema, batches))
}

pub async fn run_http_api_v1_sql(
    options: &SqlClientOptions,
    sql: impl Into<String>,
) -> Result<Vec<Vec<String>>> {
    let request = build_http_request(options, construct_sql_url(options)?, sql.into())?;
    let client = Client::default();
    let resp = client.execute(request).await?;
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

pub async fn run_lp_write(options: &SqlClientOptions, lp: &str) -> Result<()> {
    let request = build_http_request(options, construct_write_url(options)?, lp.to_string())?;
    println!("{:#?}", request);
    let client = Client::default();
    let resp = client.execute(request).await?;
    let status_code = resp.status();
    let text = resp.text().await?;
    if !status_code.is_success() {
        Err(SqlError::Http { err: text })
    } else {
        Ok(())
    }
}

pub async fn run_open_tsdb_write(options: &SqlClientOptions, content: &str) -> Result<()> {
    let request = build_http_request(
        options,
        construct_opentsdb_write_url(options)?,
        content.to_string(),
    )?;
    let client = Client::default();
    let resp = client.execute(request).await?;
    let status_code = resp.status();
    let text = resp.text().await?;
    if !status_code.is_success() {
        Err(SqlError::Http { err: text })
    } else {
        Ok(())
    }
}

pub async fn run_open_tsdb_json_write(options: &SqlClientOptions, content: &str) -> Result<()> {
    let request = build_http_request(
        options,
        construct_opentsdb_json_url(options)?,
        content.to_string(),
    )?;
    let client = Client::default();
    let resp = client.execute(request).await?;
    let status_code = resp.status();
    let text = resp.text().await?;
    if !status_code.is_success() {
        Err(SqlError::Http { err: text })
    } else {
        Ok(())
    }
}

async fn flight_channel(host: &str, port: u16) -> Result<Channel> {
    let endpoint = Endpoint::new(format!("http://{}:{}", host, port))
        .map_err(|_| ArrowError::IoError("Cannot create endpoint".to_string()))?
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
        .map_err(|e| ArrowError::IoError(format!("Cannot connect to endpoint: {e}")))?;

    Ok(channel)
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum CnosDBColumnType {
    Boolean,
    Integer,
    Float,
    Text,
    Timestamp,
    Another,
}

impl ColumnType for CnosDBColumnType {
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
pub struct CreateOptions {
    pub replication_num: u32,
    pub shard_num: u32,
}

#[derive(Debug, Clone)]
pub struct SqlClientOptions {
    pub flight_host: String,
    pub flight_port: u16,
    pub http_host: String,
    pub http_port: u16,
    pub username: String,
    pub password: String,
    pub tenant: String,
    pub db: String,
    pub target_partitions: usize,
    pub pwd: String,
    pub timeout: Option<Duration>,
    pub precision: Option<String>,
    pub chunked: Option<bool>,
}

impl SqlClientOptions {
    pub fn parse_and_change(&mut self, line: &str) {
        if let Ok((_, http_host)) = instruction_parse_str("HTTP_HOST")(line) {
            self.http_host = http_host.to_string();
        }

        if let Ok((_, http_port)) = instruction_parse_to::<u16>("HTTP_PORT")(line) {
            self.http_port = http_port;
        }

        if let Ok((_, flight_host)) = instruction_parse_str("FLIGHT_HOST")(line) {
            self.flight_host = flight_host.to_string();
        }

        if let Ok((_, flight_port)) = instruction_parse_to::<u16>("FLIGHT_PORT")(line) {
            self.flight_port = flight_port;
        }

        if let Ok((_, tenant)) = instruction_parse_str("TENANT")(line) {
            self.tenant = tenant.to_string();
        }

        if let Ok((_, dbname)) = instruction_parse_identity("DATABASE")(line) {
            self.db = dbname.to_string();
        }

        if let Ok((_, user_name)) = instruction_parse_identity("USER_NAME")(line) {
            self.username = user_name.to_string();
        }

        if let Ok((_, password)) = instruction_parse_str("PASSWORD")(line) {
            self.password = password.to_string();
        }

        if let Ok((_, dur)) = instruction_parse_str("TIMEOUT")(line) {
            self.timeout = humantime::parse_duration(dur).ok()
        }

        if let Ok((_, precision)) = instruction_parse_identity("PRECISION")(line) {
            self.precision = Some(precision.to_string())
        }

        if let Ok((_, chunked)) = instruction_parse_to::<bool>("CHUNKED")(line) {
            self.chunked = Some(chunked)
        }
    }
}
#[cfg(test)]
mod test {
    use std::time::Duration;

    use crate::instance::SqlClientOptions;

    #[test]
    fn test_parse_instruction() {
        let mut instruction = SqlClientOptions {
            flight_host: "".to_string(),
            flight_port: 0,
            http_host: "".to_string(),
            http_port: 0,
            username: "".to_string(),
            password: "".to_string(),
            tenant: "".to_string(),
            db: "".to_string(),
            pwd: "".to_string(),
            target_partitions: 0,
            timeout: None,
            precision: None,
            chunked: None,
        };

        let line = r##"--#DATABASE = _abc_"##;
        instruction.parse_and_change(line);
        assert_eq!(instruction.db, "_abc_");

        let line = r##"--#USER_NAME = hello"##;
        instruction.parse_and_change(line);
        assert_eq!(instruction.username, "hello");

        assert_eq!(instruction.timeout, None);
        let line = r##"--#TIMEOUT = 10ms"##;
        instruction.parse_and_change(line);
        assert_eq!(instruction.timeout, Some(Duration::from_millis(10)));
    }
}
