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
use reqwest::{Client, Method, Request, Url};
use sqllogictest::{ColumnType, DBOutput};
use tonic::transport::{Channel, Endpoint};

use crate::db_request::{
    instruction_parse_identity, instruction_parse_str, instruction_parse_to, DBRequest,
};
use crate::error::{Result, SqlError};
use crate::utils::normalize;

pub struct CnosDBClient {
    relative_path: PathBuf,
    options: SqlClientOptions,
}

impl CnosDBClient {
    pub fn new(
        relative_path: impl Into<PathBuf>,
        options: SqlClientOptions,
    ) -> Result<Self, SqlError> {
        Ok(Self {
            relative_path: relative_path.into(),
            options,
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

        let (schema, batches) = request.execute(&self.options, &self.relative_path).await?;
        let types = normalize::convert_schema_to_types(schema.fields());
        let rows = normalize::convert_batches(batches)?;

        if rows.is_empty() && types.is_empty() {
            Ok(DBOutput::StatementComplete(0))
        } else {
            Ok(DBOutput::Rows { types, rows })
        }
    }

    fn engine_name(&self) -> &str {
        "CnosDB"
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

fn build_http_write_request(option: &SqlClientOptions, url: Url, body: &str) -> Result<Request> {
    let request = Client::default()
        .request(Method::POST, url)
        .basic_auth::<&str, &str>(option.username.as_str(), None)
        .body(body.to_string())
        .build()?;
    Ok(request)
}

pub async fn run_query(
    options: &SqlClientOptions,
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
    let mut stmt = client.prepare(sql.into(), None).await?;
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

pub async fn run_lp_write(options: &SqlClientOptions, lp: &str) -> Result<()> {
    let request = build_http_write_request(options, construct_write_url(options)?, lp)?;
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
    let request =
        build_http_write_request(options, construct_opentsdb_write_url(options)?, content)?;
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
    let request =
        build_http_write_request(options, construct_opentsdb_json_url(options)?, content)?;
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
