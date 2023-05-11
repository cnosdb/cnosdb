use std::path::PathBuf;
use std::time::Duration;

use arrow::datatypes::Schema;
use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;
use arrow_flight::sql::client::FlightSqlServiceClient;
use arrow_flight::utils::flight_data_to_batches;
use async_trait::async_trait;
use futures::TryStreamExt;
use sqllogictest::{ColumnType, DBOutput};
use tonic::transport::{Channel, Endpoint};

use crate::error::{Result, SqlError};
use crate::utils::normalize;

pub struct CnosDBClient {
    relative_path: PathBuf,
    options: SqlClientOptions,
}

impl CnosDBClient {
    pub fn new(relative_path: impl Into<PathBuf>, options: SqlClientOptions) -> Self {
        Self {
            relative_path: relative_path.into(),
            options,
        }
    }
}

#[async_trait]
impl sqllogictest::AsyncDB for CnosDBClient {
    type Error = SqlError;

    type ColumnType = CnosDBColumnType;

    async fn run(
        &mut self,
        sql: &str,
    ) -> std::result::Result<DBOutput<Self::ColumnType>, Self::Error> {
        println!(
            "[{}] Running query: \"{}\"",
            self.relative_path.display(),
            sql
        );

        let (schema, batches) = run_query(&self.options, sql).await?;
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
}

async fn run_query(
    options: &SqlClientOptions,
    sql: impl Into<String>,
) -> Result<(Schema, Vec<RecordBatch>)> {
    let SqlClientOptions {
        host,
        port,
        username,
        password,
        tenant,
        db,
        target_partitions,
    } = options;

    let channel = flight_channel(host, *port).await?;

    let mut client = FlightSqlServiceClient::new(channel);
    client.set_header("TENANT", tenant);
    client.set_header("DB", db);
    client.set_header("target_partitions", &target_partitions.to_string());

    // 1. handshake, basic authentication
    let _ = client.handshake(username, password).await?;

    // 2. execute query, get result metadata
    let mut stmt = client.prepare(sql.into()).await?;
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
    pub host: String,
    pub port: u16,
    pub username: String,
    pub password: String,
    pub tenant: String,
    pub db: String,
    pub target_partitions: usize,
}
