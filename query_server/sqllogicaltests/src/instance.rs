use std::path::PathBuf;
use std::time::Duration;

use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use arrow_flight::sql::client::{status_to_arrow_error, FlightSqlServiceClient};
use arrow_flight::utils::flight_data_to_batches;
use async_trait::async_trait;
use futures::TryStreamExt;
use sqllogictest::{ColumnType, DBOutput};

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

    let mut client = FlightSqlServiceClient::new_with_endpoint(host, *port).await?;
    client.add_header("TENANT", tenant)?;
    client.add_header("DB", db)?;
    client.add_header("target_partitions", &target_partitions.to_string())?;

    // 1. handshake, basic authentication
    let _ = client.handshake(username, password).await?;

    // 2. execute query, get result metadata
    let mut stmt = client.prepare(sql.into()).await?;
    let flight_info = stmt.execute().await?;

    let mut batches = vec![];
    for ep in &flight_info.endpoint {
        if let Some(tkt) = &ep.ticket {
            let stream = client.do_get(tkt.clone()).await?;
            let flight_data = stream
                .try_collect::<Vec<_>>()
                .await
                .map_err(status_to_arrow_error)?;
            batches.extend(flight_data_to_batches(&flight_data)?);
        };
    }

    let schema = flight_info.try_decode_schema()?;

    Ok((schema, batches))
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
