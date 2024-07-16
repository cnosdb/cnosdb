use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct SqlParam {
    pub tenant: Option<String>,
    pub db: Option<String>,
    pub chunked: Option<bool>,
    // Number of partitions for query execution. Increasing partitions can increase concurrency.
    pub target_partitions: Option<usize>,
    pub stream_trigger_interval: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct WriteParam {
    pub precision: Option<String>,
    pub tenant: Option<String>,
    pub db: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct DumpParam {
    pub tenant: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct DebugParam {
    pub id: Option<u32>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct LogParam {
    pub tenant: Option<String>,
    pub db: Option<String>,
    pub table: Option<String>,
    pub log_type: Option<String>,
    pub tag_columns: Option<String>,
    pub time_column: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct GetOperationParam {
    pub service: Option<String>,

    #[serde(rename = "spanKind")]
    pub span_kind: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct FindTracesParam {
    pub service: Option<String>,   // service=xxx
    pub operation: Option<String>, // operation=xxx
    pub start: Option<i64>,        // start=xxx (usec since epoch)
    pub end: Option<i64>,          // end=xxx (usec since epoch)
    pub tag: Option<String>,       // tag=k1:v1,k2:v2,...
    pub tags: Option<String>,      // tags=json,json e.g. {"k1":"v1"},{"k2":"v2"}
    pub limit: Option<usize>,      // limit=xxx

    #[serde(rename = "minDuration")]
    pub min_duration: Option<String>, // minDuration=1ms

    #[serde(rename = "maxDuration")]
    pub max_duration: Option<String>, // maxDuration=5ms

    #[serde(rename = "traceID")]
    pub trace_ids: Option<String>, // traceID=xxx,xxx
}
