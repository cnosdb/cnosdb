use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct SqlParam {
    #[serde(default = "default_tenant")]
    pub tenant: Option<String>,
    #[serde(default = "default_db")]
    pub db: Option<String>,
    #[serde(default = "default_chunked")]
    pub chunked: Option<bool>,
    // Number of partitions for query execution. Increasing partitions can increase concurrency.
    pub target_partitions: Option<usize>,
    pub stream_trigger_interval: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct WriteParam {
    pub precision: Option<String>,
    #[serde(default = "default_tenant")]
    pub tenant: Option<String>,
    #[serde(default = "default_db")]
    pub db: Option<String>,
}

fn default_tenant() -> Option<String> {
    Some("cnosdb".to_string())
}

fn default_db() -> Option<String> {
    Some("public".to_string())
}

fn default_chunked() -> Option<bool> {
    Some(false)
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
