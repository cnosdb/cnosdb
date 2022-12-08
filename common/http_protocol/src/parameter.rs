use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct SqlParam {
    pub tenant: Option<String>,
    pub db: Option<String>,
    pub chunked: Option<String>,
    // Number of partitions for query execution. Increasing partitions can increase concurrency.
    pub target_partitions: Option<usize>,
}

#[derive(Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct WriteParam {
    pub tenant: Option<String>,
    pub db: String,
}
