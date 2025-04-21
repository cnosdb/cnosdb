use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TenantLimiterConfig {
    pub object_config: Option<TenantObjectLimiterConfig>,
    pub request_config: Option<RequestLimiterConfig>,
}

#[derive(Serialize, Deserialize, Debug, Clone, Copy)]
pub struct TenantObjectLimiterConfig {
    // add user limit
    pub max_users_number: Option<usize>,
    /// create database limit
    pub max_databases: Option<usize>,
    pub max_shard_number: Option<usize>,
    pub max_replicate_number: Option<usize>,
    pub max_retention_time: Option<usize>,
}

#[derive(Serialize, Deserialize, Debug, Clone, Copy)]
pub struct RateBucketConfig {
    pub max: Option<usize>,
    pub initial: usize,
    pub refill: usize,
    // ms
    pub interval: i64,
}

#[derive(Serialize, Deserialize, Debug, Clone, Copy)]
pub struct CountBucketConfig {
    pub max: Option<i64>,
    pub initial: i64,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct Bucket {
    pub remote_bucket: RateBucketConfig,
    pub local_bucket: CountBucketConfig,
}

#[derive(Serialize, Deserialize, Debug, Clone, Copy)]
pub struct RequestLimiterConfig {
    pub coord_data_in: Option<Bucket>,
    pub coord_data_out: Option<Bucket>,
    pub coord_queries: Option<Bucket>,
    pub coord_writes: Option<Bucket>,
    pub http_data_in: Option<Bucket>,
    pub http_data_out: Option<Bucket>,
    pub http_queries: Option<Bucket>,
    pub http_writes: Option<Bucket>,
}

#[test]
fn test_config() {
    let config_str = r#"
[object_config]
# add user limit
max_users_number = 1
# create database limit
max_databases = 3
max_shard_number = 2
max_replicate_number = 2
max_retention_time = 30


[request_config.coord_data_in]
local_bucket = {max = 100, initial = 0}
remote_bucket = {max = 100, initial = 0, refill = 100, interval = 100}


[request_config.coord_data_out]
local_bucket = {max = 100, initial = 0}
remote_bucket = {max = 100, initial = 0, refill = 100, interval = 100}

[request_config.coord_data_writes]
local_bucket = {max = 100, initial = 0}
remote_bucket = {max = 100, initial = 0, refill = 100, interval = 100}

[request_config.coord_data_queries]
local_bucket = {max = 100, initial = 0}
remote_bucket = {max = 100, initial = 0, refill = 100, interval = 100}

[request_config.http_data_in]
local_bucket = {max = 100, initial = 0}
remote_bucket = {max = 100, initial = 0, refill = 100, interval = 100}

[request_config.http_data_out]
local_bucket = {max = 100, initial = 0}
remote_bucket = {max = 100, initial = 0, refill = 100, interval = 100}

[request_config.http_queries]
local_bucket = {max = 100, initial = 0}
remote_bucket = {max = 100, initial = 0, refill = 100, interval = 100}

[request_config.http_writes]
local_bucket = {max = 100, initial = 0}
remote_bucket = {max = 100, initial = 0, refill = 100, interval = 100}
"#;

    let config: TenantLimiterConfig = toml::from_str(config_str).unwrap();
    dbg!(config);
}
