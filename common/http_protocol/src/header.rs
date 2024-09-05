// re-export const header names
pub use reqwest::header::{HeaderValue, ACCEPT, AUTHORIZATION, CONTENT_TYPE};

// header
// privateKey
pub const PRIVATE_KEY: &str = "X-CnosDB-PrivateKey";

// value
pub const APPLICATION_PREFIX: &str = "application/";
pub const TEXT_PREFIX: &str = "text/";
pub const APPLICATION_CSV: &str = "text/csv";
pub const APPLICATION_TSV: &str = "text/tsv";
pub const APPLICATION_JSON: &str = "application/json";
pub const APPLICATION_NDJSON: &str = "application/nd-json";
pub const APPLICATION_TABLE: &str = "text/table";
pub const APPLICATION_STAR: &str = "application/*";
pub const STAR_STAR: &str = "*/*";

// basic auth
pub const BASIC_PREFIX: &str = "Basic ";
pub const BEARER_PREFIX: &str = "Bearer ";

// parameters
pub const TENANT: &str = "tenant";
pub const DB: &str = "db";
pub const TABLE: &str = "table";
pub const TARGET_PARTITIONS: &str = "target_partitions";
pub const STREAM_TRIGGER_INTERVAL: &str = "stream_trigger_interval";

// encoding
pub const GZIP: &str = "gzip";
pub const DEFLATE: &str = "deflate";
pub const BROTLI: &str = "br";
pub const ZSTD: &str = "zstd";
pub const SNAPPY: &str = "snappy";
pub const LZ4: &str = "lz4";
pub const IDENTITY: &str = "identity";
