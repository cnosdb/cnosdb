use std::path::PathBuf;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialOrd, PartialEq, Ord, Eq)]
pub struct TraceConfig {
    pub http: Option<HttpCollectorConfig>,
    pub log: Option<LogCollectorConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialOrd, PartialEq, Ord, Eq)]
pub struct HttpCollectorConfig {
    //TODO
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialOrd, PartialEq, Ord, Eq)]
pub struct LogCollectorConfig {
    pub path: PathBuf, //TODO
}

#[test]
fn test_serialize() {
    let trace_config = TraceConfig::default();
    let res = toml::to_string_pretty(&trace_config).unwrap();
    println!("{res}");
}
