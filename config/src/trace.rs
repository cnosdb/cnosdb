use std::path::PathBuf;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct TraceConfig {
    pub auto_generate_span: bool,
    pub http: Option<HttpCollectorConfig>,
    pub log: Option<LogCollectorConfig>,
    pub jaeger: Option<JaegerCollectorConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct HttpCollectorConfig {
    //TODO
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct LogCollectorConfig {
    pub path: PathBuf, //TODO
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct JaegerCollectorConfig {
    pub jaeger_agent_endpoint: String,
    pub max_concurrent_exports: usize,
    pub max_queue_size: usize,
}

impl Default for JaegerCollectorConfig {
    fn default() -> Self {
        Self {
            jaeger_agent_endpoint: "http://localhost:14268/api/traces".into(),
            max_concurrent_exports: 2,
            max_queue_size: 4096,
        }
    }
}

#[test]
fn test_serialize() {
    let trace_config = TraceConfig::default();
    let res = toml::to_string_pretty(&trace_config).unwrap();
    println!("{res}");
}
