use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use crate::override_by_env::{entry_override, OverrideByEnv};

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct TraceConfig {
    pub auto_generate_span: bool,
    pub http: Option<HttpCollectorConfig>,
    pub log: Option<LogCollectorConfig>,
    pub jaeger: Option<JaegerCollectorConfig>,
}

impl OverrideByEnv for TraceConfig {
    fn override_by_env(&mut self) {
        entry_override(
            &mut self.auto_generate_span,
            "CNOSDB_TRACE_AUTO_GENERATE_SPAN",
        );
        // self.http.override_by_env();
        self.log.override_by_env();
        self.jaeger.override_by_env();
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct HttpCollectorConfig {
    //TODO
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct LogCollectorConfig {
    pub path: PathBuf, //TODO
}

impl OverrideByEnv for Option<LogCollectorConfig> {
    fn override_by_env(&mut self) {
        let is_some = self.is_some();
        let mut log_collector = self.take().unwrap_or_default();
        *self = match (
            is_some,
            entry_override(&mut log_collector.path, "CNOSDB_TRACE_LOG_PATH"),
        ) {
            (_, true) | (true, false) => Some(log_collector),
            (false, false) => None,
        };
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct JaegerCollectorConfig {
    pub jaeger_agent_endpoint: String,
    pub max_concurrent_exports: usize,
    pub max_queue_size: usize,
}

impl OverrideByEnv for Option<JaegerCollectorConfig> {
    fn override_by_env(&mut self) {
        let is_some = self.is_some();

        let mut jaeger_collector = self.take().unwrap_or(JaegerCollectorConfig {
            jaeger_agent_endpoint: Default::default(),
            max_concurrent_exports: Default::default(),
            max_queue_size: Default::default(),
        });
        *self = match (
            is_some,
            entry_override(
                &mut jaeger_collector.jaeger_agent_endpoint,
                "CNOSDB_TRACE_JAEGER_AGENT_ENDPOINT",
            ) || entry_override(
                &mut jaeger_collector.max_concurrent_exports,
                "CNOSDB_TRACE_MAX_CONCURRENT_EXPORTS",
            ) || entry_override(
                &mut jaeger_collector.max_queue_size,
                "CNOSDB_TRACE_MAX_QUEUE_SIZE",
            ),
        ) {
            (_, true) | (true, false) => Some(jaeger_collector),
            (false, false) => None,
        }
    }
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
