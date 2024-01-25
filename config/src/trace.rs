use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct TraceConfig {
    pub auto_generate_span: bool,
    pub max_spans_per_trace: Option<usize>,
    pub batch_report_interval_millis: u64,
    pub batch_report_max_spans: Option<usize>,
    pub jaeger_endpoint: Option<String>,
}

impl Default for TraceConfig {
    fn default() -> Self {
        Self {
            auto_generate_span: Default::default(),
            max_spans_per_trace: Default::default(),
            batch_report_interval_millis: 500,
            batch_report_max_spans: Default::default(),
            jaeger_endpoint: Default::default(),
        }
    }
}

#[test]
fn test_serialize() {
    let trace_config = TraceConfig::default();
    let res = toml::to_string_pretty(&trace_config).unwrap();
    println!("{res}");
}
