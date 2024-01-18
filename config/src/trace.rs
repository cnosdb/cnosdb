use serde::{Deserialize, Serialize};

use crate::override_by_env::{entry_override, entry_override_option, OverrideByEnv};

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

impl OverrideByEnv for TraceConfig {
    fn override_by_env(&mut self) {
        entry_override(
            &mut self.auto_generate_span,
            "CNOSDB_TRACE_AUTO_GENERATE_SPAN",
        );
        entry_override_option(
            &mut self.max_spans_per_trace,
            "CNOSDB_TRACE_MAX_SPANS_PER_TRACE",
        );
        entry_override(
            &mut self.batch_report_interval_millis,
            "CNOSDB_TRACE_BATCH_REPORT_INTERVAL_MILLIS",
        );
        entry_override_option(
            &mut self.batch_report_max_spans,
            "CNOSDB_TRACE_BATCH_REPORT_MAX_SPANS",
        );
        entry_override_option(&mut self.jaeger_endpoint, "CNOSDB_TRACE_JAEGER_ENDPOINT");
    }
}

#[test]
fn test_serialize() {
    let trace_config = TraceConfig::default();
    let res = toml::to_string_pretty(&trace_config).unwrap();
    println!("{res}");
}
