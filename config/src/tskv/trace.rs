use std::time::Duration;

use derive_traits::Keys;
use serde::{Deserialize, Serialize};

use crate::codec::duration;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Keys)]
pub struct TraceConfig {
    #[serde(default = "TraceConfig::default_auto_generate_span")]
    pub auto_generate_span: bool,
    #[serde(default = "TraceConfig::default_max_spans_per_trace")]
    pub max_spans_per_trace: Option<usize>,
    #[serde(
        with = "duration",
        default = "TraceConfig::default_batch_report_interval"
    )]
    pub batch_report_interval: Duration,
    #[serde(default = "TraceConfig::default_batch_report_max_spans")]
    pub batch_report_max_spans: Option<usize>,
    #[serde(default = "TraceConfig::default_otlp_endpoint")]
    pub otlp_endpoint: Option<String>,
    #[serde(default = "TraceConfig::default_trace_log_path")]
    pub trace_log_path: Option<String>,
}

impl TraceConfig {
    fn default_auto_generate_span() -> bool {
        false
    }

    fn default_max_spans_per_trace() -> Option<usize> {
        None
    }

    fn default_batch_report_interval() -> Duration {
        Duration::from_millis(500)
    }

    fn default_batch_report_max_spans() -> Option<usize> {
        None
    }

    fn default_otlp_endpoint() -> Option<String> {
        None
    }

    fn default_trace_log_path() -> Option<String> {
        None
    }
}

impl Default for TraceConfig {
    fn default() -> Self {
        Self {
            auto_generate_span: Self::default_auto_generate_span(),
            max_spans_per_trace: Self::default_max_spans_per_trace(),
            batch_report_interval: Self::default_batch_report_interval(),
            batch_report_max_spans: Self::default_batch_report_max_spans(),
            otlp_endpoint: Self::default_otlp_endpoint(),
            trace_log_path: Self::default_trace_log_path(),
        }
    }
}

#[test]
fn test_serialize() {
    let trace_config = TraceConfig::default();
    let res = toml::to_string_pretty(&trace_config).unwrap();
    println!("{res}");
}
