use std::borrow::Cow;
use std::io::Write;
use std::path::PathBuf;
use std::time::Duration;

use config::tskv::TraceConfig;
use minitrace::collector::{Config, Reporter, SpanRecord};
use minitrace_opentelemetry::OpenTelemetryReporter;
use opentelemetry::trace::SpanKind;
use opentelemetry::{InstrumentationLibrary, KeyValue};
use opentelemetry_otlp::{Protocol, WithExportConfig};
use opentelemetry_sdk::Resource;

pub fn init_global_tracing(trace_config: &TraceConfig, service_name: String) {
    if let Some(trace_log_path) = &trace_config.trace_log_path {
        let path = PathBuf::from(trace_log_path).join("trace.log");
        let reporter = FileReporter::new(path);
        minitrace::set_reporter(
            reporter,
            Config::default()
                .batch_report_interval(trace_config.batch_report_interval)
                .batch_report_max_spans(trace_config.batch_report_max_spans)
                .max_spans_per_trace(trace_config.max_spans_per_trace),
        );
    }

    if let Some(endpoint) = &trace_config.otlp_endpoint {
        let reporter = opentelemetry_reporter(endpoint.to_owned(), service_name);
        minitrace::set_reporter(
            reporter,
            Config::default()
                .batch_report_interval(trace_config.batch_report_interval)
                .batch_report_max_spans(trace_config.batch_report_max_spans)
                .max_spans_per_trace(trace_config.max_spans_per_trace),
        );
    }
}

pub fn finalize_global_tracing() {
    minitrace::flush();
}

fn opentelemetry_reporter(endpoint: String, service_name: String) -> OpenTelemetryReporter {
    OpenTelemetryReporter::new(
        opentelemetry_otlp::new_exporter()
            .tonic()
            .with_endpoint(endpoint)
            .with_protocol(Protocol::Grpc)
            .with_timeout(Duration::from_secs(
                opentelemetry_otlp::OTEL_EXPORTER_OTLP_TIMEOUT_DEFAULT,
            ))
            .build_span_exporter()
            .expect("initialize oltp exporter"),
        SpanKind::Server,
        Cow::Owned(Resource::new([KeyValue::new("service.name", service_name)])),
        InstrumentationLibrary::new(
            "cnosdb",
            Some(env!("CARGO_PKG_VERSION")),
            None::<&'static str>,
            None,
        ),
    )
}

pub struct FileReporter {
    file: std::fs::File,
}

impl FileReporter {
    pub fn new(path: PathBuf) -> Self {
        let file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .unwrap_or_else(|e| {
                panic!("Failed to open file: {:?}", e);
            });
        Self { file }
    }
}

impl Reporter for FileReporter {
    fn report(&mut self, spans: &[SpanRecord]) {
        for span in spans {
            let _ = self.file.write_all(format!("{:#?}", span).as_bytes());
        }
    }
}
