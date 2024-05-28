use std::borrow::Cow;
use std::time::Duration;

use config::TraceConfig;
use minitrace::collector::Config;
use minitrace_opentelemetry::OpenTelemetryReporter;
use opentelemetry::trace::SpanKind;
use opentelemetry::{InstrumentationLibrary, KeyValue};
use opentelemetry_otlp::{Protocol, WithExportConfig};
use opentelemetry_sdk::Resource;

pub fn init_global_tracing(trace_config: &TraceConfig, service_name: String) {
    match &trace_config.otlp_endpoint {
        None => (),
        Some(endpoint) => {
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
