use std::any::Any;
use std::borrow::Cow;
use std::sync::Arc;

use config::JaegerCollectorConfig;
use opentelemetry_api::trace::{Event, TraceError};
use opentelemetry_api::{trace, InstrumentationLibrary, Key, KeyValue, Value};
use opentelemetry_jaeger::config::collector::CollectorPipeline;
use opentelemetry_sdk::export::trace::SpanData;
use opentelemetry_sdk::trace::{BatchSpanProcessor, EvictedHashMap, EvictedQueue, SpanProcessor};
use opentelemetry_sdk::{runtime, Resource};

use crate::{MetaValue, Span, SpanContext, SpanEvent, SpanId, SpanStatus, TraceExporter, TraceId};

pub fn jaeger_exporter(
    config: &JaegerCollectorConfig,
    service_name: impl Into<String>,
) -> Result<Arc<dyn TraceExporter>, TraceError> {
    let inner = build_batch_span_processor(config, service_name)?;
    let exporter = JaegerExporter::with_span_processor(inner);
    Ok(Arc::new(exporter))
}

fn build_batch_span_processor(
    config: &JaegerCollectorConfig,
    service_name: impl Into<String>,
) -> Result<BatchSpanProcessor<runtime::Tokio>, TraceError> {
    let exporter = CollectorPipeline::default()
        // .with_endpoint("http://localhost:14268/api/traces")
        .with_endpoint(&config.jaeger_agent_endpoint)
        .with_hyper()
        .with_service_name(service_name)
        .build_collector_exporter::<runtime::Tokio>()?;

    // runtime::Tokio: 使用当前线程上下文的Tokio运行时
    let processor = BatchSpanProcessor::builder(exporter, runtime::Tokio)
        .with_max_concurrent_exports(config.max_concurrent_exports)
        .with_max_queue_size(config.max_queue_size)
        .build();

    Ok(processor)
}

#[derive(Debug)]
struct JaegerExporter<T> {
    inner: T,
}

impl<T> JaegerExporter<T>
where
    T: SpanProcessor + 'static,
{
    pub fn with_span_processor(inner: T) -> Self {
        Self { inner }
    }
}

impl<T> TraceExporter for JaegerExporter<T>
where
    T: SpanProcessor + 'static,
{
    fn export(&self, span: Span) {
        self.inner.on_end(span.into());
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl From<Span> for SpanData {
    fn from(span: Span) -> Self {
        let Span {
            name,
            ctx,
            start,
            end,
            status,
            metadata,
            events,
        } = span;

        let parent_span_id = if let Some(parent_span_id) = ctx.parent_span_id {
            parent_span_id.into()
        } else {
            trace::SpanId::INVALID
        };

        let start_time = start.unwrap_or_default().into();
        let end_time = end.unwrap_or_default().into();

        let mut attributes = EvictedHashMap::new(u32::MAX, metadata.len());
        for (k, v) in metadata {
            attributes.insert(KeyValue::new(key(k), v));
        }

        let mut jaeger_events = EvictedQueue::new(events.len() as u32);
        jaeger_events.extend(events.into_iter().map(Event::from));

        let instrumentation_lib =
            InstrumentationLibrary::new("trace", Some(env!("CARGO_PKG_VERSION")), None);

        SpanData {
            span_context: ctx.into(),
            parent_span_id,
            span_kind: trace::SpanKind::Server,
            name,
            start_time,
            end_time,
            attributes,
            events: jaeger_events,
            links: EvictedQueue::new(0),
            status: status.into(),
            resource: Cow::Owned(Resource::default()),
            instrumentation_lib,
        }
    }
}

impl From<SpanContext> for trace::SpanContext {
    fn from(value: SpanContext) -> Self {
        let trace_flags = if value.sampled {
            trace::TraceFlags::SAMPLED
        } else {
            trace::TraceFlags::default()
        };

        trace::SpanContext::new(
            value.trace_id.into(),
            value.span_id.into(),
            trace_flags,
            false,
            trace::TraceState::default(),
        )
    }
}

impl From<TraceId> for trace::TraceId {
    fn from(value: TraceId) -> Self {
        let span_id = value.0.get().to_be_bytes();
        trace::TraceId::from_bytes(span_id)
    }
}

impl From<SpanId> for trace::SpanId {
    fn from(value: SpanId) -> Self {
        let span_id = value.0.get().to_be_bytes();
        trace::SpanId::from_bytes(span_id)
    }
}

impl From<MetaValue> for Value {
    fn from(value: MetaValue) -> Self {
        match value {
            MetaValue::String(e) => e.into(),
            MetaValue::Float(e) => e.into(),
            MetaValue::Int(e) => e.into(),
            MetaValue::Bool(e) => e.into(),
            MetaValue::U64(e) => (e as i64).into(),
        }
    }
}

impl From<SpanStatus> for trace::Status {
    fn from(value: SpanStatus) -> Self {
        match value {
            SpanStatus::Unknown => trace::Status::Unset,
            SpanStatus::Ok => trace::Status::Ok,
            SpanStatus::Err => trace::Status::error(""),
        }
    }
}

impl From<SpanEvent> for Event {
    fn from(value: SpanEvent) -> Self {
        let SpanEvent { time, msg } = value;
        Event::new(msg, time.into(), vec![], 0)
    }
}

fn key(key: Cow<'static, str>) -> Key {
    match key {
        Cow::Borrowed(e) => Key::from_static_str(e),
        Cow::Owned(e) => Key::new(e),
    }
}

#[cfg(test)]
mod tests {
    use std::borrow::Cow;
    use std::time::SystemTime;

    use opentelemetry_api::trace::{
        SpanContext, SpanId, SpanKind, Status, TraceFlags, TraceId, TraceState,
    };
    use opentelemetry_api::InstrumentationLibrary;
    use opentelemetry_jaeger::config::collector::CollectorPipeline;
    use opentelemetry_sdk::export::trace::SpanData;
    use opentelemetry_sdk::trace::{self, EvictedHashMap, EvictedQueue, SpanProcessor};
    use opentelemetry_sdk::{runtime, Resource};

    #[ignore = "environment"]
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_collector_exporter() {
        let exporter = CollectorPipeline::default()
            .with_endpoint("http://localhost:14268/api/traces")
            .with_hyper()
            .with_service_name("test")
            .build_collector_exporter::<runtime::Tokio>()
            .unwrap();

        let mut batch = trace::BatchSpanProcessor::builder(exporter, runtime::Tokio)
            .with_max_queue_size(4096)
            .build();

        let span = SpanData {
            span_context: SpanContext::new(
                TraceId::INVALID,
                SpanId::INVALID,
                TraceFlags::SAMPLED,
                false,
                TraceState::default(),
            ),
            parent_span_id: SpanId::INVALID,
            span_kind: SpanKind::Server,
            name: "todo!()".into(),
            start_time: SystemTime::now(),
            end_time: SystemTime::now(),
            attributes: EvictedHashMap::new(10, 10),
            events: EvictedQueue::new(10),
            links: EvictedQueue::new(5),
            status: Status::Ok,
            resource: Cow::Owned(Resource::default()),
            instrumentation_lib: InstrumentationLibrary::new("xx", None, None),
        };

        batch.on_end(span);

        batch.force_flush().unwrap();

        batch.shutdown().unwrap();
    }
}
