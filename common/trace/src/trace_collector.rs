use std::any::Any;
use std::collections::VecDeque;
use std::fmt::Debug;
use std::sync::Arc;

use config::LogCollectorConfig;
use parking_lot::Mutex;
use tracing::level_filters::LevelFilter;
use tracing_appender::non_blocking::{NonBlocking, WorkerGuard};
use tracing_subscriber::fmt::format::{DefaultFields, Format};
use tracing_subscriber::fmt::{format, Subscriber};

use crate::{info, Span};

pub trait TraceCollector: std::fmt::Debug + Send + Sync {
    /// Exports the specified `Span` for collection by the sink
    fn export(&self, span: Span);

    /// Cast client to [`Any`], useful for downcasting.
    fn as_any(&self) -> &dyn Any;
}

/// A basic trace collector that prints to stdout
#[allow(dead_code)]
#[derive(Debug)]
pub struct LogTraceCollector {
    subscriber: Arc<Subscriber<DefaultFields, Format<format::Full>, LevelFilter, NonBlocking>>,
    _guard: WorkerGuard,
}

impl LogTraceCollector {
    pub fn new(log_trace_config: &LogCollectorConfig) -> Self {
        // Safety log_trace_collector is initialized at startup, so unwrap can be called
        let file_appender = tracing_appender::rolling::daily(&log_trace_config.path, "trace.log");
        let (non_blocking, _guard) = tracing_appender::non_blocking(file_appender);

        let subscriber = tracing_subscriber::FmtSubscriber::builder()
            .with_ansi(false)
            .with_writer(non_blocking)
            .finish();
        let subscriber = Arc::new(subscriber);
        Self { subscriber, _guard }
    }
}

impl TraceCollector for LogTraceCollector {
    fn export(&self, span: Span) {
        tracing::subscriber::with_default(
            self.subscriber.clone(),
            || info!(target: "log_trace_collector", "completed span {:?}", span),
        );
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// A trace collector that maintains a ring buffer of spans
#[derive(Debug)]
pub struct RingBufferTraceCollector {
    buffer: Mutex<VecDeque<Span>>,
}

impl RingBufferTraceCollector {
    pub fn new(capacity: usize) -> Self {
        Self {
            buffer: Mutex::new(VecDeque::with_capacity(capacity)),
        }
    }

    pub fn spans(&self) -> Vec<Span> {
        self.buffer.lock().iter().cloned().collect()
    }
}

impl TraceCollector for RingBufferTraceCollector {
    fn export(&self, span: Span) {
        let mut buffer = self.buffer.lock();
        if buffer.len() == buffer.capacity() {
            buffer.pop_front();
        }
        buffer.push_back(span);
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[derive(Debug)]
pub struct CombinationTraceCollector {
    trace_collectors: Vec<Arc<dyn TraceCollector>>,
}

impl CombinationTraceCollector {
    pub fn new(trace_collectors: Vec<Arc<dyn TraceCollector>>) -> Self {
        Self { trace_collectors }
    }
}

impl TraceCollector for CombinationTraceCollector {
    fn export(&self, span: Span) {
        self.trace_collectors
            .iter()
            .for_each(|c| c.export(span.clone()));
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
