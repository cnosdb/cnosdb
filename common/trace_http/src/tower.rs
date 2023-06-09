use std::sync::Arc;
use std::task::{Context, Poll};

use http::Request;
use tower::{Layer, Service};
use trace::{warn, TraceExporter};

use crate::ctx::TraceHeaderParser;

/// `TraceLayer` implements `tower::Layer` and can be used to decorate a
/// `tower::Service` to collect information about requests flowing through it
///
/// Including:
///
/// - Extracting distributed trace context and attaching span context
#[derive(Debug, Clone)]
pub struct TraceLayer {
    trace_header_parser: TraceHeaderParser,
    collector: Option<Arc<dyn TraceExporter>>,
    name: Arc<str>,
}

impl TraceLayer {
    /// Create a new tower [`Layer`] for tracing
    pub fn new(
        trace_header_parser: TraceHeaderParser,
        collector: Option<Arc<dyn TraceExporter>>,
        name: &str,
    ) -> Self {
        Self {
            trace_header_parser,
            collector,
            name: name.into(),
        }
    }
}

impl<S> Layer<S> for TraceLayer {
    type Service = TraceService<S>;

    fn layer(&self, service: S) -> Self::Service {
        TraceService {
            service,
            collector: self.collector.clone(),
            trace_header_parser: self.trace_header_parser.clone(),
            name: Arc::clone(&self.name),
        }
    }
}

#[derive(Debug, Clone)]
pub struct TraceService<S> {
    service: S,
    trace_header_parser: TraceHeaderParser,
    collector: Option<Arc<dyn TraceExporter>>,
    name: Arc<str>,
}

impl<S, R> Service<Request<R>> for TraceService<S>
where
    S: Service<Request<R>>,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = S::Future;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.service.poll_ready(cx)
    }

    fn call(&mut self, mut request: Request<R>) -> Self::Future {
        match self
            .trace_header_parser
            .parse(self.collector.as_ref(), request.headers())
        {
            Ok(Some(ctx)) => {
                if ctx.sampled && self.collector.is_some() {
                    request.extensions_mut().insert(ctx);
                }
            }
            Ok(None) => {}
            Err(e) => {
                warn!(
                    "parse trace context from request {}, error: {}",
                    self.name, e
                );
            }
        };

        self.service.call(request)
    }
}
