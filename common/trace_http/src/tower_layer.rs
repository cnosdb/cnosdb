use std::sync::Arc;
use std::task::{Context, Poll};

use http::Request;
use tower::{Layer, Service};
use trace::warn;

use crate::ctx::SpanContextExtractor;

/// `TraceLayer` implements `tower::Layer` and can be used to decorate a
/// `tower::Service` to collect information about requests flowing through it
///
/// Including:
///
/// - Extracting distributed trace context and attaching span context
#[derive(Debug, Clone)]
pub struct TraceLayer {
    extractor: Arc<SpanContextExtractor>,
    name: Arc<str>,
}

impl TraceLayer {
    /// Create a new tower [`Layer`] for tracing
    pub fn new(extractor: Arc<SpanContextExtractor>, name: &str) -> Self {
        Self {
            extractor,
            name: name.into(),
        }
    }
}

impl<S> Layer<S> for TraceLayer {
    type Service = TraceService<S>;

    fn layer(&self, service: S) -> Self::Service {
        TraceService {
            service,
            extractor: self.extractor.clone(),
            name: Arc::clone(&self.name),
        }
    }
}

#[derive(Debug, Clone)]
pub struct TraceService<S> {
    service: S,
    extractor: Arc<SpanContextExtractor>,
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
        match self.extractor.extract_from_headers(request.headers()) {
            Ok(Some(ctx)) if ctx.sampled => {
                request.extensions_mut().insert(ctx);
            }
            Err(e) => {
                warn!(
                    "parse trace context from request {}, error: {}",
                    self.name, e
                );
            }
            _ => {}
        };

        self.service.call(request)
    }
}
