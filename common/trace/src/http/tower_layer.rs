use std::sync::Arc;
use std::task::{Context, Poll};

use http::Request;
use tower::{Layer, Service};

use super::http_ctx::span_context_from_http_headers;
use crate::{warn, SpanContext};

/// `TraceLayer` implements `tower::Layer` and can be used to decorate a
/// `tower::Service` to collect information about requests flowing through it
///
/// Including:
///
/// - Extracting distributed trace context and attaching span context
#[derive(Debug, Clone)]
pub struct TraceLayer {
    auto_generate_span: bool,
    name: Arc<str>,
}

impl TraceLayer {
    /// Create a new tower [`Layer`] for tracing
    pub fn new(auto_generate_span: bool, name: &str) -> Self {
        Self {
            auto_generate_span,
            name: name.into(),
        }
    }
}

impl<S> Layer<S> for TraceLayer {
    type Service = TraceService<S>;

    fn layer(&self, service: S) -> Self::Service {
        TraceService {
            service,
            auto_generate_span: self.auto_generate_span,
            name: Arc::clone(&self.name),
        }
    }
}

#[derive(Debug, Clone)]
pub struct TraceService<S> {
    service: S,
    auto_generate_span: bool,
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
        match (
            self.auto_generate_span,
            span_context_from_http_headers(request.headers()),
        ) {
            (_, Ok(Some(ctx))) => {
                request.extensions_mut().insert(ctx);
            }
            (_, Err(e)) => {
                warn!(
                    "parse trace context from request {}, error: {}",
                    self.name, e
                );
            }
            (true, _) => {
                request.extensions_mut().insert(SpanContext::random());
            }
            _ => {}
        }
        self.service.call(request)
    }
}
