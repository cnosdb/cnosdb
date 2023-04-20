use std::borrow::Cow;
use std::num::{NonZeroU128, NonZeroU64};
use std::sync::Arc;

use rand::Rng;

use crate::span::Span;
use crate::{SpanId, TraceCollector, TraceId};

#[derive(Debug, Clone)]
pub struct SpanContext {
    pub trace_id: TraceId,

    pub parent_span_id: Option<SpanId>,

    pub span_id: SpanId,

    pub collector: Option<Arc<dyn TraceCollector>>,

    /// If we should also sample based on this context (i.e. emit child spans).
    pub sampled: bool,
}

impl SpanContext {
    /// Create a new root span context, sent to `collector`. The
    /// new span context has a random trace_id and span_id, and thus
    /// is not connected to any existing span or trace.
    pub fn new(collector: Arc<dyn TraceCollector>) -> Self {
        Self::new_with_optional_collector(Some(collector))
    }

    /// Same as [`new`](Self::new), but with an optional collector.
    pub fn new_with_optional_collector(collector: Option<Arc<dyn TraceCollector>>) -> Self {
        let mut rng = rand::thread_rng();
        let trace_id: u128 = rng.gen_range(1..u128::MAX);
        let span_id: u64 = rng.gen_range(1..u64::MAX);

        Self {
            trace_id: TraceId(NonZeroU128::new(trace_id).unwrap()),
            parent_span_id: None,
            span_id: SpanId(NonZeroU64::new(span_id).unwrap()),
            collector,
            sampled: true,
        }
    }

    /// Creates a new child of the Span described by this TraceContext
    pub fn child(&self, name: impl Into<Cow<'static, str>>) -> Span {
        let ctx = Self {
            trace_id: self.trace_id,
            span_id: SpanId::gen(),
            collector: self.collector.clone(),
            parent_span_id: Some(self.span_id),
            sampled: self.sampled,
        };
        Span::new(name, ctx)
    }
}
impl PartialEq for SpanContext {
    fn eq(&self, other: &Self) -> bool {
        self.trace_id == other.trace_id
            && self.parent_span_id == other.parent_span_id
            && self.span_id == other.span_id
            && self.collector.is_some() == other.collector.is_some()
            && self.sampled == other.sampled
    }
}
