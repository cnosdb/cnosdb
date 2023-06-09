pub mod jaeger;
pub mod log;

use std::any::Any;

use crate::Span;

pub trait TraceExporter: std::fmt::Debug + Send + Sync {
    /// Exports the specified `Span` for collection by the sink
    fn export(&self, span: Span);

    /// Cast client to [`Any`], useful for downcasting.
    fn as_any(&self) -> &dyn Any;
}
