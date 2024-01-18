use std::num::ParseIntError;

use itertools::Itertools;
use minitrace::collector::{SpanContext, SpanId, TraceId};
use snafu::Snafu;

#[derive(Debug, Snafu)]
pub enum DecodeError {
    #[snafu(display("value decode error: {}", source))]
    ValueDecodeError { source: ParseIntError },

    #[snafu(display("Expected \"trace-id:span-id\", found: {}", value))]
    InvalidTrace { value: String },
}

impl From<ParseIntError> for DecodeError {
    // Snafu doesn't allow both no context and a custom message
    fn from(source: ParseIntError) -> Self {
        Self::ValueDecodeError { source }
    }
}

pub trait SpanContextExt {
    fn to_string(&self) -> String;
    fn from_str(s: &str) -> Result<Self, DecodeError>
    where
        Self: Sized;
}

impl SpanContextExt for SpanContext {
    fn to_string(&self) -> String {
        let TraceId(trace_id) = self.trace_id;
        let SpanId(span_id) = self.span_id;
        format!("{trace_id:x}:{span_id:x}")
    }

    fn from_str(s: &str) -> Result<Self, DecodeError> {
        let (trace_id, span_id) =
            s.split(':')
                .collect_tuple()
                .ok_or(DecodeError::InvalidTrace {
                    value: s.to_string(),
                })?;
        let trace_id = parse_trace(trace_id)?;
        let span_id = parse_span(span_id)?;
        Ok(SpanContext::new(TraceId(trace_id), SpanId(span_id)))
    }
}

fn parse_trace(s: &str) -> Result<u128, DecodeError> {
    u128::from_str_radix(s, 16).map_err(|source| DecodeError::ValueDecodeError { source })
}

fn parse_span(s: &str) -> Result<u64, DecodeError> {
    u64::from_str_radix(s, 16).map_err(|source| DecodeError::ValueDecodeError { source })
}
