use http::HeaderMap;
use minitrace::collector::SpanContext;
use snafu::Snafu;
use tonic::metadata::errors::InvalidMetadataValue;
use tonic::metadata::MetadataMap;

use crate::span_ctx_ext::{DecodeError, SpanContextExt};

pub const DEFAULT_TRACE_HEADER_NAME: &str = "cnosdb-trace-ctx";

#[derive(Debug, Snafu)]
pub enum ContextError {
    #[snafu(display("header '{}' has non-UTF8 content: {}", header, source))]
    InvalidUtf8 {
        header: String,
        source: http::header::ToStrError,
    },

    #[snafu(display("decoding header '{}': {}", header, source))]
    HeaderDecodeError { header: String, source: DecodeError },
}

impl From<DecodeError> for ContextError {
    fn from(value: DecodeError) -> Self {
        ContextError::HeaderDecodeError {
            header: DEFAULT_TRACE_HEADER_NAME.to_owned(),
            source: value,
        }
    }
}

pub fn span_context_from_http_headers(
    headers: &HeaderMap,
) -> Result<Option<SpanContext>, ContextError> {
    if let Some(value) = headers.get(DEFAULT_TRACE_HEADER_NAME) {
        let s = value.to_str().map_err(|source| ContextError::InvalidUtf8 {
            header: DEFAULT_TRACE_HEADER_NAME.to_owned(),
            source,
        })?;
        SpanContext::from_str(s)
            .map(Some)
            .map_err(ContextError::from)
    } else {
        Ok(None)
    }
}

pub fn grpc_append_trace_context(
    span_ctx: Option<&SpanContext>,
    headers: &mut MetadataMap,
) -> Result<(), InvalidMetadataValue> {
    if let Some(context) = span_ctx {
        let value = context.to_string();
        headers.insert(DEFAULT_TRACE_HEADER_NAME, value.parse()?);
    }
    Ok(())
}
