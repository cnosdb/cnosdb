use coordinator::errors::CoordinatorError;
use http_protocol::response::ErrorResponse;
use http_protocol::status_code::UNPROCESSABLE_ENTITY;
use meta::error::MetaError;
use models::error_code::{ErrorCode, ErrorCoder};
use snafu::Snafu;
use spi::QueryError;
use warp::reject;
use warp::reply::Response;

use self::response::ResponseBuilder;

pub mod header;
pub mod http_service;
mod metrics;
mod response;
mod result_format;

#[derive(Debug, Snafu, ErrorCoder)]
#[error_code(mod_code = "04")]
#[snafu(visibility(pub))]
pub enum Error {
    Tskv {
        source: tskv::Error,
    },

    Coordinator {
        source: coordinator::errors::CoordinatorError,
    },

    Meta {
        source: meta::error::MetaError,
    },

    Query {
        source: QueryError,
    },

    #[snafu(display("Error parsing message: {}", source))]
    #[error_code(code = 4)]
    ParseLineProtocol {
        source: protocol_parser::Error,
    },

    #[snafu(display("Invalid header: {}", reason))]
    #[error_code(code = 5)]
    InvalidHeader {
        reason: String,
    },

    #[snafu(display("Parse auth, malformed basic auth encoding: {}", reason))]
    #[error_code(code = 6)]
    ParseAuth {
        reason: String,
    },

    #[snafu(display("Fetch result: {}", reason))]
    #[error_code(code = 7)]
    FetchResult {
        reason: String,
    },

    #[snafu(display("Can't find tenant: {}", name))]
    #[error_code(code = 8)]
    NotFoundTenant {
        name: String,
    },

    #[snafu(display("gerante pprof files: {}", reason))]
    #[error_code(code = 9)]
    PProfError {
        reason: String,
    },

    #[snafu(display("Error parsing message: {}", source))]
    #[error_code(code = 10)]
    ParseOpentsdbProtocol {
        source: protocol_parser::Error,
    },

    #[snafu(display("Error parsing message: {}", source))]
    #[error_code(code = 11)]
    ParseOpentsdbJsonProtocol {
        source: serde_json::Error,
    },

    #[snafu(display("Parse trace context, error: {}", source))]
    #[error_code(code = 12)]
    TraceHttp {
        source: trace_http::ctx::ContextError,
    },
}

impl From<tskv::Error> for Error {
    fn from(value: tskv::Error) -> Self {
        Error::Tskv { source: value }
    }
}

impl From<CoordinatorError> for Error {
    fn from(value: CoordinatorError) -> Self {
        Error::Coordinator { source: value }
    }
}

impl From<MetaError> for Error {
    fn from(value: MetaError) -> Self {
        Error::Meta { source: value }
    }
}

impl From<QueryError> for Error {
    fn from(value: QueryError) -> Self {
        Error::Query { source: value }
    }
}

impl From<trace_http::ctx::ContextError> for Error {
    fn from(source: trace_http::ctx::ContextError) -> Self {
        Error::TraceHttp { source }
    }
}

impl reject::Reject for Error {}

impl Error {
    pub fn error_code(&self) -> &dyn ErrorCode {
        match self {
            Error::Query { source } => source.error_code(),
            Error::Meta { source } => source.error_code(),

            Error::Tskv { source } => source.error_code(),

            Error::Coordinator { source } => source.error_code(),

            _ => self,
        }
    }
}

impl From<&Error> for Response {
    fn from(e: &Error) -> Self {
        let error_resp = ErrorResponse::new(e.error_code());
        match e {
            Error::Query { .. }
            | Error::FetchResult { .. }
            | Error::Tskv { .. }
            | Error::Coordinator { .. } => {
                ResponseBuilder::new(UNPROCESSABLE_ENTITY).json(&error_resp)
            }
            Error::InvalidHeader { .. } | Error::ParseAuth { .. } | Error::TraceHttp { .. } => {
                ResponseBuilder::bad_request(&error_resp)
            }
            _ => ResponseBuilder::internal_server_error(),
        }
    }
}

impl From<Error> for Response {
    fn from(e: Error) -> Self {
        (&e).into()
    }
}

#[cfg(test)]
mod tests {
    use http_protocol::header::APPLICATION_JSON;
    use http_protocol::status_code::BAD_REQUEST;
    use spi::QueryError;
    use warp::http::header::{HeaderValue, CONTENT_TYPE};

    use super::*;

    #[test]
    fn test_query_error() {
        let q_err = QueryError::BuildQueryDispatcher {
            err: "test".to_string(),
        };
        let resp: Response = Error::Query { source: q_err }.into();

        assert_eq!(resp.status(), UNPROCESSABLE_ENTITY);

        let content_type = resp.headers().get(CONTENT_TYPE).unwrap();

        assert_eq!(content_type, HeaderValue::from_static(APPLICATION_JSON));
    }

    #[test]
    fn test_fetch_result_error() {
        let resp: Response = Error::FetchResult {
            reason: "test".to_string(),
        }
        .into();

        assert_eq!(resp.status(), UNPROCESSABLE_ENTITY);

        let content_type = resp.headers().get(CONTENT_TYPE).unwrap();

        assert_eq!(content_type, HeaderValue::from_static(APPLICATION_JSON));
    }

    #[test]
    fn test_tskv_error() {
        let resp: Response = Error::Tskv {
            source: tskv::Error::CommonError {
                reason: "".to_string(),
            },
        }
        .into();

        assert_eq!(resp.status(), UNPROCESSABLE_ENTITY);

        let content_type = resp.headers().get(CONTENT_TYPE).unwrap();

        assert_eq!(content_type, HeaderValue::from_static(APPLICATION_JSON));
    }

    #[test]
    fn test_invalid_header_error() {
        let resp: Response = Error::InvalidHeader {
            reason: "test".to_string(),
        }
        .into();

        assert_eq!(resp.status(), BAD_REQUEST);

        let content_type = resp.headers().get(CONTENT_TYPE).unwrap();

        assert_eq!(content_type, HeaderValue::from_static(APPLICATION_JSON));
    }

    #[test]
    fn test_parse_auth_error() {
        let resp: Response = Error::ParseAuth {
            reason: "test".to_string(),
        }
        .into();

        assert_eq!(resp.status(), BAD_REQUEST);

        let content_type = resp.headers().get(CONTENT_TYPE).unwrap();

        assert_eq!(content_type, HeaderValue::from_static(APPLICATION_JSON));
    }
}
