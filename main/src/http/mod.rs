use coordinator::errors::CoordinatorError;
use http_protocol::response::ErrorResponse;
use http_protocol::status_code::UNPROCESSABLE_ENTITY;
use meta::error::MetaError;
use models::error_code::{ErrorCode, ErrorCoder};
use snafu::Snafu;
use spi::QueryError;
use trace::http::http_ctx::ContextError;
use warp::reject;
use warp::reply::Response;

use self::response::ResponseBuilder;

mod api_type;
mod encoding;
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
        source: tskv::TskvError,
    },

    Coordinator {
        source: CoordinatorError,
    },

    Meta {
        source: MetaError,
    },

    Query {
        source: QueryError,
    },

    #[snafu(display("Error parsing message: {}", source))]
    #[error_code(code = 4)]
    ParseLineProtocol {
        source: protocol_parser::LineProtocolError,
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
    PProf {
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
        source: trace::http::http_ctx::ContextError,
    },

    #[snafu(display("Error decode request: {}", source))]
    #[error_code(code = 13)]
    DecodeRequest {
        source: std::io::Error,
    },

    #[snafu(display("Error encode response: {}", source))]
    #[error_code(code = 14)]
    EncodeResponse {
        source: std::io::Error,
    },

    #[snafu(display("Invalid utf-8 sequence: {}", source))]
    #[error_code(code = 15)]
    InvalidUTF8 {
        source: simdutf8::basic::Utf8Error,
    },

    #[snafu(display("Error parsing message: {}", source))]
    #[error_code(code = 16)]
    ParseLog {
        source: protocol_parser::JsonLogError,
    },

    #[snafu(display("Error parsing message: {}", source))]
    #[error_code(code = 17)]
    ParseLogJson {
        source: serde_json::Error,
    },

    #[snafu(display("Error context: {}", source))]
    #[error_code(code = 18)]
    Context {
        source: ContextError,
    },
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
            | Error::Coordinator { .. }
            | Error::Meta { .. }
            | Error::NotFoundTenant { .. }
            | Error::EncodeResponse { .. }
            | Error::ParseLineProtocol { .. }
            | Error::ParseLog { .. }
            | Error::ParseLogJson { .. }
            | Error::InvalidUTF8 { .. } => {
                ResponseBuilder::new(UNPROCESSABLE_ENTITY).json(&error_resp)
            }
            Error::InvalidHeader { .. }
            | Error::ParseAuth { .. }
            | Error::TraceHttp { .. }
            | Error::DecodeRequest { .. }
            | Error::ParseOpentsdbProtocol { .. }
            | Error::ParseOpentsdbJsonProtocol { .. } => ResponseBuilder::bad_request(&error_resp),
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
    use tskv::error::CommonSnafu;
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
            source: CommonSnafu {
                reason: "".to_string(),
            }
            .build(),
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
