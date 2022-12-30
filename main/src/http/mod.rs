use std::net::AddrParseError;

use models::error_code::{ErrorCode, ErrorCoder, UnknownCode};
use snafu::Snafu;
use tokio::sync::mpsc::error::SendError;
use tokio::sync::oneshot::error::RecvError;

use warp::reject;
use warp::reply::Response;

use http_protocol::response::ErrorResponse;
use http_protocol::status_code::UNPROCESSABLE_ENTITY;
use spi::QueryError;

use self::response::ResponseBuilder;

pub mod header;
pub mod http_service;
mod response;
mod result_format;

#[derive(Debug, Snafu, ErrorCoder)]
#[error_code(mod_code = "04")]
#[snafu(visibility(pub))]
pub enum Error {
    Tskv { source: tskv::Error },

    Coordinator {
        source: coordinator::errors::CoordinatorError,
    },

    Meta { source: meta::error::MetaError },

    Query { source: QueryError },

    #[snafu(display("Failed to parse address. err: {}", source))]
    #[error_code(code = 1)]
    AddrParse { source: AddrParseError },

    #[snafu(display("Body oversize: {}", size))]
    #[error_code(code = 2)]
    BodyOversize { size: usize },

    #[snafu(display("Message is not valid UTF-8"))]
    #[error_code(code = 3)]
    NotUtf8,

    #[snafu(display("Error parsing message: {}", source))]
    #[error_code(code = 4)]
    ParseLineProtocol { source: line_protocol::Error },

    #[snafu(display("Invalid header: {}", reason))]
    #[error_code(code = 5)]
    InvalidHeader { reason: String },

    #[snafu(display("Parse auth, malformed basic auth encoding: {}", reason))]
    #[error_code(code = 6)]
    ParseAuth { reason: String },

    #[snafu(display("Fetch result: {}", reason))]
    #[error_code(code = 7)]
    FetchResult { reason: String },

    #[snafu(display("Can't find tenant: {}", name))]
    #[error_code(code = 8)]
    NotFoundTenant { name: String },

    #[snafu(display("Error sending to channel receiver: {}", source))]
    #[error_code(code = 9)]
    ChannelSend { source: SendError<tskv::Task> },

    #[snafu(display("Error receiving from channel receiver: {}", source))]
    #[error_code(code = 10)]
    ChannelReceive { source: RecvError },
}

impl Error {
    // pub fn flatten(self) -> Error {
    //     if let Some(source) = std::error::Error::source(&self) {
    //
    //     } else {
    //         self
    //     }
    //
    // }
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
        ResponseBuilder::new(UNPROCESSABLE_ENTITY).json(&error_resp)
    }
}

impl From<Error> for Response {
    fn from(e: Error) -> Self {
        (&e).into()
    }
}

#[cfg(test)]
mod tests {
    use spi::QueryError;
    use warp::http::header::{HeaderValue, CONTENT_TYPE};

    use http_protocol::{header::APPLICATION_JSON, status_code::BAD_REQUEST};

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

        // #[snafu(display("fails to send to channel"))]
        // Send,

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
