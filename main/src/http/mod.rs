use std::net::AddrParseError;

use models::error_code::ErrorCode;
use snafu::Snafu;
use spi::server::ServerError;
use tokio::sync::mpsc::error::SendError;
use tokio::sync::oneshot::error::RecvError;

// use async_channel as channel;
use warp::reject;
use warp::reply::Response;

use self::response::{ErrorResponse, ResponseBuilder};
use self::status_code::UNPROCESSABLE_ENTITY;

mod header;
pub mod http_service;
mod parameter;
mod response;
mod result_format;
mod status_code;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(display("Failed to parse address. err: {}", source))]
    AddrParse { source: AddrParseError },

    #[snafu(display("Body oversize: {}", size))]
    BodyOversize { size: usize },

    #[snafu(display("Message is not valid UTF-8"))]
    NotUtf8,

    #[snafu(display("Error parsing message: {}", source))]
    ParseLineProtocol { source: line_protocol::Error },

    #[snafu(display("Error sending to channel receiver: {}", source))]
    ChannelSend { source: SendError<tskv::Task> },

    #[snafu(display("Error receiving from channel receiver: {}", source))]
    ChannelReceive { source: RecvError },

    // #[snafu(display("Error sending to channel receiver: {}", source))]
    // AsyncChanSend {
    //     source: channel::SendError<tskv::Task>,
    // },
    #[snafu(display("Error executiong query: {}", source))]
    Query { source: ServerError },

    #[snafu(display("Error from tskv: {}", source))]
    Tskv { source: tskv::Error },

    #[snafu(display("Invalid header: {}", reason))]
    InvalidHeader { reason: String },

    #[snafu(display("Parse auth, malformed basic auth encoding: {}", reason))]
    ParseAuth { reason: String },

    #[snafu(display("Fetch result: {}", reason))]
    FetchResult { reason: String },
}

impl reject::Reject for Error {}

impl From<&Error> for Response {
    fn from(e: &Error) -> Self {
        let error_message = format!("{}", e);

        match e {
            Error::Query { source: _ } => {
                let error_resp = ErrorResponse::new(ErrorCode::QueryUnknown, error_message);

                ResponseBuilder::new(UNPROCESSABLE_ENTITY).json(&error_resp)
            }
            Error::FetchResult { reason: _ } => {
                let error_resp = ErrorResponse::new(ErrorCode::QueryUnknown, error_message);

                ResponseBuilder::new(UNPROCESSABLE_ENTITY).json(&error_resp)
            }
            Error::Tskv { source: _ } => {
                let error_resp = ErrorResponse::new(ErrorCode::TskvUnknown, error_message);

                ResponseBuilder::new(UNPROCESSABLE_ENTITY).json(&error_resp)
            }
            Error::InvalidHeader { reason: _ } | Error::ParseAuth { reason: _ } => {
                let error_resp = ErrorResponse::new(ErrorCode::Unknown, error_message);

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
    use spi::query::QueryError;
    use warp::http::header::{HeaderValue, CONTENT_TYPE};

    use crate::http::{header::APPLICATION_JSON, status_code::BAD_REQUEST};

    use super::*;

    #[test]
    fn test_query_error() {
        let q_err = QueryError::BuildQueryDispatcher {
            err: "test".to_string(),
        };
        let s_err = ServerError::Query { source: q_err };

        let resp: Response = Error::Query { source: s_err }.into();

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
            source: tskv::Error::Cancel,
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
