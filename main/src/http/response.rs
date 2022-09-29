use models::error_code::ErrorCode;
use serde::{Deserialize, Serialize};
use warp::http::header::HeaderMap;
use warp::http::header::CONTENT_TYPE;
use warp::http::HeaderValue;
use warp::http::StatusCode;
use warp::reply::Response;
use warp::Reply;

use super::header::IntoHeaderPair;
use super::header::APPLICATION_JSON;
use super::status_code::BAD_REQUEST;
use super::status_code::INTERNAL_SERVER_ERROR;
use super::status_code::METHOD_NOT_ALLOWED;
use super::status_code::NOT_FOUND;
use super::status_code::OK;
use super::status_code::PAYLOAD_TOO_LARGE;

#[derive(Debug, Serialize)]
pub struct EmptyResponse {}

#[derive(Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct ErrorResponse {
    error_code: String,
    error_message: String,
}

impl ErrorResponse {
    pub fn new(error_code: ErrorCode, error_message: String) -> ErrorResponse {
        Self {
            error_code: error_code.as_str().to_string(),
            error_message,
        }
    }
}

#[derive(Default)]
pub struct ResponseBuilder {
    status_code: Option<StatusCode>,
    headers: HeaderMap<HeaderValue>,
}

impl ResponseBuilder {
    pub fn new(status_code: StatusCode) -> Self {
        Self {
            status_code: Some(status_code),
            ..Default::default()
        }
    }

    pub fn insert_header(mut self, header: impl IntoHeaderPair) -> Self {
        let (key, val) = header.into_pair();
        self.headers.insert(key, val);

        self
    }

    pub fn build(self, body: Vec<u8>) -> Response {
        let mut res = Response::new(body.into());

        *res.headers_mut() = self.headers;

        *res.status_mut() = self.status_code.unwrap();

        res
    }

    pub fn json<T>(self, body: &T) -> Response
    where
        T: Serialize,
    {
        let error = serde_json::to_vec(body).map_err(|err| {
            trace::error!("response::json error: {}", err);
        });

        let builder = self.insert_header((CONTENT_TYPE, APPLICATION_JSON));

        match error {
            Ok(body) => builder.build(body),
            Err(()) => INTERNAL_SERVER_ERROR.into_response(),
        }
    }
}

impl ResponseBuilder {
    pub fn ok() -> Response {
        OK.into_response()
    }

    pub fn bad_request<T>(error_info: &T) -> Response
    where
        T: Serialize,
    {
        Self::new(BAD_REQUEST).json(error_info)
    }

    pub fn not_found() -> Response {
        NOT_FOUND.into_response()
    }

    pub fn internal_server_error() -> Response {
        INTERNAL_SERVER_ERROR.into_response()
    }

    pub fn method_not_allowed() -> Response {
        METHOD_NOT_ALLOWED.into_response()
    }

    pub fn payload_too_large() -> Response {
        PAYLOAD_TOO_LARGE.into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_response() {
        assert_eq!(ResponseBuilder::ok().status(), OK);
        assert_eq!(ResponseBuilder::not_found().status(), NOT_FOUND);
        assert_eq!(
            ResponseBuilder::internal_server_error().status(),
            INTERNAL_SERVER_ERROR
        );
        assert_eq!(
            ResponseBuilder::method_not_allowed().status(),
            METHOD_NOT_ALLOWED
        );
        assert_eq!(
            ResponseBuilder::payload_too_large().status(),
            PAYLOAD_TOO_LARGE
        );
    }

    #[test]
    fn test_bad_request() {
        let error_message = "error";
        let error_resp = ErrorResponse::new(ErrorCode::Unknown, error_message.to_string());
        let resp = ResponseBuilder::bad_request(&error_resp);

        assert_eq!(resp.status(), BAD_REQUEST);

        let content_type = resp.headers().get(CONTENT_TYPE).unwrap();

        assert_eq!(content_type, HeaderValue::from_static(APPLICATION_JSON));
    }
}
