use std::mem;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Poll;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use futures::future::BoxFuture;
use futures::{ready, FutureExt, Stream, StreamExt};
use http_protocol::encoding::Encoding;
use http_protocol::header::{APPLICATION_JSON, CONTENT_TYPE};
use http_protocol::response::ErrorResponse;
use http_protocol::status_code::{
    BAD_REQUEST, INTERNAL_SERVER_ERROR, METHOD_NOT_ALLOWED, NOT_FOUND, OK, PAYLOAD_TOO_LARGE,
};
use meta::limiter::RequestLimiter;
use metrics::count::U64Counter;
use reqwest::header::CONTENT_ENCODING;
use serde::Serialize;
use snafu::ResultExt;
use spi::query::execution::Output;
use spi::QueryError;
use warp::http::header::HeaderMap;
use warp::http::{HeaderValue, StatusCode};
use warp::reply::Response;
use warp::{hyper, Reply};

use super::header::IntoHeaderPair;
use super::result_format::ResultFormat;
use super::{Error as HttpError, MetaSnafu, QuerySnafu};

#[derive(Default)]
pub struct ResponseBuilder {
    status_code: StatusCode,
    headers: HeaderMap<HeaderValue>,
}

impl ResponseBuilder {
    pub fn new(status_code: StatusCode) -> Self {
        Self {
            status_code,
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

        *res.status_mut() = self.status_code;

        res
    }

    pub fn build_stream_response(self, body: impl Reply) -> Response {
        let mut res = body.into_response();

        *res.headers_mut() = self.headers;

        *res.status_mut() = self.status_code;

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
pub type CheckFuture = BoxFuture<'static, Result<(), HttpError>>;
pub enum HttpResponseStreamState {
    PollNext,
    Finish,
    // future, res body, is_finish
    CheckLimiter(CheckFuture, Vec<u8>, bool),
}

pub struct HttpResponse {
    result: Output,
    format: ResultFormat,
    encoding: Option<Encoding>,
    schema: Option<SchemaRef>,
    http_query_data_out: U64Counter,
    limiter: Arc<dyn RequestLimiter>,
    stream_state: HttpResponseStreamState,
}

impl HttpResponse {
    pub fn new(
        result: Output,
        format: ResultFormat,
        encoding: Option<Encoding>,
        http_query_data_out: U64Counter,
        limiter: Arc<dyn RequestLimiter>,
    ) -> Self {
        let schema = result.schema();
        Self {
            result,
            format,
            encoding,
            schema: Some(schema),
            limiter,
            stream_state: HttpResponseStreamState::PollNext,
            http_query_data_out,
        }
    }

    pub async fn wrap_batches_to_response(self) -> Result<Response, HttpError> {
        let actual = self.result.chunk_result().await.context(QuerySnafu)?;
        self.format.wrap_batches_to_response(
            &actual,
            true,
            self.http_query_data_out.clone(),
            self.encoding,
        )
    }
    pub fn wrap_stream_to_response(self) -> Result<Response, HttpError> {
        let mut builder = ResponseBuilder::new(OK)
            .insert_header((CONTENT_TYPE, self.format.get_http_content_type()));
        if let Some(encoding) = self.encoding {
            builder = builder.insert_header((CONTENT_ENCODING, encoding.to_header_value()));
        }
        let resp = builder.build_stream_response(self);
        Ok(resp)
    }

    pub fn handle_opt_result_record_batch(
        &mut self,
        opt_result_batch: Option<Result<RecordBatch, QueryError>>,
    ) -> Result<HttpResponseStreamState, HttpError> {
        match opt_result_batch {
            None => {
                if let Some(schema) = self.schema.take() {
                    let has_headers = !schema.fields().is_empty();
                    let rb = RecordBatch::new_empty(schema);
                    let mut buffer =
                        self.format
                            .format_batches(&[rb], has_headers)
                            .map_err(|e| HttpError::FetchResult {
                                reason: format!("{}", e),
                            })?;
                    if let Some(encoding) = self.encoding {
                        buffer = encoding
                            .encode(buffer)
                            .map_err(|e| HttpError::EncodeResponse { source: e })?;
                    }
                    self.schema = None;
                    let limiter = self.limiter.clone();
                    let buffer_len = buffer.len();
                    self.http_query_data_out.inc(buffer_len as u64);
                    let future = async move {
                        limiter
                            .check_http_data_out(buffer_len)
                            .await
                            .context(MetaSnafu)
                    };
                    Ok(HttpResponseStreamState::CheckLimiter(
                        Box::pin(future),
                        buffer,
                        true,
                    ))
                } else {
                    Ok(HttpResponseStreamState::Finish)
                }
            }
            Some(Ok(rb)) => {
                if rb.num_rows() > 0 {
                    let mut buffer = self
                        .format
                        .format_batches(&[rb], self.schema.is_some())
                        .map_err(|e| HttpError::FetchResult {
                            reason: format!("{}", e),
                        })?;
                    if let Some(encoding) = self.encoding.as_ref() {
                        buffer = encoding
                            .encode(buffer)
                            .map_err(|e| HttpError::EncodeResponse { source: e })?;
                    }
                    self.http_query_data_out.inc(buffer.len() as u64);
                    self.schema = None;

                    let limiter = self.limiter.clone();
                    let buffer_len = buffer.len();
                    let future = async move {
                        limiter
                            .check_http_data_out(buffer_len)
                            .await
                            .context(MetaSnafu)
                    };
                    Ok(HttpResponseStreamState::CheckLimiter(
                        Box::pin(future),
                        buffer,
                        false,
                    ))
                } else {
                    Ok(HttpResponseStreamState::PollNext)
                }
            }
            Some(Err(e)) => {
                let http_error = HttpError::FetchResult {
                    reason: e.to_string(),
                };
                Err(http_error)
            }
        }
    }
}

impl Stream for HttpResponse {
    type Item = Result<Vec<u8>, HttpError>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        loop {
            fn http_error_to_vec(error: HttpError) -> Vec<u8> {
                ErrorResponse::new(error.error_code()).to_vec()
            }
            match &mut self.stream_state {
                HttpResponseStreamState::PollNext => {
                    let res = ready!(self.result.poll_next_unpin(cx));
                    match self.handle_opt_result_record_batch(res) {
                        Ok(state) => self.stream_state = state,
                        Err(e) => {
                            self.stream_state = HttpResponseStreamState::Finish;
                            return Poll::Ready(Some(Ok(http_error_to_vec(e))));
                        }
                    }
                }
                HttpResponseStreamState::Finish => return Poll::Ready(None),
                HttpResponseStreamState::CheckLimiter(future, res, finish) => {
                    return match ready!(future.poll_unpin(cx)).map(|_| res) {
                        Ok(res) => {
                            let res = mem::take(res);
                            if *finish {
                                self.stream_state = HttpResponseStreamState::Finish;
                            } else {
                                self.stream_state = HttpResponseStreamState::PollNext;
                            }
                            Poll::Ready(Some(Ok(res)))
                        }
                        Err(e) => {
                            self.stream_state = HttpResponseStreamState::Finish;
                            Poll::Ready(Some(Ok(http_error_to_vec(e))))
                        }
                    };
                }
            }
        }
    }
}

impl Reply for HttpResponse {
    fn into_response(self) -> Response {
        let body = hyper::Body::wrap_stream(self);
        Response::new(body)
    }
}

#[cfg(test)]
mod tests {
    use derive_traits::error_code::UnknownCode;
    use http_protocol::response::ErrorResponse;

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
        let error_resp = ErrorResponse::new(&UnknownCode);
        let resp = ResponseBuilder::bad_request(&error_resp);

        assert_eq!(resp.status(), BAD_REQUEST);

        let content_type = resp.headers().get(CONTENT_TYPE).unwrap();

        assert_eq!(content_type, HeaderValue::from_static(APPLICATION_JSON));
    }
}
