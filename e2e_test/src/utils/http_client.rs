use std::path::Path;

use reqwest::blocking::{ClientBuilder, Request, RequestBuilder, Response};
use reqwest::{Certificate, IntoUrl, Method, StatusCode};

use crate::utils::split_csv_into_vec;
use crate::{E2eError, E2eResult};

#[derive(Debug, Clone)]
pub struct Client {
    inner: reqwest::blocking::Client,
    user: String,
    password: Option<String>,
}

impl Client {
    pub fn new() -> Self {
        let inner = ClientBuilder::new().no_proxy().build().unwrap_or_else(|e| {
            panic!("Failed to build http client: {}", e);
        });
        Self {
            inner,
            user: String::new(),
            password: None,
        }
    }

    pub fn with_auth(user: String, password: Option<String>) -> Self {
        let inner = ClientBuilder::new().no_proxy().build().unwrap_or_else(|e| {
            panic!("Failed to build http client: {}", e);
        });
        Self {
            inner,
            user,
            password,
        }
    }

    pub fn with_auth_and_tls(
        user: String,
        password: Option<String>,
        crt_path: impl AsRef<Path>,
    ) -> Self {
        let cert_bytes = std::fs::read(crt_path).expect("fail to read crt file");
        let cert = Certificate::from_pem(&cert_bytes).expect("fail to load crt file");
        let inner = ClientBuilder::new()
            .no_proxy()
            .add_root_certificate(cert)
            .build()
            .unwrap_or_else(|e| {
                panic!("Failed to build http client with tls: {}", e);
            });
        Self {
            inner,
            user,
            password,
        }
    }

    pub fn auth(&self) -> String {
        format!("{}:{}", self.user, self.password.as_deref().unwrap_or(""))
    }

    /// Returns request builder with method and url.
    pub fn request(&self, method: Method, url: impl IntoUrl) -> RequestBuilder {
        self.inner.request(method, url)
    }

    /// Returns request builder with method and url, and basic_auth header if user is not empty.
    pub fn request_with_auth(&self, method: Method, url: impl IntoUrl) -> RequestBuilder {
        let mut req_builder = self.inner.request(method, url);
        if !self.user.is_empty() {
            req_builder = req_builder.basic_auth(&self.user, self.password.as_ref());
        }
        req_builder
    }

    pub fn execute(&self, request: Request) -> E2eResult<Response> {
        self.inner
            .execute(request)
            .map_err(|e| E2eError::Connect(format!("HTTP execute failed: {e}")))
    }

    pub fn send<U: IntoUrl + std::fmt::Display>(
        &self,
        method: Method,
        url: U,
        body: &str,
        content_encoding: Option<&str>,
        accept_encoding: Option<&str>,
    ) -> E2eResult<Response> {
        let url_str = format!("{url}");
        let mut req_builder = self.request_with_auth(method, url);
        if let Some(encoding) = content_encoding {
            req_builder = req_builder.header(reqwest::header::CONTENT_ENCODING, encoding);
        }
        if let Some(encoding) = accept_encoding {
            req_builder = req_builder.header(reqwest::header::ACCEPT, encoding);
        }
        if !body.is_empty() {
            req_builder = req_builder.body(body.to_string());
        }

        match req_builder.send() {
            Ok(r) => Ok(r),
            Err(e) => Err(Self::map_reqwest_err(e, url_str.as_str(), body)),
        }
    }

    pub fn map_reqwest_err(e: reqwest::Error, url: &str, req: &str) -> E2eError {
        let msg = format!("HTTP request failed: url: '{url}', req: '{req}', error: {e}");
        E2eError::Connect(msg)
    }

    pub fn map_reqwest_resp_err(resp: Response, url: &str, req: &str) -> E2eError {
        let status = resp.status();
        match resp.text() {
            Ok(resp_msg) => E2eError::Api {
                status,
                url: Some(url.to_string()),
                req: Some(req.to_string()),
                resp: Some(resp_msg),
            },
            Err(e) => E2eError::Http {
                status,
                url: Some(url.to_string()),
                req: Some(req.to_string()),
                err: Some(e.to_string()),
            },
        }
    }

    pub fn get<U: IntoUrl + std::fmt::Display>(&self, url: U, body: &str) -> E2eResult<Response> {
        self.send(Method::GET, url, body, None, None)
    }

    pub fn post<U: IntoUrl + std::fmt::Display>(&self, url: U, body: &str) -> E2eResult<Response> {
        self.send(Method::POST, url, body, None, None)
    }

    pub fn post_json<U: IntoUrl + std::fmt::Display>(
        &self,
        url: U,
        body: &str,
    ) -> E2eResult<Response> {
        self.send(Method::POST, url, body, Some("application/json"), None)
    }

    pub fn put<U: IntoUrl + std::fmt::Display>(&self, url: U, body: &str) -> E2eResult<Response> {
        self.send(Method::PUT, url, body, None, None)
    }

    pub fn delete<U: IntoUrl + std::fmt::Display>(
        &self,
        url: U,
        body: &str,
    ) -> E2eResult<Response> {
        self.send(Method::DELETE, url, body, None, None)
    }

    pub fn head<U: IntoUrl + std::fmt::Display>(&self, url: U, body: &str) -> E2eResult<Response> {
        self.send(Method::HEAD, url, body, None, None)
    }

    #[allow(unused)]
    pub fn options<U: IntoUrl + std::fmt::Display>(
        &self,
        url: U,
        body: &str,
    ) -> E2eResult<Response> {
        self.send(Method::OPTIONS, url, body, None, None)
    }

    #[allow(unused)]
    pub fn connect<U: IntoUrl + std::fmt::Display>(
        &self,
        url: U,
        body: &str,
    ) -> E2eResult<Response> {
        self.send(Method::CONNECT, url, body, None, None)
    }

    #[allow(unused)]
    pub fn patch<U: IntoUrl + std::fmt::Display>(&self, url: U, body: &str) -> E2eResult<Response> {
        self.send(Method::PATCH, url, body, None, None)
    }

    #[allow(unused)]
    pub fn trace<U: IntoUrl + std::fmt::Display>(&self, url: U, body: &str) -> E2eResult<Response> {
        self.send(Method::TRACE, url, body, None, None)
    }

    pub fn api_v1_sql<U: IntoUrl + std::fmt::Display>(
        &self,
        url: U,
        sql: &str,
    ) -> E2eResult<Vec<String>> {
        let url_str = format!("{url}");
        let resp = self.post(url, sql)?;
        if resp.status() != StatusCode::OK {
            return Err(Client::map_reqwest_resp_err(resp, &url_str, sql));
        }
        let resp_csv_str = resp
            .text()
            .map_err(|e| Client::map_reqwest_err(e, &url_str, sql))?;
        let resp_lines = split_csv_into_vec(resp_csv_str.trim());
        Ok(resp_lines)
    }

    pub fn api_v1_write<U: IntoUrl + std::fmt::Display>(
        &self,
        url: U,
        req: &str,
    ) -> E2eResult<Vec<String>> {
        let url_str = format!("{url}");
        let resp = self.post(url, req)?;
        if resp.status() != StatusCode::OK {
            return Err(Client::map_reqwest_resp_err(resp, &url_str, req));
        }
        Ok(resp
            .text()
            .map_err(|e| Client::map_reqwest_err(e, &url_str, req))?
            .trim()
            .split_terminator('\n')
            .map(|s| s.to_owned())
            .collect::<Vec<_>>())
    }
}
