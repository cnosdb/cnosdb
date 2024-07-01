use std::path::PathBuf;

use reqwest::header::HeaderValue;
use reqwest::{Certificate, RequestBuilder};
use snafu::{ResultExt, Snafu};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to read certificate from path '{}': {source}", path.display()))]
    LoadCertificate {
        source: std::io::Error,
        path: PathBuf,
    },

    #[snafu(display("Failed to parse certificate '{}': {source}", path.display()))]
    ParseCertificate {
        source: reqwest::Error,
        path: PathBuf,
    },

    #[snafu(display("Failed to build http client: {source}"))]
    BuildReqwestHttpClient { source: reqwest::Error },

    #[snafu(display("Failed to build http header: {source}"))]
    BuildHeader {
        source: reqwest::header::InvalidHeaderValue,
    },
}

pub struct HttpClient {
    client: reqwest::Client,
    addr: String,
}

impl HttpClient {
    pub fn new(
        host: &str,
        port: u16,
        use_ssl: bool,
        use_unsafe_ssl: bool,
        cert_files: &[String],
    ) -> Result<HttpClient, Error> {
        Self::with_proxy_settings(
            host,
            port,
            use_ssl,
            use_unsafe_ssl,
            cert_files,
            &None,
            &None,
            &None,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn with_proxy_settings(
        host: &str,
        port: u16,
        use_ssl: bool,
        use_unsafe_ssl: bool,
        cert_files: &[String],
        proxy_url: &Option<String>,
        proxy_basic_auth: &Option<(String, String)>,
        proxy_custom_auth: &Option<String>,
    ) -> Result<HttpClient, Error> {
        let mut client_builder = reqwest::Client::builder();
        let addr = if use_ssl || use_unsafe_ssl {
            client_builder = client_builder.use_rustls_tls();
            format!("https://{}:{}", host, port)
        } else {
            format!("http://{}:{}", host, port)
        };

        if use_unsafe_ssl {
            client_builder = client_builder.danger_accept_invalid_certs(true);
        }
        for p in cert_files {
            let crt_path = PathBuf::from(p);
            let cert_bytes = match std::fs::read(&crt_path) {
                Ok(v) => v,
                Err(e) => {
                    return Err(Error::LoadCertificate {
                        source: e,
                        path: crt_path,
                    })
                }
            };
            let cert = match Certificate::from_pem(&cert_bytes) {
                Ok(v) => v,
                Err(e) => {
                    return Err(Error::ParseCertificate {
                        source: e,
                        path: crt_path,
                    })
                }
            };
            client_builder = client_builder.add_root_certificate(cert);
        }

        client_builder = match proxy_url {
            Some(url) => {
                if !url.is_empty() {
                    let mut proxy =
                        reqwest::Proxy::all(url).context(BuildReqwestHttpClientSnafu)?;
                    if let Some((user, pass)) = proxy_basic_auth {
                        proxy = proxy.basic_auth(user, pass);
                    }
                    if let Some(auth) = proxy_custom_auth {
                        let header = HeaderValue::from_str(auth).context(BuildHeaderSnafu)?;
                        proxy = proxy.custom_http_auth(header)
                    }
                    client_builder.proxy(proxy)
                } else {
                    client_builder.no_proxy()
                }
            }
            None => client_builder.no_proxy(),
        };

        let client = client_builder
            .build()
            .context(BuildReqwestHttpClientSnafu)?;
        Ok(HttpClient { client, addr })
    }

    /// Construct test server url
    pub fn url(&self, uri: &str) -> String {
        if uri.starts_with('/') {
            format!("{}{}", self.addr, uri)
        } else {
            format!("{}/{}", self.addr, uri)
        }
    }

    /// Create `GET` request
    pub fn get<S: AsRef<str>>(&self, path: S) -> RequestBuilder {
        self.client.get(self.url(path.as_ref()).as_str())
    }

    /// Create `POST` request
    pub fn post<S: AsRef<str>>(&self, path: S) -> RequestBuilder {
        self.client.post(self.url(path.as_ref()).as_str())
    }

    /// Create `HEAD` request
    pub fn head<S: AsRef<str>>(&self, path: S) -> RequestBuilder {
        self.client.head(self.url(path.as_ref()).as_str())
    }

    /// Create `PUT` request
    pub fn put<S: AsRef<str>>(&self, path: S) -> RequestBuilder {
        self.client.put(self.url(path.as_ref()).as_str())
    }

    /// Create `PATCH` request
    pub fn patch<S: AsRef<str>>(&self, path: S) -> RequestBuilder {
        self.client.patch(self.url(path.as_ref()).as_str())
    }

    /// Create `DELETE` request
    pub fn delete<S: AsRef<str>>(&self, path: S) -> RequestBuilder {
        self.client.delete(self.url(path.as_ref()).as_str())
    }
}
