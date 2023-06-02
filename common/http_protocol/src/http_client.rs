use std::path::PathBuf;

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
    BuildHttpClient { source: reqwest::Error },
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
        let addr = if use_ssl || use_unsafe_ssl {
            format!("https://{}:{}", host, port)
        } else {
            format!("http://{}:{}", host, port)
        };

        let mut client_builder = reqwest::Client::builder();
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

        let client = client_builder.build().context(BuildHttpClientSnafu)?;
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
