use reqwest::RequestBuilder;

pub struct HttpClient {
    client: reqwest::Client,
    host: String,
    port: u16,
}

impl HttpClient {
    pub fn from_addr(host: String, port: u16) -> HttpClient {
        HttpClient {
            host,
            port,
            client: reqwest::Client::new(),
        }
    }

    /// Construct test server url
    pub fn url(&self, uri: &str) -> String {
        if uri.starts_with('/') {
            format!("http://{}:{}{}", self.host, self.port, uri)
        } else {
            format!("http://{}:{}/{}", self.host, self.port, uri)
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
