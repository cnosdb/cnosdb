#[cfg(test)]
mod test {
    use std::net::SocketAddr;

    use reqwest::{
        header::{HeaderValue, ACCEPT, CONTENT_TYPE},
        RequestBuilder, Response, StatusCode,
    };

    pub struct TestClient {
        client: reqwest::Client,
        addr: SocketAddr,
    }

    impl TestClient {
        pub fn from_addr(addr: SocketAddr) -> TestClient {
            TestClient {
                addr,
                client: reqwest::Client::new(),
            }
        }

        /// Construct test server url
        pub fn url(&self, uri: &str) -> String {
            if uri.starts_with('/') {
                format!("http://localhost:{}{}", self.addr.port(), uri)
            } else {
                format!("http://localhost:{}/{}", self.addr.port(), uri)
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

    fn client() -> TestClient {
        let addr = "0.0.0.0:31007".parse::<SocketAddr>().unwrap();
        TestClient::from_addr(addr)
    }

    #[tokio::test]
    async fn test_v1_sql_path() {
        let path = "/api/v1/sql";
        let invalid_path = "/api/v1/xx";
        let param = &[("db", "public")];
        let username = "cnosdb";

        let client = client();

        let body = "select 1;";
        let resp: Response = client
            .post(path)
            .query(param)
            .basic_auth::<&str, &str>(username, None)
            .body(body)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(
            resp.headers().get(CONTENT_TYPE).unwrap(),
            &HeaderValue::from_static("application/csv")
        );

        // invalid basic auth
        let body = "select 1;";
        let resp: Response = client
            .post(path)
            .query(param)
            .bearer_auth(username)
            .body(body)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);

        // lost auth
        let body = "select 1;";
        let resp: Response = client
            .post(path)
            .query(param)
            .body(body)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);

        // accept: application/csv
        let body = "select 1;";
        let resp: Response = client
            .post(path)
            .query(param)
            .basic_auth::<&str, &str>(username, None)
            .header(ACCEPT, "application/csv")
            .body(body)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(
            resp.headers().get(CONTENT_TYPE).unwrap(),
            &HeaderValue::from_static("application/csv")
        );

        // accept: application/json
        let body = "select 1;";
        let resp: Response = client
            .post(path)
            .query(param)
            .basic_auth::<&str, &str>(username, None)
            .header(ACCEPT, "application/json")
            .body(body)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(
            resp.headers().get(CONTENT_TYPE).unwrap(),
            &HeaderValue::from_static("application/json")
        );

        // accept: application/nd-json
        let body = "select 1;";
        let resp: Response = client
            .post(path)
            .query(param)
            .basic_auth::<&str, &str>(username, None)
            .header(ACCEPT, "application/nd-json")
            .body(body)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(
            resp.headers().get(CONTENT_TYPE).unwrap(),
            &HeaderValue::from_static("application/nd-json")
        );

        // accept: application/*
        let body = "select 1;";
        let resp: Response = client
            .post(path)
            .query(param)
            .basic_auth::<&str, &str>(username, None)
            .header(ACCEPT, "application/*")
            .body(body)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(
            resp.headers().get(CONTENT_TYPE).unwrap(),
            &HeaderValue::from_static("application/csv")
        );

        // accept: */*
        let body = "select 1;";
        let resp: Response = client
            .post(path)
            .query(param)
            .basic_auth::<&str, &str>(username, None)
            .header(ACCEPT, "*/*")
            .body(body)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(
            resp.headers().get(CONTENT_TYPE).unwrap(),
            &HeaderValue::from_static("application/csv")
        );

        // accept: *
        let body = "select 1;";
        let resp: Response = client
            .post(path)
            .query(param)
            .basic_auth::<&str, &str>(username, None)
            .header(ACCEPT, "*")
            .body(body)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);

        // accept: xx
        let body = "select 1;";
        let resp: Response = client
            .post(path)
            .query(param)
            .basic_auth::<&str, &str>(username, None)
            .header(ACCEPT, "xx")
            .body(body)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);

        // lost param
        let body = "select 1;";
        let resp: Response = client
            .post(path)
            .basic_auth::<&str, &str>(username, None)
            .body(body)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        // path not found
        let body = "select 1;";
        let resp: Response = client
            .post(invalid_path)
            .query(param)
            .basic_auth::<&str, &str>(username, None)
            .body(body)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);

        // GET
        let body = "select 1;";
        let resp: Response = client
            .get(path)
            .query(param)
            .basic_auth::<&str, &str>(username, None)
            .body(body)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::METHOD_NOT_ALLOWED);

        // PUT
        let body = "select 1;";
        let resp: Response = client
            .put(path)
            .query(param)
            .basic_auth::<&str, &str>(username, None)
            .body(body)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::METHOD_NOT_ALLOWED);

        // PATCH
        let body = "select 1;";
        let resp: Response = client
            .patch(path)
            .query(param)
            .basic_auth::<&str, &str>(username, None)
            .body(body)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::METHOD_NOT_ALLOWED);

        // DELETE
        let body = "select 1;";
        let resp: Response = client
            .delete(path)
            .query(param)
            .basic_auth::<&str, &str>(username, None)
            .body(body)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::METHOD_NOT_ALLOWED);

        // HEAD
        let body = "select 1;";
        let resp: Response = client
            .head(path)
            .query(param)
            .basic_auth::<&str, &str>(username, None)
            .body(body)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::METHOD_NOT_ALLOWED);
    }

    #[tokio::test]
    async fn test_v1_write_path() {
        let path = "/api/v1/write";
        let param = &[("db", "public")];
        let username = "cnosdb";

        let client = client();

        let body = "test_v1_write_path,ta=a1,tb=b1 fa=1,fb=2";

        let resp: Response = client
            .post(path)
            .query(param)
            .basic_auth::<&str, &str>(username, None)
            .body(body)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        // lost username
        let resp: Response = client
            .post(path)
            .query(param)
            .body(body)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);

        let resp: Response = client
            .get(path)
            .query(param)
            .basic_auth::<&str, &str>(username, None)
            .body(body)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::METHOD_NOT_ALLOWED);

        let resp: Response = client
            .head(path)
            .query(param)
            .basic_auth::<&str, &str>(username, None)
            .body(body)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::METHOD_NOT_ALLOWED);

        let resp: Response = client
            .put(path)
            .query(param)
            .basic_auth::<&str, &str>(username, None)
            .body(body)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::METHOD_NOT_ALLOWED);

        let resp: Response = client
            .patch(path)
            .query(param)
            .basic_auth::<&str, &str>(username, None)
            .body(body)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::METHOD_NOT_ALLOWED);

        let resp: Response = client
            .delete(path)
            .query(param)
            .basic_auth::<&str, &str>(username, None)
            .body(body)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::METHOD_NOT_ALLOWED);
    }

    #[tokio::test]
    async fn test_v1_ping_path() {
        let path = "/api/v1/ping";

        let client = client();

        let resp: Response = client.get(path).send().await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        let resp: Response = client.head(path).send().await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        let resp: Response = client.post(path).send().await.unwrap();
        assert_eq!(resp.status(), StatusCode::METHOD_NOT_ALLOWED);

        let resp: Response = client.put(path).send().await.unwrap();
        assert_eq!(resp.status(), StatusCode::METHOD_NOT_ALLOWED);

        let resp: Response = client.patch(path).send().await.unwrap();
        assert_eq!(resp.status(), StatusCode::METHOD_NOT_ALLOWED);

        let resp: Response = client.delete(path).send().await.unwrap();
        assert_eq!(resp.status(), StatusCode::METHOD_NOT_ALLOWED);
    }
}
