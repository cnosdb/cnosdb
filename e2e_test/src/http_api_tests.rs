#[cfg(test)]
mod test {
    use http_protocol::header::{HeaderValue, ACCEPT, CONTENT_TYPE};
    use http_protocol::http_client::HttpClient;
    use http_protocol::response::Response;
    use http_protocol::status_code;

    fn client() -> HttpClient {
        HttpClient::new("127.0.0.1", 8902, false, false, &[]).unwrap()
    }

    #[tokio::test]
    async fn test_v1_sql_path() {
        let path = "/api/v1/sql";
        let invalid_path = "/api/v1/xx";
        let param = &[("db", "public")];
        let username = "root";

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
        assert_eq!(resp.status(), status_code::OK);
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
        assert_eq!(resp.status(), status_code::BAD_REQUEST);

        // lost auth
        let body = "select 1;";
        let resp: Response = client
            .post(path)
            .query(param)
            .body(body)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), status_code::BAD_REQUEST);

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
        assert_eq!(resp.status(), status_code::OK);
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
        assert_eq!(resp.status(), status_code::OK);
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
        assert_eq!(resp.status(), status_code::OK);
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
        assert_eq!(resp.status(), status_code::OK);
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
        assert_eq!(resp.status(), status_code::OK);
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
        assert_eq!(resp.status(), status_code::BAD_REQUEST);

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
        assert_eq!(resp.status(), status_code::BAD_REQUEST);

        // lost param
        let body = "select 1;";
        let resp: Response = client
            .post(path)
            .basic_auth::<&str, &str>(username, None)
            .body(body)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), status_code::OK);

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
        assert_eq!(resp.status(), status_code::NOT_FOUND);

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
        assert_eq!(resp.status(), status_code::METHOD_NOT_ALLOWED);

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
        assert_eq!(resp.status(), status_code::METHOD_NOT_ALLOWED);

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
        assert_eq!(resp.status(), status_code::METHOD_NOT_ALLOWED);

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
        assert_eq!(resp.status(), status_code::METHOD_NOT_ALLOWED);

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
        assert_eq!(resp.status(), status_code::METHOD_NOT_ALLOWED);
    }

    #[tokio::test]
    async fn test_v1_write_path() {
        let path = "/api/v1/write";
        let param = &[("db", "public")];
        let username = "root";

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
        assert_eq!(resp.status(), status_code::OK);

        // lost username
        let resp: Response = client
            .post(path)
            .query(param)
            .body(body)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), status_code::BAD_REQUEST);

        // method error
        let resp: Response = client
            .get(path)
            .query(param)
            .basic_auth::<&str, &str>(username, None)
            .body(body)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), status_code::METHOD_NOT_ALLOWED);

        let resp: Response = client
            .head(path)
            .query(param)
            .basic_auth::<&str, &str>(username, None)
            .body(body)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), status_code::METHOD_NOT_ALLOWED);

        let resp: Response = client
            .put(path)
            .query(param)
            .basic_auth::<&str, &str>(username, None)
            .body(body)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), status_code::METHOD_NOT_ALLOWED);

        let resp: Response = client
            .patch(path)
            .query(param)
            .basic_auth::<&str, &str>(username, None)
            .body(body)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), status_code::METHOD_NOT_ALLOWED);

        let resp: Response = client
            .delete(path)
            .query(param)
            .basic_auth::<&str, &str>(username, None)
            .body(body)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), status_code::METHOD_NOT_ALLOWED);
    }

    #[tokio::test]
    async fn test_v1_ping_path() {
        let path = "/api/v1/ping";

        let client = client();

        let resp: Response = client.get(path).send().await.unwrap();
        assert_eq!(resp.status(), status_code::OK);

        let resp: Response = client.head(path).send().await.unwrap();
        assert_eq!(resp.status(), status_code::OK);

        let resp: Response = client.post(path).send().await.unwrap();
        assert_eq!(resp.status(), status_code::METHOD_NOT_ALLOWED);

        let resp: Response = client.put(path).send().await.unwrap();
        assert_eq!(resp.status(), status_code::METHOD_NOT_ALLOWED);

        let resp: Response = client.patch(path).send().await.unwrap();
        assert_eq!(resp.status(), status_code::METHOD_NOT_ALLOWED);

        let resp: Response = client.delete(path).send().await.unwrap();
        assert_eq!(resp.status(), status_code::METHOD_NOT_ALLOWED);
    }
}
