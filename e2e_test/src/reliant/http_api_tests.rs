#[cfg(test)]
pub mod test {
    use fly_accept_encoding::Encoding;
    use http_protocol::blocking::{HttpClient, Response};
    use http_protocol::encoding::EncodingExt;
    use http_protocol::header::{HeaderValue, ACCEPT, CONTENT_TYPE};
    use http_protocol::status_code;
    use reqwest::header::{ACCEPT_ENCODING, CONTENT_ENCODING};

    pub fn client() -> HttpClient {
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
            .unwrap();
        assert_eq!(resp.status(), status_code::BAD_REQUEST);

        // lost auth
        let body = "select 1;";
        let resp: Response = client.post(path).query(param).body(body).send().unwrap();
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
            .unwrap();
        assert_eq!(resp.status(), status_code::BAD_REQUEST);

        // lost param
        let body = "select 1;";
        let resp: Response = client
            .post(path)
            .basic_auth::<&str, &str>(username, None)
            .body(body)
            .send()
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
            .unwrap();
        assert_eq!(resp.status(), status_code::OK);

        // lost username
        let resp: Response = client.post(path).query(param).body(body).send().unwrap();
        assert_eq!(resp.status(), status_code::BAD_REQUEST);

        // method error
        let resp: Response = client
            .get(path)
            .query(param)
            .basic_auth::<&str, &str>(username, None)
            .body(body)
            .send()
            .unwrap();
        assert_eq!(resp.status(), status_code::METHOD_NOT_ALLOWED);

        let resp: Response = client
            .head(path)
            .query(param)
            .basic_auth::<&str, &str>(username, None)
            .body(body)
            .send()
            .unwrap();
        assert_eq!(resp.status(), status_code::METHOD_NOT_ALLOWED);

        let resp: Response = client
            .put(path)
            .query(param)
            .basic_auth::<&str, &str>(username, None)
            .body(body)
            .send()
            .unwrap();
        assert_eq!(resp.status(), status_code::METHOD_NOT_ALLOWED);

        let resp: Response = client
            .patch(path)
            .query(param)
            .basic_auth::<&str, &str>(username, None)
            .body(body)
            .send()
            .unwrap();
        assert_eq!(resp.status(), status_code::METHOD_NOT_ALLOWED);

        let resp: Response = client
            .delete(path)
            .query(param)
            .basic_auth::<&str, &str>(username, None)
            .body(body)
            .send()
            .unwrap();
        assert_eq!(resp.status(), status_code::METHOD_NOT_ALLOWED);
    }

    #[tokio::test]
    async fn test_v1_ping_path() {
        let path = "/api/v1/ping";

        let client = client();

        let resp: Response = client.get(path).send().unwrap();
        assert_eq!(resp.status(), status_code::OK);

        let resp: Response = client.head(path).send().unwrap();
        assert_eq!(resp.status(), status_code::OK);

        let resp: Response = client.post(path).send().unwrap();
        assert_eq!(resp.status(), status_code::METHOD_NOT_ALLOWED);

        let resp: Response = client.put(path).send().unwrap();
        assert_eq!(resp.status(), status_code::METHOD_NOT_ALLOWED);

        let resp: Response = client.patch(path).send().unwrap();
        assert_eq!(resp.status(), status_code::METHOD_NOT_ALLOWED);

        let resp: Response = client.delete(path).send().unwrap();
        assert_eq!(resp.status(), status_code::METHOD_NOT_ALLOWED);
    }

    #[tokio::test]
    async fn test_compression() {
        async fn send_encoded_request(
            client: &HttpClient,
            path: &str,
            data: Vec<u8>,
            content_encoding: Encoding,
            accept_encoding: Encoding,
        ) -> String {
            let username = "root";
            let param = &[("db", "public")];
            let resp = client
                .post(path)
                .basic_auth::<&str, &str>(username, None)
                .query(param)
                .header(CONTENT_ENCODING, content_encoding.to_header_value())
                .header(ACCEPT_ENCODING, accept_encoding.to_header_value())
                .body(content_encoding.encode(data).unwrap())
                .send()
                .unwrap();
            String::from_utf8(
                accept_encoding
                    .decode(resp.bytes().unwrap())
                    .unwrap()
                    .to_vec(),
            )
            .unwrap()
        }

        let client = client();

        let path = "/api/v1/sql";

        for &encoding_a in Encoding::iterator() {
            for &encoding_b in Encoding::iterator() {
                let response = send_encoded_request(
                    &client,
                    path,
                    b"select 1;".to_vec(),
                    encoding_a,
                    encoding_b,
                )
                .await;
                assert_eq!(response, "Int64(1)\n1\n");
            }
        }

        let path = "/api/v1/write";

        for &encoding in Encoding::iterator() {
            send_encoded_request(
                &client,
                path,
                b"test_v1_write_path_compression,ta=a1,tb=b1 fa=1,fb=2".to_vec(),
                encoding,
                Encoding::Identity,
            )
            .await;
        }

        let path = "/api/v1/opentsdb/write";
        for &encoding in Encoding::iterator() {
            send_encoded_request(
                &client,
                path,
                b"test_v1_opentsdb_write_path_compression 1 1 ta=a1 tb=b1 fa=1 fb=2".to_vec(),
                encoding,
                Encoding::Identity,
            )
            .await;
        }
    }
}
