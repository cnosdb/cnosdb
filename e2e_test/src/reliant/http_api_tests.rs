#![cfg(test)]

use http_protocol::encoding::Encoding;
use http_protocol::header::{HeaderValue, ACCEPT, CONTENT_TYPE};
use reqwest::header::{ACCEPT_ENCODING, CONTENT_ENCODING};
use reqwest::{Method, StatusCode};

use crate::utils::Client;
use crate::{check_response, headers, E2eResult};

#[test]
fn test_v1_sql_path() {
    let url = "http://127.0.0.1:8902/api/v1/sql?db=public";
    let url_no_param = "http://127.0.0.1:8902/api/v1/sql";
    let url_invalid_path = "http://127.0.0.1:8902/api/v1/xx";

    let body = "select 1;";
    let client = Client::with_auth("root".to_string(), None);

    let resp = check_response!(client.post(url, body));
    assert_eq!(
        resp.headers().get(CONTENT_TYPE).unwrap(),
        &HeaderValue::from_static("text/csv")
    );

    // invalid basic auth
    let req = client
        .request(Method::POST, url)
        .bearer_auth(client.auth())
        .body(body);
    let resp = client.execute(req.build().unwrap()).unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);

    // lost auth
    let req = client.request(Method::POST, url).body(body);
    let resp = client.execute(req.build().unwrap()).unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);

    // accept: text/csv
    let mut req = client.request_with_auth(Method::POST, url).body(body);
    req = req.headers(headers! {
        ACCEPT.as_str() => "text/csv"
    });
    let resp = check_response!(client.execute(req.build().unwrap()));
    assert_eq!(
        resp.headers().get(CONTENT_TYPE).unwrap(),
        &HeaderValue::from_static("text/csv")
    );

    // accept: application/json
    let mut req = client.request_with_auth(Method::POST, url).body(body);
    req = req.headers(headers! {
        ACCEPT.as_str() => "application/json"
    });
    let resp = check_response!(client.execute(req.build().unwrap()));
    assert_eq!(
        resp.headers().get(CONTENT_TYPE).unwrap(),
        &HeaderValue::from_static("application/json")
    );

    // accept: application/nd-json
    let mut req = client.request_with_auth(Method::POST, url).body(body);
    req = req.headers(headers! {
        ACCEPT.as_str() => "application/nd-json"
    });
    let resp = check_response!(client.execute(req.build().unwrap()));
    assert_eq!(
        resp.headers().get(CONTENT_TYPE).unwrap(),
        &HeaderValue::from_static("application/nd-json")
    );

    // accept: application/*
    let mut req = client.request_with_auth(Method::POST, url).body(body);
    req = req.headers(headers! {
        ACCEPT.as_str() => "application/*"
    });
    let resp = check_response!(client.execute(req.build().unwrap()));
    assert_eq!(
        resp.headers().get(CONTENT_TYPE).unwrap(),
        &HeaderValue::from_static("text/csv")
    );

    // accept: */*
    let mut req = client.request_with_auth(Method::POST, url).body(body);
    req = req.headers(headers! {
        ACCEPT.as_str() => "*/*"
    });
    let resp = check_response!(client.execute(req.build().unwrap()));
    assert_eq!(
        resp.headers().get(CONTENT_TYPE).unwrap(),
        &HeaderValue::from_static("text/csv")
    );

    // accept: *
    let mut req = client.request_with_auth(Method::POST, url).body(body);
    req = req.headers(headers! {
        ACCEPT.as_str() => "*"
    });
    let resp = client.execute(req.build().unwrap()).unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);

    // accept: xx
    let mut req = client.request_with_auth(Method::POST, url).body(body);
    req = req.headers(headers! {
        ACCEPT.as_str() => "xx"
    });
    let resp = client.execute(req.build().unwrap()).unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);

    // lost param
    let resp = client.post(url_no_param, body).unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    // path not found
    let resp = client.post(url_invalid_path, body).unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);

    // GET
    let resp = client.get(url, body).unwrap();
    assert_eq!(resp.status(), StatusCode::METHOD_NOT_ALLOWED);

    // PUT
    let resp = client.put(url, body).unwrap();
    assert_eq!(resp.status(), StatusCode::METHOD_NOT_ALLOWED);

    // PATCH
    let resp = client.patch(url, body).unwrap();
    assert_eq!(resp.status(), StatusCode::METHOD_NOT_ALLOWED);

    // DELETE
    let resp = client.delete(url, body).unwrap();
    assert_eq!(resp.status(), StatusCode::METHOD_NOT_ALLOWED);

    // HEAD
    let resp = client.head(url, body).unwrap();
    assert_eq!(resp.status(), StatusCode::METHOD_NOT_ALLOWED);
}

#[test]
fn test_v1_write_path() {
    let url = "http://127.0.0.1:8902/api/v1/write?db=public";
    let body = "test_v1_write_path,ta=a1,tb=b1 fa=1,fb=2";
    let client = Client::with_auth("root".to_string(), None);

    check_response!(client.post(url, body));

    // lost username
    let req = client.request(Method::POST, url).body(body);
    let resp = client.execute(req.build().unwrap()).unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);

    // GET
    let resp = client.get(url, body).unwrap();
    assert_eq!(resp.status(), StatusCode::METHOD_NOT_ALLOWED);

    // PUT
    let resp = client.put(url, body).unwrap();
    assert_eq!(resp.status(), StatusCode::METHOD_NOT_ALLOWED);

    // PATCH
    let resp = client.patch(url, body).unwrap();
    assert_eq!(resp.status(), StatusCode::METHOD_NOT_ALLOWED);

    // DELETE
    let resp = client.delete(url, body).unwrap();
    assert_eq!(resp.status(), StatusCode::METHOD_NOT_ALLOWED);

    // HEAD
    let resp = client.head(url, body).unwrap();
    assert_eq!(resp.status(), StatusCode::METHOD_NOT_ALLOWED);
}

#[test]
fn test_v1_ping_path() {
    let url: &str = "http://127.0.0.1:8902/api/v1/ping";
    let client = Client::with_auth("root".to_string(), None);

    check_response!(client.get(url, ""));

    check_response!(client.head(url, ""));

    let resp = client.post(url, "").unwrap();
    assert_eq!(resp.status(), StatusCode::METHOD_NOT_ALLOWED);

    let resp = client.put(url, "").unwrap();
    assert_eq!(resp.status(), StatusCode::METHOD_NOT_ALLOWED);

    let resp = client.patch(url, "").unwrap();
    assert_eq!(resp.status(), StatusCode::METHOD_NOT_ALLOWED);

    let resp = client.delete(url, "").unwrap();
    assert_eq!(resp.status(), StatusCode::METHOD_NOT_ALLOWED);
}

#[test]
fn test_compression() {
    fn send_encoded_request(
        client: &Client,
        url: &str,
        body: Vec<u8>,
        content_encoding: Encoding,
        accept_encoding: Encoding,
    ) -> E2eResult<reqwest::blocking::Response> {
        client
            .request_with_auth(Method::POST, url)
            .body(body)
            .header(CONTENT_ENCODING, content_encoding.to_header_value())
            .header(ACCEPT_ENCODING, accept_encoding.to_header_value())
            .send()
            .map_err(|e| Client::map_reqwest_err(e, url, "<encoded_data>"))
    }

    let client = Client::with_auth("root".to_string(), None);

    let url_cnosdb_public = "http://127.0.0.1:8902/api/v1/sql?db=public";

    let _ = client
        .api_v1_sql(
            url_cnosdb_public,
            "CREATE DATABASE IF NOT EXISTS e2e_test_compression",
        )
        .unwrap();

    {
        let url = "http://127.0.0.1:8902/api/v1/sql?db=e2e_test_compression";
        let body = b"select 1;".to_vec();
        for req_encoding in Encoding::iterator() {
            for resp_encoding in Encoding::iterator() {
                let req_bytes_enc = req_encoding.encode(body.clone()).unwrap();
                let req_bytes_dec = req_encoding.decode(req_bytes_enc.clone().into()).unwrap();
                assert_eq!(body, req_bytes_dec.to_vec());
                let resp =
                    send_encoded_request(&client, url, req_bytes_enc, req_encoding, resp_encoding)
                        .unwrap();
                if !resp.status().is_success() {
                    panic!(
                        "req_encoding: {:?}, resp_encoding: {:?}, resp: {:?}",
                        req_encoding, resp_encoding, resp
                    );
                }
                let resp_bytes_enc = resp.bytes().unwrap();
                let resp_bytes_dec = resp_encoding.decode(resp_bytes_enc).unwrap();
                let resp_text = String::from_utf8(resp_bytes_dec.to_vec()).unwrap();
                assert_eq!(
                    resp_text, "Int64(1)\n1\n",
                    "req_encoding: {:?}, resp_encoding: {:?}",
                    req_encoding, resp_encoding
                );
            }
        }
    }

    {
        let url = "http://127.0.0.1:8902/api/v1/write?db=e2e_test_compression";
        let body = b"test_v1_write_path_compression,ta=a1,tb=b1 fa=1,fb=2".to_vec();
        for encoding in Encoding::iterator() {
            let req_bytes_enc = encoding.encode(body.clone()).unwrap();
            let req_bytes_dec = encoding.decode(req_bytes_enc.clone().into()).unwrap();
            assert_eq!(body, req_bytes_dec.to_vec());
            check_response!(send_encoded_request(
                &client,
                url,
                req_bytes_enc,
                encoding,
                Encoding::Identity
            ));
        }
    }

    {
        let url = "http://127.0.0.1:8902/api/v1/opentsdb/write?db=e2e_test_compression";
        let body = b"test_v1_opentsdb_write_path_compression 1 1 ta=a1 tb=b1 fa=1 fb=2".to_vec();
        for encoding in Encoding::iterator() {
            let req_bytes_enc = encoding.encode(body.clone()).unwrap();
            let req_bytes_dec = encoding.decode(req_bytes_enc.clone().into()).unwrap();
            assert_eq!(body, req_bytes_dec.to_vec());
            check_response!(send_encoded_request(
                &client,
                url,
                req_bytes_enc,
                encoding,
                Encoding::Identity
            ));
        }
    }
}
