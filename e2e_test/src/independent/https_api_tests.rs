#![cfg(test)]

use std::ffi::OsStr;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use http_protocol::header::{HeaderValue, ACCEPT, CONTENT_TYPE};
use reqwest::{Method, StatusCode};
use serial_test::serial;

use crate::case::{CnosdbRequest, E2eExecutor, Step};
use crate::utils::{
    build_data_node_config, copy_cnosdb_server_certificate, get_workspace_dir, kill_all,
    run_singleton, Client,
};
use crate::{check_response, cluster_def, headers, E2eError};

fn run_case_with_tls(
    test_dir: &str,
    case_name: &str,
    case: fn(workspace_dir: PathBuf, client: &Client),
) {
    kill_all();

    println!("[{case_name}]: Preparing test_directory '{test_dir}'");
    let test_dir = Path::new(test_dir);
    let _ = std::fs::remove_dir_all(test_dir);
    std::fs::create_dir_all(test_dir).unwrap();
    let cnosdb_dir = test_dir.join("data");

    let workspace_dir = get_workspace_dir();
    let test_dir_tls = cnosdb_dir.join("config").join("tls");
    println!(
        "[{case_name}]: Copying tls files to '{}'",
        test_dir_tls.display()
    );
    copy_cnosdb_server_certificate(&workspace_dir, &test_dir_tls);

    let data_node_def = &cluster_def::one_data(1);

    let mut config = build_data_node_config(test_dir, &data_node_def.config_file_name);
    data_node_def.update_config(&mut config);
    config.security.tls_config = Some(config::tskv::TLSConfig {
        certificate: test_dir_tls
            .join("server.crt")
            .to_string_lossy()
            .to_string(),
        private_key: test_dir_tls
            .join("server.key")
            .to_string_lossy()
            .to_string(),
    });

    let config_dir = cnosdb_dir.join("config");
    std::fs::create_dir_all(&config_dir).unwrap();
    let config_file_path = config_dir.join(&data_node_def.config_file_name);
    println!(
        "[{case_name}]: saving new config_file to '{}'.",
        config_file_path.display()
    );
    std::fs::write(&config_file_path, config.to_string_pretty()).unwrap();
    println!(
        "[{case_name}]: saved new config_file to '{}'.",
        config_file_path.display()
    );

    println!("[{case_name}]: Starting cnosdb singleton");
    let data = run_singleton(test_dir, data_node_def, true, false);

    println!("[{case_name}]: Running test");
    case(workspace_dir, &data.client);
}

#[test]
#[serial]
fn test_v1_sql_path() {
    run_case_with_tls(
        "/tmp/e2e_test/http_api_tests/test_v1_sql_path",
        "test_v1_sql_path",
        case,
    );

    fn case(_: PathBuf, client: &Client) {
        let url = "https://127.0.0.1:8902/api/v1/sql?db=public";
        let url_no_param = "https://127.0.0.1:8902/api/v1/sql";
        let url_invalid_path = "https://127.0.0.1:8902/api/v1/xx";

        let body = "select 1;";
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
}

#[test]
#[serial]
fn test_v1_write_path() {
    run_case_with_tls(
        "/tmp/e2e_test/http_api_tests/test_v1_write_path",
        "test_v1_write_path",
        case,
    );

    fn case(_: PathBuf, client: &Client) {
        let url = "https://127.0.0.1:8902/api/v1/write?db=public";
        let body = "test_v1_write_path,ta=a1,tb=b1 fa=1,fb=2";

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
}

#[test]
#[serial]
fn test_v1_ping_path() {
    run_case_with_tls(
        "/tmp/e2e_test/https_api_tests/test_v1_ping_path",
        "test_v1_ping_path",
        case,
    );

    fn case(_: PathBuf, client: &Client) {
        let url: &str = "https://127.0.0.1:8902/api/v1/ping";

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
}

#[test]
#[serial]
fn test_cli_connection() {
    run_case_with_tls(
        "/tmp/e2e_test/http_api_tests/test_cli_connection",
        "test_cli_connection",
        case,
    );

    fn case(workspace_dir: PathBuf, _client: &Client) {
        let tls_path = workspace_dir.join("config").join("tls").join("ca.crt");
        let mut child = Command::new("cargo")
            .current_dir(&workspace_dir)
            .args([
                OsStr::new("run"),
                OsStr::new("--package"),
                OsStr::new("client"),
                OsStr::new("--bin"),
                OsStr::new("cnosdb-cli"),
                OsStr::new("--"),
                OsStr::new("--ssl"),
                OsStr::new("--cacert"),
                tls_path.as_os_str(),
            ])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .unwrap();
        let mut child_stdout = child.stdout.take().unwrap();
        let mut child_stdin = child.stdin.take().unwrap();
        writeln!(&mut child_stdin, "select 10086;").unwrap();
        writeln!(&mut child_stdin, "\\q").unwrap();
        child.wait().unwrap();
        let mut buf_string = String::new();
        child_stdout.read_to_string(&mut buf_string).unwrap();
        assert!(buf_string.contains("10086"));
    }
}

#[test]
#[serial]
fn es_api_test() {
    let executor =
        E2eExecutor::new_singleton("api_router_tests", "es_api_test", cluster_def::one_data(1));

    executor.execute_steps(&[
        Step::CnosdbRequest {
            req: CnosdbRequest::Write {
                url: "http://127.0.0.1:8902/api/v1/es/_bulk", 
                req: r#"{"create":{}}
                {"msg":"test"}"#,
                resp: Err(E2eError::Api {
                    status: StatusCode::UNPROCESSABLE_ENTITY,
                    url: None,
                    req: None,
                    resp: Some(r#"{"error_code":"040016","error_message":"Error parsing log message: table param is None"}"#.to_string())
                })
            },
            auth: None,
        },
        Step::CnosdbRequest { // no time_column, no tag_columns
            req: CnosdbRequest::Write {
                url: "http://127.0.0.1:8902/api/v1/es/_bulk?table=test1", 
                req: r#"{"create":{}}
                {"time":"2024-03-27T02:51:11.687Z", "name":"asd", "sex": "man", "msg":"test", "int1":10, "int2":-10, "float1":10.5, "float2":-10.5, "flag":false}"#,
                resp: Ok("".to_string()),
            },
            auth: None,
        },
        Step::CnosdbRequest { // no time_column, one tag_columns
            req: CnosdbRequest::Write {
                url: "http://127.0.0.1:8902/api/v1/es/_bulk?table=test2&tag_columns=name", 
                req: r#"{"create":{}}
                {"time":"2024-03-27T02:51:11.687Z", "name":"asd", "sex": "man", "msg":"test", "int1":10, "int2":-10, "float1":10.5, "float2":-10.5, "flag":false}"#,
                resp: Ok("".to_string()),
            },
            auth: None,
        },
        Step::CnosdbRequest { // no time_column, multi tag_columns
            req: CnosdbRequest::Write {
                url: "http://127.0.0.1:8902/api/v1/es/_bulk?table=test3&tag_columns=name,sex", 
                req: r#"{"create":{}}
                {"time":"2024-03-27T02:51:11.687Z", "name":"asd", "sex": "man", "msg":"test", "int1":10, "int2":-10, "float1":10.5, "float2":-10.5, "flag":false}"#,
                resp: Ok("".to_string()),
            },
            auth: None,
        },
        Step::CnosdbRequest { // time_column, multi tag_columns
            req: CnosdbRequest::Write {
                url: "http://127.0.0.1:8902/api/v1/es/_bulk?table=test4&time_column=date&tag_columns=name,sex", 
                req: r#"{"create":{}}
                {"date":"2024-03-27T02:51:11.687Z", "name":"asd", "sex": "man", "msg":"test", "int1":10, "int2":-10, "float1":10.5, "float2":-10.5, "flag":false}"#,
                resp: Ok("".to_string()),
            },
            auth: None,
        },
        Step::CnosdbRequest { // no time, no tag_columns
            req: CnosdbRequest::Write {
                url: "http://127.0.0.1:8902/api/v1/es/_bulk?table=test5", 
                req: r#"{"create":{}}
                {"name":"asd", "sex": "man", "msg":"test", "int1":10, "int2":-10, "float1":10.5, "float2":-10.5, "flag":false}"#,
                resp: Ok("".to_string()),
            },
            auth: None,
        },
        Step::CnosdbRequest { // no time, multi tag_columns
            req: CnosdbRequest::Write {
                url: "http://127.0.0.1:8902/api/v1/es/_bulk?table=test6&tag_columns=name,sex", 
                req: r#"{"create":{}}
                {"name":"asd", "sex": "man", "msg":"test", "int1":10, "int2":-10, "float1":10.5, "float2":-10.5, "flag":false}"#,
                resp: Ok("".to_string()),
            },
            auth: None,
        },
        Step::CnosdbRequest { // nest json, multi tag_columns
            req: CnosdbRequest::Write {
                url: "http://127.0.0.1:8902/api/v1/es/_bulk?table=test7&tag_columns=name,age,is_student", 
                req: r#"{"create":{}}
                {"time": "2021-01-01T00:00:00Z","name": "John Doe","age": 43,"is_student": false,"address": {"street": "123 Main Street","city": "Springfield","state": "IL","zip": {"code": 62701}},"children": [{"name": "Jane Doe","age": 7,"is_student": {"grade": 2},"shoes": [{"brand": "Nike","size": 7},{"brand": "Adidas","size": 8}]},{"name": "Dave Doe","age": 12}]}"#,
                resp: Ok("".to_string()),
            },
            auth: None,
        },
        Step::CnosdbRequest { // create and index
            req: CnosdbRequest::Write {
                url: "http://127.0.0.1:8902/api/v1/es/_bulk?table=test8", 
                req: r#"{"create":{}}
                {"time":"2024-03-27T02:51:11.687Z", "name":"asd", "sex": "man", "msg":"test", "int1":10, "int2":-10, "float1":10.5, "float2":-10.5, "flag":false}
                {"index":{}}
                {"time":"2024-04-27T02:51:11.687Z", "name":"asd", "sex": "man", "msg":"test", "int1":10, "int2":-10, "float1":10.5, "float2":-10.5, "flag":false}"#,
                resp: Ok("".to_string()),
            },
            auth: None,
        },
        Step::CnosdbRequest { // index and create
            req: CnosdbRequest::Write {
                url: "http://127.0.0.1:8902/api/v1/es/_bulk?table=test9", 
                req: r#"{"index":{}}
                {"time":"2024-03-27T02:51:11.687Z", "name":"asd", "sex": "man", "msg":"test", "int1":10, "int2":-10, "float1":10.5, "float2":-10.5, "flag":false}
                {"create":{}}
                {"time":"2024-04-27T02:51:11.687Z", "name":"asd", "sex": "man", "msg":"test", "int1":10, "int2":-10, "float1":10.5, "float2":-10.5, "flag":false}"#,
                resp: Ok("".to_string()),
            },
            auth: None,
        },
        Step::CnosdbRequest { // create and create
            req: CnosdbRequest::Write {
                url: "http://127.0.0.1:8902/api/v1/es/_bulk?table=test10", 
                req: r#"{"create":{}}
                {"time":"2024-03-27T02:51:11.687Z", "name":"asd", "sex": "man", "msg":"test", "int1":10, "int2":-10, "float1":10.5, "float2":-10.5, "flag":false}
                {"create":{}}
                {"time":"2024-04-27T02:51:11.687Z", "name":"asd", "sex": "man", "msg":"test", "int1":10, "int2":-10, "float1":10.5, "float2":-10.5, "flag":false}"#,
                resp: Ok("".to_string()),
            },
            auth: None,
        },
        Step::CnosdbRequest { // index and index
            req: CnosdbRequest::Write {
                url: "http://127.0.0.1:8902/api/v1/es/_bulk?table=test11", 
                req: r#"{"index":{}}
                {"time":"2024-03-27T02:51:11.687Z", "name":"asd", "sex": "man", "msg":"test", "int1":10, "int2":-10, "float1":10.5, "float2":-10.5, "flag":false}
                {"index":{}}
                {"time":"2024-04-27T02:51:11.687Z", "name":"asd", "sex": "man", "msg":"test", "int1":10, "int2":-10, "float1":10.5, "float2":-10.5, "flag":false}"#,
                resp: Ok("".to_string()),
            },
            auth: None,
        },
    ])
}
