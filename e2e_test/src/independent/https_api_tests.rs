use std::ffi::OsStr;
use std::io::{Read, Write};
use std::path::PathBuf;
use std::process::{Command, Stdio};

use http_protocol::header::{HeaderValue, ACCEPT, CONTENT_TYPE};
use reqwest::{Method, StatusCode};

use crate::case::step::{EsBulk, RequestStep, StepPtr};
use crate::case::E2eExecutor;
use crate::utils::get_workspace_dir;
use crate::utils::global::E2eContext;
use crate::{check_response, cluster_def, headers, E2eError};

fn run_case_with_tls(mut e2e_context: E2eContext, case: fn(_: PathBuf, _: &mut E2eExecutor)) {
    let mut executor = {
        let mut cluster_definition = cluster_def::one_data(1);
        cluster_definition.data_cluster_def[0].enable_tls = true;
        e2e_context.build_executor(cluster_definition)
    };
    let test_dir = executor.case_context().test_dir();
    let workspace_dir = get_workspace_dir();

    println!(
        "[{}]: Preparing test_directory '{}'",
        executor.case_context().case_name(),
        test_dir.display(),
    );

    let tls_dir = workspace_dir.join("config").join("tls");
    let crt_path = tls_dir.join("server.crt");
    let key_path = tls_dir.join("server.key");

    println!(
        "[{}]: Starting cnosdb singleton",
        executor.case_context().case_name()
    );
    executor.set_update_data_config_fn_vec(vec![Some(Box::new(move |config| {
        config.security.tls_config = Some(config::tskv::TLSConfig {
            certificate: crt_path.to_string_lossy().to_string(),
            private_key: key_path.to_string_lossy().to_string(),
        });
    }))]);
    executor.startup();

    println!("[{}]: Running test", executor.case_context().case_name());
    case(workspace_dir, &mut executor);
}

#[test]
fn test_v1_sql_path() {
    let ctx = E2eContext::new("https_api_tests", "test_v1_sql_path");
    run_case_with_tls(ctx, case);

    fn case(_: PathBuf, executor: &mut E2eExecutor) {
        let ctx = executor.case_context();
        let host_port = ctx.cluster_definition().data_cluster_def[0].http_host_port;
        let url = &format!("https://{host_port}/api/v1/sql?db=public");
        let url_no_param = &format!("https://{host_port}/api/v1/sql");
        let url_invalid_path = &format!("https://{host_port}/api/v1/xx");
        let client = ctx.data_client(0);

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
fn test_v1_write_path() {
    let ctx = E2eContext::new("https_api_tests", "test_v1_write_path");
    run_case_with_tls(ctx, case);

    fn case(_: PathBuf, executor: &mut E2eExecutor) {
        let ctx = executor.case_context();
        let host_port = ctx.cluster_definition().data_cluster_def[0].http_host_port;
        let url = &format!("https://{host_port}/api/v1/write?db=public");
        let client = ctx.data_client(0);

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
fn test_v1_ping_path() {
    let ctx = E2eContext::new("https_api_tests", "test_v1_ping_path");
    run_case_with_tls(ctx, case);

    fn case(_: PathBuf, executor: &mut E2eExecutor) {
        let ctx = executor.case_context();
        let host_port = ctx.cluster_definition().data_cluster_def[0].http_host_port;
        let url = &format!("https://{host_port}/api/v1/ping");
        let client = ctx.data_client(0);

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
fn test_cli_connection() {
    let ctx = E2eContext::new("https_api_tests", "test_cli_connection");
    run_case_with_tls(ctx, case);

    fn case(workspace_dir: PathBuf, executor: &mut E2eExecutor) {
        let tls_path = workspace_dir.join("config").join("tls").join("ca.crt");
        let port = executor.cluster_definition().data_cluster_def[0]
            .http_host_port
            .port();
        let mut child = Command::new("cargo")
            .current_dir(&workspace_dir)
            .args([
                OsStr::new("run"),
                OsStr::new("--package"),
                OsStr::new("client"),
                OsStr::new("--bin"),
                OsStr::new("cnosdb-cli"),
                OsStr::new("--"),
                OsStr::new("--port"),
                OsStr::new(port.to_string().as_str()),
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
fn es_api_test() {
    let mut ctx = E2eContext::new("https_api_tests", "es_api_test");
    let mut executor = ctx.build_executor(cluster_def::one_data(1));
    let http_addr = executor.cluster_definition().data_cluster_def[0].http_host_port;

    let steps: Vec<StepPtr> = vec![
        RequestStep::new_boxed(
            "_bulk 1",
            EsBulk::build_request_with_str(
                format!("http://{http_addr}/api/v1/es/_bulk"),
                r#"{"create":{}}
                {"msg":"test"}"#,
                Err(E2eError::Api {
                    status: StatusCode::UNPROCESSABLE_ENTITY,
                    url: None,
                    req: None,
                    resp: Some(r#"{"error_code":"040016","error_message":"Error parsing log message: table param is None"}"#.to_string())
                }),
            ),
            None,
            None,
        ),
        RequestStep::new_boxed(
            "_bulk 2",
            EsBulk::build_request_with_str(
                format!("http://{http_addr}/api/v1/es/_bulk?table=test1"),
                r#"{"create":{}}
                {"time":"2024-03-27T02:51:11.687Z", "name":"asd", "sex": "man", "msg":"test", "int1":10, "int2":-10, "float1":10.5, "float2":-10.5, "flag":false}"#,
                Ok("".to_string())
            ),
            None,
            None
        ),
        RequestStep::new_boxed(
            "_bulk no time_column, one tag_columns",
            EsBulk::build_request_with_str(
                format!("http://{http_addr}/api/v1/es/_bulk?table=test2&tag_columns=name"),
                r#"{"create":{}}
                {"time":"2024-03-27T02:51:11.687Z", "name":"asd", "sex": "man", "msg":"test", "int1":10, "int2":-10, "float1":10.5, "float2":-10.5, "flag":false}"#,
                Ok("".to_string())
            ),
            None,
            None
        ),
        RequestStep::new_boxed(
            "_bulk no time_column, multi tag_columns",
            EsBulk::build_request_with_str(
                format!("http://{http_addr}/api/v1/es/_bulk?table=test3&tag_columns=name,sex"),
                r#"{"create":{}}
                {"time":"2024-03-27T02:51:11.687Z", "name":"asd", "sex": "man", "msg":"test", "int1":10, "int2":-10, "float1":10.5, "float2":-10.5, "flag":false}"#,
                Ok("".to_string())
            ),
            None,
            None
        ),
        RequestStep::new_boxed(
            "_bulk time_column, multi tag_columns",
            EsBulk::build_request_with_str(
                format!("http://{http_addr}/api/v1/es/_bulk?table=test4&time_column=date&tag_columns=name,sex"),
                r#"{"create":{}}
                {"date":"2024-03-27T02:51:11.687Z", "name":"asd", "sex": "man", "msg":"test", "int1":10, "int2":-10, "float1":10.5, "float2":-10.5, "flag":false}"#,
                Ok("".to_string())
            ),
            None,
            None
        ),
        RequestStep::new_boxed(
            "_bulk no time, no tag_columns",
            EsBulk::build_request_with_str(
                format!("http://{http_addr}/api/v1/es/_bulk?table=test5"),
                r#"{"create":{}}
                {"name":"asd", "sex": "man", "msg":"test", "int1":10, "int2":-10, "float1":10.5, "float2":-10.5, "flag":false}"#,
                Ok("".to_string())
            ),
            None,
            None
        ),
        RequestStep::new_boxed(
            "_bulk no time, multi tag_columns",
            EsBulk::build_request_with_str(
                format!("http://{http_addr}/api/v1/es/_bulk?table=test6&tag_columns=name,sex"),
                r#"{"create":{}}
                {"name":"asd", "sex": "man", "msg":"test", "int1":10, "int2":-10, "float1":10.5, "float2":-10.5, "flag":false}"#,
                Ok("".to_string())
            ),
            None,
            None
        ),
        RequestStep::new_boxed(
            "_bulk nest json, multi tag_columns",
            EsBulk::build_request_with_str(
                format!("http://{http_addr}/api/v1/es/_bulk?table=test7&tag_columns=name,age,is_student"),
                r#"{"create":{}}
                {"time": "2021-01-01T00:00:00Z","name": "John Doe","age": 43,"is_student": false,"address": {"street": "123 Main Street","city": "Springfield","state": "IL","zip": {"code": 62701}},"children": [{"name": "Jane Doe","age": 7,"is_student": {"grade": 2},"shoes": [{"brand": "Nike","size": 7},{"brand": "Adidas","size": 8}]},{"name": "Dave Doe","age": 12}]}"#,
                Ok("".to_string())
            ),
            None,
            None
        ),
        RequestStep::new_boxed(
            "_bulk create and index",
            EsBulk::build_request_with_str(
                format!("http://{http_addr}/api/v1/es/_bulk?table=test8"),
                r#"{"create":{}}
                {"time":"2024-03-27T02:51:11.687Z", "name":"asd", "sex": "man", "msg":"test", "int1":10, "int2":-10, "float1":10.5, "float2":-10.5, "flag":false}
                {"index":{}}
                {"time":"2024-04-27T02:51:11.687Z", "name":"asd", "sex": "man", "msg":"test", "int1":10, "int2":-10, "float1":10.5, "float2":-10.5, "flag":false}"#,
                Ok("".to_string())
            ),
            None,
            None
        ),
        RequestStep::new_boxed(
            "_bulk index and create",
            EsBulk::build_request_with_str(
                format!("http://{http_addr}/api/v1/es/_bulk?table=test9"),
                r#"{"index":{}}
                {"time":"2024-03-27T02:51:11.687Z", "name":"asd", "sex": "man", "msg":"test", "int1":10, "int2":-10, "float1":10.5, "float2":-10.5, "flag":false}
                {"create":{}}
                {"time":"2024-04-27T02:51:11.687Z", "name":"asd", "sex": "man", "msg":"test", "int1":10, "int2":-10, "float1":10.5, "float2":-10.5, "flag":false}"#,
                Ok("".to_string())
            ),
            None,
            None
        ),
        RequestStep::new_boxed(
            "_bulk create and create",
            EsBulk::build_request_with_str(
                format!("http://{http_addr}/api/v1/es/_bulk?table=test10"),
                r#"{"create":{}}
                {"time":"2024-03-27T02:51:11.687Z", "name":"asd", "sex": "man", "msg":"test", "int1":10, "int2":-10, "float1":10.5, "float2":-10.5, "flag":false}
                {"create":{}}
                {"time":"2024-04-27T02:51:11.687Z", "name":"asd", "sex": "man", "msg":"test", "int1":10, "int2":-10, "float1":10.5, "float2":-10.5, "flag":false}"#,
                Ok("".to_string())
            ),
            None,
            None
        ),
        RequestStep::new_boxed(
            "_bulk index and index",
            EsBulk::build_request_with_str(
                format!("http://{http_addr}/api/v1/es/_bulk?table=test11"),
                r#"{"index":{}}
                {"time":"2024-03-27T02:51:11.687Z", "name":"asd", "sex": "man", "msg":"test", "int1":10, "int2":-10, "float1":10.5, "float2":-10.5, "flag":false}
                {"index":{}}
                {"time":"2024-04-27T02:51:11.687Z", "name":"asd", "sex": "man", "msg":"test", "int1":10, "int2":-10, "float1":10.5, "float2":-10.5, "flag":false}"#,
                Ok("".to_string())
            ),
            None,
            None
        ),
    ];
    executor.execute_steps(&steps);
}
