#[cfg(test)]
pub mod test {
    use std::fs;
    use std::io::{Read, Write};
    use std::path::PathBuf;
    use std::process::{Command, Stdio};

    use http_protocol::blocking::{HttpClient, Response};
    use http_protocol::header::{HeaderValue, ACCEPT, CONTENT_TYPE};
    use http_protocol::status_code;
    use serial_test::serial;

    use crate::utils::{
        change_config_file, clean_env, config_8902_path, start_singleton, workspace_dir,
    };

    pub fn client() -> HttpClient {
        let crate_dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let workspace_dir = crate_dir.parent().unwrap();
        let crt_path = workspace_dir
            .join("config")
            .join("tls")
            .join("ca.crt")
            .to_str()
            .unwrap()
            .to_owned();
        HttpClient::new("127.0.0.1", 8902, true, false, &[crt_path]).unwrap()
    }

    fn run_case(case: fn(PathBuf) -> ()) {
        clean_env();
        let config_8902_old = change_config_file(
            &config_8902_path(),
            vec![
                ("# [security.tls_config]", "[security.tls_config]"),
                (
                    "# certificate = \"./config/tls/server.crt\"",
                    "certificate = \"./config/tls/server.crt\"",
                ),
                (
                    "# private_key = \"./config/tls/server.key\"",
                    "private_key = \"./config/tls/server.key\"",
                ),
            ],
        );
        let data = start_singleton();

        case(workspace_dir());

        drop(data);
        fs::write(config_8902_path(), config_8902_old).unwrap();
        clean_env();
    }

    #[test]
    #[serial]
    fn test_v1_sql_path() {
        fn case(_: PathBuf) {
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
        run_case(case)
    }

    #[test]
    #[serial]
    fn test_v1_write_path() {
        fn case(_: PathBuf) {
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

        run_case(case)
    }

    #[test]
    #[serial]
    fn test_v1_ping_path() {
        fn case(_: PathBuf) {
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

        run_case(case);
    }

    #[test]
    #[serial]
    fn test_cli_connection() {
        fn case(workspace: PathBuf) {
            let mut child = Command::new("cargo")
                .current_dir(&workspace)
                .args([
                    "run",
                    "--package",
                    "client",
                    "--bin",
                    "cnosdb-cli",
                    "--",
                    "--ssl",
                    "--cacert",
                    &*workspace
                        .join("config")
                        .join("tls")
                        .join("ca.crt")
                        .to_string_lossy(),
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

        run_case(case);
    }
}
