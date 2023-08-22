#[cfg(test)]
pub mod test {
    use std::collections::HashMap;
    use std::ffi::OsStr;
    use std::io::{Read, Write};
    use std::path::{Path, PathBuf};
    use std::process::{Child, Command, Stdio};
    use std::time::{Duration, Instant};

    use futures::Future;
    use http_protocol::header::{HeaderValue, ACCEPT, CONTENT_TYPE};
    use http_protocol::http_client::HttpClient;
    use http_protocol::response::Response;
    use http_protocol::status_code;
    use serial_test::serial;
    use sysinfo::{ProcessExt, System, SystemExt};

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
        println!("{}", crt_path);
        HttpClient::new("127.0.0.1", 8902, true, false, &[crt_path]).unwrap()
    }

    pub struct CnosdbData {
        pub(crate) workspace: PathBuf,
        pub(crate) last_build: Option<Instant>,
        pub(crate) exe_path: PathBuf,
        pub(crate) config_dir: PathBuf,
        pub(crate) sub_processes: HashMap<String, Child>,
    }

    impl CnosdbData {
        pub fn new(workspace_dir: impl AsRef<Path>) -> Self {
            let workspace = workspace_dir.as_ref().to_path_buf();
            Self {
                workspace: workspace.clone(),
                last_build: None,
                exe_path: workspace.join("target").join("debug").join("cnosdb"),
                config_dir: workspace.join("config"),
                sub_processes: HashMap::with_capacity(2),
            }
        }

        pub async fn run(&mut self) {
            println!("Runing cnosdb cluster at '{}'", self.workspace.display());
            println!("- Building cnosdb");
            self.build().await;
            println!("- Running cnosdb with config 'config_8902.toml'");
            self.execute("config_8902.toml").await;

            self.wait_startup().await;
        }

        async fn build(&mut self) {
            let mut cargo_build = Command::new("cargo");
            let output = cargo_build
                .current_dir(&self.workspace)
                .args(["build", "--package", "main", "--bin", "cnosdb"])
                .output()
                .expect("failed to execute cargo build");
            if !output.status.success() {
                let message = format!(
                    "Failed to build cnosdb: stdout: {}, stderr: {}",
                    String::from_utf8_lossy(&output.stdout),
                    String::from_utf8_lossy(&output.stderr)
                );
                panic!("Failed to build cnosdb-meta: {message}");
            }
            self.last_build = Some(Instant::now());
        }

        // a. nohup ./target/debug/cnosdb run --config ./config/config_8902.toml -M singleton &
        async fn execute(&mut self, config_file: &str) {
            let config_path = self.config_dir.join(config_file);
            let proc = Command::new(&self.exe_path)
                .args([
                    OsStr::new("run"),
                    OsStr::new("--config"),
                    config_path.as_os_str(),
                    OsStr::new("-M"),
                    OsStr::new("singleton"),
                ])
                .stderr(Stdio::null())
                .stdout(Stdio::null())
                .spawn()
                .expect("failed to execute cnosdb");
            self.sub_processes.insert(config_file.to_string(), proc);
        }

        /// Wait cnosdb startup by checking ping api in loop
        async fn wait_startup(&self) {
            let ping_api = "/api/v1/ping";
            let startup_time = std::time::Instant::now();
            let client = client();
            loop {
                tokio::time::sleep(Duration::from_secs(3)).await;

                if let Err(e) = client.get(&ping_api).send().await {
                    println!(
                        "HTTP get '{ping_api}' failed after {} seconds: {}",
                        startup_time.elapsed().as_secs(),
                        e
                    );
                } else {
                    break;
                }
            }
        }
    }

    impl Drop for CnosdbData {
        fn drop(&mut self) {
            for (k, p) in self.sub_processes.iter_mut() {
                if let Err(e) = p.kill() {
                    println!("Failed to kill cnosdb ({k}) sub-process: {e}");
                }
            }
        }
    }

    async fn start_data() -> CnosdbData {
        let crate_dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let workspace_dir = crate_dir.parent().unwrap();
        let mut data = CnosdbData::new(workspace_dir);
        data.run().await;
        data
    }

    fn clean_env() {
        println!("Cleaning environment...");
        kill_process("cnosdb");
        println!(" - Removing directory '/tmp/cnosdb'");
        let _ = std::fs::remove_dir_all("/tmp/cnosdb");
        println!("Clean environment completed.");
    }

    fn kill_process(process_name: &str) {
        println!("- Killing processes {process_name}...");
        let system = System::new_all();
        for (pid, process) in system.processes() {
            if process.name() == process_name {
                let output = Command::new("kill")
                    .args(["-9", &(pid.to_string())])
                    .output()
                    .expect("failed to execute kill");
                if !output.status.success() {
                    println!(" - failed killing process {} ('{}')", pid, process.name());
                }
                println!(" - killed process {pid} ('{}')", process.name());
            }
        }
    }

    async fn run_case<Fut>(case: fn(PathBuf) -> Fut)
    where
        Fut: Future<Output = ()>,
    {
        let crate_dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let workspace_dir = crate_dir.parent().unwrap();
        let workspace = workspace_dir.to_path_buf();

        clean_env();

        let config_8902_path = workspace.join("config").join("config_8902.toml");
        let config_8902_old = tokio::fs::read_to_string(&config_8902_path).await.unwrap();
        let config_8902_new = config_8902_old
            .replace("# [security.tls_config]", "[security.tls_config]")
            .replace(
                "# certificate = \"./config/tls/server.crt\"",
                "certificate = \"../config/tls/server.crt\"",
            )
            .replace(
                "# private_key = \"./config/tls/server.key\"",
                "private_key = \"../config/tls/server.key\"",
            );
        tokio::fs::write(&config_8902_path, config_8902_new)
            .await
            .unwrap();
        let data = start_data().await;

        case(workspace).await;

        drop(data);
        tokio::fs::write(&config_8902_path, config_8902_old)
            .await
            .unwrap();

        clean_env();
    }

    #[tokio::test]
    #[serial]
    async fn test_v1_sql_path() {
        async fn case(_: PathBuf) {
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
        run_case(case).await
    }

    #[tokio::test]
    #[serial]
    async fn test_v1_write_path() {
        async fn case(_: PathBuf) {
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

        run_case(case).await
    }

    #[tokio::test]
    #[serial]
    async fn test_v1_ping_path() {
        async fn case(_: PathBuf) {
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

        run_case(case).await;
    }

    #[tokio::test]
    #[serial]
    async fn test_cli_connection() {
        async fn case(workspace: PathBuf) {
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

        run_case(case).await;
    }
}
