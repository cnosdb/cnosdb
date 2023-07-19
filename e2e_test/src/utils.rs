#![allow(dead_code)]

use std::collections::HashMap;
use std::ffi::OsStr;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use meta::client::MetaHttpClient;
use reqwest::blocking::{ClientBuilder, Response};
use reqwest::IntoUrl;
use tokio::runtime::Runtime;

use crate::{E2eError, E2eResult};

pub struct Client {
    inner: reqwest::blocking::Client,
    user: String,
    password: Option<String>,
}

impl Client {
    pub fn new(user: String, password: Option<String>) -> Self {
        let inner = ClientBuilder::new().build().unwrap_or_else(|e| {
            panic!("Failed to build http client: {}", e);
        });
        Self {
            inner,
            user,
            password,
        }
    }

    pub fn no_auth() -> Self {
        let inner = ClientBuilder::new().build().unwrap_or_else(|e| {
            panic!("Failed to build http client: {}", e);
        });
        Self {
            inner,
            user: String::new(),
            password: None,
        }
    }

    fn do_post(&self, url: impl IntoUrl, body: &str, json: bool) -> E2eResult<Response> {
        let url_str = url.as_str().to_string();
        let mut req_builder = self.inner.post(url);
        if !self.user.is_empty() {
            req_builder = req_builder.basic_auth(&self.user, self.password.as_ref());
        }
        if json {
            req_builder = req_builder.header(reqwest::header::CONTENT_TYPE, "application/json");
        }
        if !body.is_empty() {
            req_builder = req_builder.body(body.to_string());
        }
        match req_builder.send() {
            Ok(r) => Ok(r),
            Err(e) => {
                let msg = format!("HTTP post failed: '{url_str}', '{body}': {e}");
                Err(E2eError::Http(msg))
            }
        }
    }

    pub fn post(&self, url: impl IntoUrl, body: &str) -> E2eResult<Response> {
        self.do_post(url, body, false)
    }

    pub fn post_json(&self, url: impl IntoUrl, body: &str) -> E2eResult<Response> {
        self.do_post(url, body, true)
    }

    pub fn get(&self, url: &str, body: &str) -> E2eResult<Response> {
        let mut req_builder = self.inner.get(url);
        if !self.user.is_empty() {
            req_builder = req_builder.basic_auth(&self.user, self.password.as_ref());
        }
        if !body.is_empty() {
            req_builder = req_builder.body(body.to_string());
        }
        match req_builder.send() {
            Ok(r) => Ok(r),
            Err(e) => {
                let msg = format!("HTTP get failed: '{url}', '{body}': {e}");
                Err(E2eError::Http(msg))
            }
        }
    }
}

pub struct CnosdbMeta {
    pub(crate) runtime: Arc<Runtime>,
    pub(crate) workspace: PathBuf,
    pub(crate) last_build: Option<Instant>,
    pub(crate) exe_path: PathBuf,
    pub(crate) config_dir: PathBuf,
    pub(crate) client: Arc<Client>,
    pub(crate) meta_client: Arc<MetaHttpClient>,
    pub(crate) sub_processes: HashMap<String, Child>,
}

impl CnosdbMeta {
    pub fn new(runtime: Arc<Runtime>, workspace_dir: impl AsRef<Path>) -> Self {
        let workspace = workspace_dir.as_ref().to_path_buf();
        Self {
            runtime,
            workspace: workspace.clone(),
            last_build: None,
            exe_path: workspace.join("target").join("debug").join("cnosdb-meta"),
            config_dir: workspace.join("meta").join("config"),
            client: Arc::new(Client::no_auth()),
            meta_client: Arc::new(MetaHttpClient::new("127.0.0.1:8901")),
            sub_processes: HashMap::with_capacity(3),
        }
    }

    pub fn run_cluster(&mut self) {
        println!(
            "Running cnosdb-meta cluster at '{}'",
            self.workspace.display()
        );
        println!("- Building cnosdb-meta...");
        self.build();
        println!("- Running cnosdb-meta with config 'config_8901.toml'");
        self.execute("config_8901.toml");
        println!("- Running cnosdb-meta with config 'config_8911.toml'");
        self.execute("config_8911.toml");
        println!("- Running cnosdb-meta with config 'config_8921.toml'");
        self.execute("config_8921.toml");
        thread::sleep(Duration::from_secs(3));

        println!("- Installing cnosdb-meta cluster...");
        let master = "http://127.0.0.1:8901";
        self.client
            .post_json(format!("{master}/init").as_str(), "{}")
            .unwrap();
        thread::sleep(Duration::from_secs(1));
        self.client
            .post_json(
                format!("{master}/add-learner").as_str(),
                "[2, \"127.0.0.1:8911\"]",
            )
            .unwrap();
        thread::sleep(Duration::from_secs(1));
        self.client
            .post_json(
                format!("{master}/add-learner").as_str(),
                "[3, \"127.0.0.1:8921\"]",
            )
            .unwrap();
        thread::sleep(Duration::from_secs(1));
        self.client
            .post_json(format!("{master}/change-membership").as_str(), "[1, 2, 3]")
            .unwrap();
        thread::sleep(Duration::from_secs(1));
    }

    pub fn query(&self) -> String {
        self.client
            .get("http://127.0.0.1:8901/debug", "")
            .unwrap()
            .text()
            .unwrap()
    }

    fn build(&mut self) {
        let mut cargo_build = Command::new("cargo");
        let output = cargo_build
            .current_dir(&self.workspace)
            .args(["build", "--package", "meta", "--bin", "cnosdb-meta"])
            .output()
            .expect("failed to execute cargo build");
        if !output.status.success() {
            let message = format!(
                "Failed to build cnosdb-meta: stdout: {}, stderr: {}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
            panic!("Failed to build cnosdb-meta: {message}");
        }
        self.last_build = Some(Instant::now());
    }

    fn execute(&mut self, config_file: &str) {
        let config_path = self.config_dir.join(config_file);
        let proc = Command::new(&self.exe_path)
            .args([OsStr::new("-c"), config_path.as_os_str()])
            .stderr(Stdio::null())
            .stdout(Stdio::null())
            .spawn()
            .expect("failed to execute cnosdb-meta");
        self.sub_processes.insert(config_file.to_string(), proc);
    }
}

impl Drop for CnosdbMeta {
    fn drop(&mut self) {
        for (k, p) in self.sub_processes.iter_mut() {
            if let Err(e) = p.kill() {
                println!("Failed to kill cnosdb-meta ({k}) sub-process: {e}");
            }
        }
    }
}

pub struct CnosdbData {
    pub(crate) runtime: Arc<Runtime>,
    pub(crate) workspace: PathBuf,
    pub(crate) last_build: Option<Instant>,
    pub(crate) exe_path: PathBuf,
    pub(crate) config_dir: PathBuf,
    pub(crate) client: Arc<Client>,
    pub(crate) sub_processes: HashMap<String, Child>,
}

impl CnosdbData {
    pub fn new(runtime: Arc<Runtime>, workspace_dir: impl AsRef<Path>) -> Self {
        let workspace = workspace_dir.as_ref().to_path_buf();
        Self {
            runtime,
            workspace: workspace.clone(),
            last_build: None,
            exe_path: workspace.join("target").join("debug").join("cnosdb"),
            config_dir: workspace.join("config"),
            client: Arc::new(Client::new("root".to_string(), Some(String::new()))),
            sub_processes: HashMap::with_capacity(2),
        }
    }

    pub fn run_cluster(&mut self) {
        println!("Runing cnosdb cluster at '{}'", self.workspace.display());
        println!("- Building cnosdb");
        self.build();
        println!("- Running cnosdb with config 'config_8902.toml'");
        self.execute("config_8902.toml");
        println!("- Running cnosdb with config 'config_8912.toml'");
        self.execute("config_8912.toml");

        let jh1 = self.wait_startup("127.0.0.1:8902");
        let jh2 = self.wait_startup("127.0.0.1:8912");
        let _ = jh1.join();
        let _ = jh2.join();
    }

    fn build(&mut self) {
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

    fn execute(&mut self, config_file: &str) {
        let config_path = self.config_dir.join(config_file);
        let proc = Command::new(&self.exe_path)
            .args([
                OsStr::new("run"),
                OsStr::new("--config"),
                config_path.as_os_str(),
            ])
            .stderr(Stdio::null())
            .stdout(Stdio::null())
            .spawn()
            .expect("failed to execute cnosdb");
        self.sub_processes.insert(config_file.to_string(), proc);
    }

    /// Wait cnosdb startup by checking ping api in loop
    fn wait_startup(&self, host: &str) -> thread::JoinHandle<()> {
        let ping_api = format!("http://{host}/api/v1/ping");
        let startup_time = std::time::Instant::now();
        let client = self.client.clone();
        thread::spawn(move || loop {
            thread::sleep(Duration::from_secs(3));
            if let Err(e) = client.get(&ping_api, "") {
                println!(
                    "HTTP get '{ping_api}' failed after {} seconds: {}",
                    startup_time.elapsed().as_secs(),
                    e
                );
            } else {
                break;
            }
        })
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
