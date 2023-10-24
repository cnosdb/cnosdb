#![allow(dead_code)]

use std::collections::HashMap;
use std::ffi::OsStr;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use meta::client::MetaHttpClient;
use regex::Regex;
use reqwest::blocking::{ClientBuilder, Response};
use reqwest::{Certificate, IntoUrl};
use sysinfo::{ProcessExt, System, SystemExt};
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

    pub fn tls_client(crt_path: &Path) -> Self {
        let cert_bytes = std::fs::read(crt_path).expect("fail to read crt file");
        let cert = Certificate::from_pem(&cert_bytes).expect("fail to load crt file");
        let inner = ClientBuilder::new()
            .add_root_certificate(cert)
            .build()
            .unwrap_or_else(|e| {
                panic!("Failed to build http client: {}", e);
            });
        Self {
            inner,
            user: String::new(),
            password: None,
        }
    }

    fn do_post(
        &self,
        url: impl IntoUrl,
        body: &str,
        json: bool,
        accept_json: bool,
    ) -> E2eResult<Response> {
        let url_str = url.as_str().to_string();
        let mut req_builder = self.inner.post(url);
        if !self.user.is_empty() {
            req_builder = req_builder.basic_auth(&self.user, self.password.as_ref());
        }
        if json && accept_json {
            req_builder = req_builder.headers({
                let mut m = reqwest::header::HeaderMap::new();
                m.insert(
                    reqwest::header::CONTENT_TYPE,
                    reqwest::header::HeaderValue::from_static("application/json"),
                );
                m.insert(
                    reqwest::header::ACCEPT,
                    reqwest::header::HeaderValue::from_static("application/json"),
                );
                m
            });
        } else if json {
            req_builder = req_builder.header(reqwest::header::CONTENT_TYPE, "application/json");
        } else if accept_json {
            req_builder = req_builder.header(reqwest::header::ACCEPT, "application/json");
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
        self.do_post(url, body, false, false)
    }

    pub fn post_json(&self, url: impl IntoUrl, body: &str) -> E2eResult<Response> {
        self.do_post(url, body, true, false)
    }

    pub fn post_accept_json(&self, url: impl IntoUrl, body: &str) -> E2eResult<Response> {
        self.do_post(url, body, true, true)
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
    pub(crate) workspace: PathBuf,
    pub(crate) last_build: Option<Instant>,
    pub(crate) exe_path: PathBuf,
    pub(crate) config_dir: PathBuf,
    pub(crate) client: Arc<Client>,
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
            client: Arc::new(Client::new("root".to_string(), Some(String::new()))),
            sub_processes: HashMap::with_capacity(2),
        }
    }

    pub fn with_config_dir(workspace_dir: impl AsRef<Path>, config_dir: PathBuf) -> Self {
        let workspace = workspace_dir.as_ref().to_path_buf();
        Self {
            workspace: workspace.clone(),
            last_build: None,
            exe_path: workspace.join("target").join("debug").join("cnosdb"),
            config_dir,
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

    pub fn run_cluster_with_three_data(&mut self) {
        println!("Runing cnosdb cluster at '{}'", self.workspace.display());
        println!("- Building cnosdb");
        self.build();
        println!("- Running cnosdb with config 'config_8902.toml'");
        self.execute("config_8902.toml");
        println!("- Running cnosdb with config 'config_8912.toml'");
        self.execute("config_8912.toml");
        println!("- Running cnosdb with config 'config_8922.toml'");
        self.execute("config_8922.toml");

        let jh1 = self.wait_startup("127.0.0.1:8902");
        let jh2 = self.wait_startup("127.0.0.1:8912");
        let jh3 = self.wait_startup("127.0.0.1:8922");
        let _ = jh1.join();
        let _ = jh2.join();
        let _ = jh3.join();
    }

    pub fn run_singleton(&mut self, config_file: &str) {
        println!("Runing cnosdb singleton at '{}'", self.workspace.display());
        println!("- Building cnosdb");
        self.build();
        println!("- Running cnosdb with config '{config_file}'");
        self.execute_singleton(config_file);

        let jh1 = self.wait_startup(&format!("127.0.0.1:{}", &config_file[7..11]));
        let _ = jh1.join();
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

    fn execute_singleton(&mut self, config_file: &str) {
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
    pub fn wait_startup(&self, host: &str) -> thread::JoinHandle<()> {
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

    pub fn restart(&mut self, config_file: &str) {
        let proc = self
            .sub_processes
            .get_mut(config_file)
            .unwrap_or_else(|| panic!("No data node created with {}", config_file));
        if let Err(e) = proc.kill() {
            println!("Failed to kill cnosdb ({config_file}) sub-process: {e}");
        }
        proc.wait().unwrap();
        let new_proc = Command::new(&self.exe_path)
            .args([
                OsStr::new("run"),
                OsStr::new("--config"),
                self.config_dir.join(config_file).as_os_str(),
            ])
            .stderr(Stdio::null())
            .stdout(Stdio::null())
            .spawn()
            .expect("failed to execute cnosdb");
        *proc = new_proc;
        let jh1 = self.wait_startup(&format!("127.0.0.1:{}", &config_file[7..11]));
        let _ = jh1.join();
    }

    pub fn restart_singleton(&mut self, config_file: &str) {
        let proc = self
            .sub_processes
            .get_mut(config_file)
            .unwrap_or_else(|| panic!("No data node created with {}", config_file));
        if let Err(e) = proc.kill() {
            println!("Failed to kill cnosdb ({config_file}) sub-process: {e}");
        }
        proc.wait().unwrap();
        let new_proc = Command::new(&self.exe_path)
            .args([
                OsStr::new("run"),
                OsStr::new("--config"),
                self.config_dir.join(config_file).as_os_str(),
                OsStr::new("-M"),
                OsStr::new("singleton"),
            ])
            .stderr(Stdio::null())
            .stdout(Stdio::null())
            .spawn()
            .expect("failed to execute cnosdb");
        *proc = new_proc;
        let jh1 = self.wait_startup(&format!("127.0.0.1:{}", &config_file[7..11]));
        let _ = jh1.join();
    }

    pub fn kill_process(&mut self, config_file: &str) {
        let proc = self
            .sub_processes
            .get_mut(config_file)
            .unwrap_or_else(|| panic!("No data node created with {}", config_file));
        if let Err(e) = proc.kill() {
            println!("Failed to kill cnosdb ({config_file}) sub-process: {e}");
        }
        proc.wait().unwrap();

        self.sub_processes.remove(config_file);
    }

    pub fn start_process(&mut self, config_file: &str) {
        let new_proc = Command::new(&self.exe_path)
            .args([
                OsStr::new("run"),
                OsStr::new("--config"),
                self.config_dir.join(config_file).as_os_str(),
            ])
            .stderr(Stdio::null())
            .stdout(Stdio::null())
            .spawn()
            .expect("failed to execute cnosdb");
        self.sub_processes.insert(config_file.to_string(), new_proc);
        let jh1 = self.wait_startup(&format!("127.0.0.1:{}", &config_file[7..11]));
        let _ = jh1.join();
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

/// Start the cnosdb cluster
pub fn start_cluster(runtime: Arc<Runtime>) -> (CnosdbMeta, CnosdbData) {
    let crate_dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let workspace_dir = crate_dir.parent().unwrap();
    let mut meta = CnosdbMeta::new(runtime, workspace_dir);
    meta.run_cluster();
    let mut data = CnosdbData::new(workspace_dir);
    data.run_cluster();
    (meta, data)
}

/// Start the cnosdb cluster with three data nodes
pub fn start_cluster_with_three_data(runtime: Arc<Runtime>) -> (CnosdbMeta, CnosdbData) {
    let crate_dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let workspace_dir = crate_dir.parent().unwrap();
    let mut meta = CnosdbMeta::new(runtime, workspace_dir);
    meta.run_cluster();
    let mut data = CnosdbData::new(workspace_dir);
    data.run_cluster_with_three_data();
    (meta, data)
}

/// Start the cnosdb ingleton
pub fn start_singleton(config_file: &str) -> CnosdbData {
    let crate_dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let workspace_dir = crate_dir.parent().unwrap();
    let mut data = CnosdbData::new(workspace_dir);
    data.run_singleton(config_file);
    data
}

/// Clean test environment.
///
/// 1. Kill all 'cnosdb' and 'cnosdb-meta' process,
/// 2. Remove directory '/tmp/cnosdb'.
pub fn clean_env() {
    println!("Cleaning environment...");
    kill_process("cnosdb");
    kill_process("cnosdb-meta");
    println!(" - Removing directory '/tmp/cnosdb'");
    let _ = std::fs::remove_dir_all("/tmp/cnosdb");
    println!("Clean environment completed.");
}

/// Kill all processes with specified process name.
pub fn kill_process(process_name: &str) {
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

pub fn modify_config_file(file_content: &str, re: &str, new: &str) -> String {
    let reg = Regex::new(re).unwrap();
    reg.replace_all(file_content, new).to_string()
}
