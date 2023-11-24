#![allow(dead_code)]

use std::collections::HashMap;
use std::ffi::OsStr;
use std::fmt::{self, Display, Formatter};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant};
use std::{fs, thread};

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

pub struct CnosDBMeta {
    pub(crate) runtime: Arc<Runtime>,
    pub(crate) workspace: PathBuf,
    pub(crate) last_build: Option<Instant>,
    pub(crate) exe_path: PathBuf,
    pub(crate) config_path: PathBuf,
    pub(crate) client: Arc<Client>,
    pub(crate) meta_client: Arc<MetaHttpClient>,
    pub(crate) sub_processes: HashMap<String, Child>,
}

impl CnosDBMeta {
    pub fn new(runtime: Arc<Runtime>, workspace_dir: impl AsRef<Path>) -> Self {
        let workspace = workspace_dir.as_ref().to_path_buf();
        Self {
            runtime,
            workspace: workspace.clone(),
            last_build: None,
            exe_path: workspace.join("target").join("debug").join("cnosdb-meta"),
            config_path: workspace.join("meta").join("config"),
            client: Arc::new(Client::no_auth()),
            meta_client: Arc::new(MetaHttpClient::new("127.0.0.1:8901")),
            sub_processes: HashMap::with_capacity(3),
        }
    }

    pub fn run_single_meta(&mut self) {
        println!("Running cnosdb-meta at '{}'", self.workspace.display());
        println!("- Building cnosdb-meta...");
        self.build();
        println!("- Running cnosdb-meta with config 'config_8901.toml'");
        self.execute("config_8901.toml");
        thread::sleep(Duration::from_secs(3));

        println!("- Init cnosdb-meta ...");
        let master = "http://127.0.0.1:8901";
        self.client
            .post_json(format!("{master}/init").as_str(), "{}")
            .unwrap();
        thread::sleep(Duration::from_secs(1));
        self.client
            .post_json(format!("{master}/change-membership").as_str(), "[1]")
            .unwrap();
        thread::sleep(Duration::from_secs(1));
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
        let config_path = self.config_path.join(config_file);
        let proc = Command::new(&self.exe_path)
            .args([OsStr::new("-c"), config_path.as_os_str()])
            .stderr(Stdio::inherit())
            .stdout(Stdio::inherit())
            .spawn()
            .expect("failed to execute cnosdb-meta");
        self.sub_processes.insert(config_file.to_string(), proc);
    }
}

impl Drop for CnosDBMeta {
    fn drop(&mut self) {
        for (k, p) in self.sub_processes.iter_mut() {
            if let Err(e) = p.kill() {
                println!("Failed to kill cnosdb-meta ({k}) sub-process: {e}");
            }
        }
    }
}

pub enum CnosDBDataMode {
    Query,
    Tskv,
    Bundle,
    Singleton,
}

impl AsRef<OsStr> for CnosDBDataMode {
    fn as_ref(&self) -> &OsStr {
        match self {
            CnosDBDataMode::Query => OsStr::new("query"),
            CnosDBDataMode::Tskv => OsStr::new("tskv"),
            CnosDBDataMode::Bundle => OsStr::new("bundle"),
            CnosDBDataMode::Singleton => OsStr::new("singleton"),
        }
    }
}

impl Display for CnosDBDataMode {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match self {
            CnosDBDataMode::Query => write!(f, "Query"),
            CnosDBDataMode::Tskv => write!(f, "Tskv"),
            CnosDBDataMode::Bundle => write!(f, "Bundle"),
            CnosDBDataMode::Singleton => write!(f, "Singleton"),
        }
    }
}

pub struct CnosDBData {
    pub(crate) workspace: PathBuf,
    pub(crate) last_build: Option<Instant>,
    pub(crate) exe_path: PathBuf,
    pub(crate) config_path: PathBuf,
    pub(crate) client: Arc<Client>,
    pub(crate) sub_processes: HashMap<String, (Child, CnosDBDataMode)>,
}

impl CnosDBData {
    pub fn new(workspace_dir: impl AsRef<Path>) -> Self {
        let workspace = workspace_dir.as_ref().to_path_buf();
        Self {
            workspace: workspace.clone(),
            last_build: None,
            exe_path: workspace.join("target").join("debug").join("cnosdb"),
            config_path: workspace.join("config"),
            client: Arc::new(Client::new("root".to_string(), Some(String::new()))),
            sub_processes: HashMap::with_capacity(2),
        }
    }

    pub fn with_config_path(workspace_dir: impl AsRef<Path>, config_path: PathBuf) -> Self {
        let workspace = workspace_dir.as_ref().to_path_buf();
        Self {
            workspace: workspace.clone(),
            last_build: None,
            exe_path: workspace.join("target").join("debug").join("cnosdb"),
            config_path,
            client: Arc::new(Client::new("root".to_string(), Some(String::new()))),
            sub_processes: HashMap::with_capacity(2),
        }
    }

    pub fn run(&mut self, mode: CnosDBDataMode, config_file: &str, http_addr: &str) {
        println!("Runing cnosdb at '{}'", self.workspace.display());
        println!("- Building cnosdb");
        self.build();
        println!("- Running cnosdb with config '{config_file}', deploy mode {mode}");
        self.execute(config_file, mode);

        let jh = self.wait_startup(http_addr);
        jh.join().unwrap();
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

    fn execute(&mut self, config_file: &str, mode: CnosDBDataMode) {
        let config_path = self.config_path.join(config_file);
        let proc = Command::new(&self.exe_path)
            .args([
                OsStr::new("run"),
                OsStr::new("--config"),
                config_path.as_os_str(),
                OsStr::new("-M"),
                mode.as_ref(),
            ])
            .stderr(Stdio::inherit())
            .stdout(Stdio::inherit())
            .spawn()
            .expect("failed to execute cnosdb");
        self.sub_processes
            .insert(config_file.to_string(), (proc, mode));
    }

    /// Wait cnosdb startup by checking ping api in loop
    pub fn wait_startup(&self, host: &str) -> thread::JoinHandle<()> {
        let host = host.to_owned();
        let ping_api = format!("http://{host}/api/v1/ping");
        let startup_time = std::time::Instant::now();
        let client = self.client.clone();
        thread::spawn(move || {
            let mut counter = 0;
            loop {
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
                counter += 1;
                if counter == 30 {
                    panic!("Test case failed, waiting too long for {host} to startup");
                }
            }
        })
    }

    // /// Wait all data node healthy
    // pub fn wait_healthy(&self, host: &str) {
    //     let show_datanodes_api = format!("http://{host}/api/v1/sql?db=public");
    //     let startup_time = std::time::Instant::now();
    //     let client = self.client.clone();
    //     let mut counter = 0;
    //     loop {
    //         thread::sleep(Duration::from_secs(3));
    //         match client.post_accept_json(&show_datanodes_api, "show datanodes") {
    //             Err(e) => {
    //                 println!(
    //                     "HTTP post '{show_datanodes_api}' failed after {} seconds: {}",
    //                     startup_time.elapsed().as_secs(),
    //                     e
    //                 );
    //             }
    //             Ok(resp) => {
    //                 let text = resp.text().unwrap();
    //                 if text.match_indices("HEALTHY").count() == self.sub_processes.len() {
    //                     break;
    //                 } else {
    //                     println!("Waiting for all nodes to be healthy")
    //                 }
    //             }
    //         }
    //         counter += 1;
    //         if counter == 30 {
    //             println!("Test case failed, waiting too long for cluster to be healthy");
    //         }
    //     }
    // }

    pub fn restart(&mut self, config_file: &str, http_addr: &str) {
        let (proc, mode) = self
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
                self.config_path.join(config_file).as_os_str(),
                OsStr::new("-M"),
                mode.as_ref(),
            ])
            .stderr(Stdio::inherit())
            .stdout(Stdio::inherit())
            .spawn()
            .expect("failed to execute cnosdb");
        *proc = new_proc;
        let jh = self.wait_startup(http_addr);
        jh.join().unwrap();
    }

    pub fn kill_process(&mut self, config_file: &str) {
        let (proc, _) = self
            .sub_processes
            .get_mut(config_file)
            .unwrap_or_else(|| panic!("No data node created with {}", config_file));
        if let Err(e) = proc.kill() {
            println!("Failed to kill cnosdb ({config_file}) sub-process: {e}");
        }
        proc.wait().unwrap();

        self.sub_processes.remove(config_file);
    }

    pub fn start_process(&mut self, config_file: &str, mode: CnosDBDataMode) {
        let new_proc = Command::new(&self.exe_path)
            .args([
                OsStr::new("run"),
                OsStr::new("--config"),
                self.config_path.join(config_file).as_os_str(),
                OsStr::new("-M"),
                mode.as_ref(),
            ])
            .stderr(Stdio::inherit())
            .stdout(Stdio::inherit())
            .spawn()
            .expect("failed to execute cnosdb");
        self.sub_processes
            .insert(config_file.to_string(), (new_proc, mode));
        let jh = self.wait_startup(&format!("127.0.0.1:{}", &config_file[7..11]));
        jh.join().unwrap();
    }
}

impl Drop for CnosDBData {
    fn drop(&mut self) {
        for (k, (p, _)) in self.sub_processes.iter_mut() {
            if let Err(e) = p.kill() {
                println!("Failed to kill cnosdb ({k}) sub-process: {e}");
            }
        }
    }
}

/// Start the cnosdb bundle cluster
pub fn start_bundle_cluster(
    runtime: Arc<Runtime>,
    meta_num: u32,
    bundle_num: u32,
) -> (CnosDBMeta, CnosDBData) {
    let crate_dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let workspace_dir = crate_dir.parent().unwrap();
    let mut meta = CnosDBMeta::new(runtime, workspace_dir);
    let mut data = CnosDBData::new(workspace_dir);
    match (meta_num, bundle_num) {
        (1, 2) => {
            meta.run_single_meta();
            data.run(CnosDBDataMode::Bundle, "config_8902.toml", "127.0.0.1:8902");
            data.run(CnosDBDataMode::Bundle, "config_8912.toml", "127.0.0.1:8912");
        }
        (1, 3) => {
            meta.run_single_meta();
            data.run(CnosDBDataMode::Bundle, "config_8902.toml", "127.0.0.1:8902");
            data.run(CnosDBDataMode::Bundle, "config_8912.toml", "127.0.0.1:8912");
            data.run(CnosDBDataMode::Bundle, "config_8922.toml", "127.0.0.1:8922");
        }
        (3, 2) => {
            meta.run_cluster();
            data.run(CnosDBDataMode::Bundle, "config_8902.toml", "127.0.0.1:8902");
            data.run(CnosDBDataMode::Bundle, "config_8912.toml", "127.0.0.1:8912");
        }
        (3, 3) => {
            meta.run_cluster();
            data.run(CnosDBDataMode::Bundle, "config_8902.toml", "127.0.0.1:8902");
            data.run(CnosDBDataMode::Bundle, "config_8912.toml", "127.0.0.1:8912");
            data.run(CnosDBDataMode::Bundle, "config_8922.toml", "127.0.0.1:8922");
        }
        _ => panic!("unsupported cluster: meta_num: {meta_num}, bundle_num: {bundle_num}"),
    }
    (meta, data)
}

/// Start the cnosdb sepration cluster
pub fn start_sepration_cluster(
    runtime: Arc<Runtime>,
    meta_num: u32,
    query_num: u32,
    tskv_num: u32,
) -> (CnosDBMeta, CnosDBData) {
    let crate_dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let workspace_dir = crate_dir.parent().unwrap();
    let mut meta = CnosDBMeta::new(runtime, workspace_dir);
    let mut data = CnosDBData::new(workspace_dir);
    match (meta_num, query_num, tskv_num) {
        (1, 1, 1) => {
            meta.run_single_meta();
            data.run(CnosDBDataMode::Tskv, "config_8902.toml", "127.0.0.1:8902");
            data.run(CnosDBDataMode::Query, "config_8912.toml", "127.0.0.1:8912");
        }
        (3, 1, 1) => {
            meta.run_cluster();
            data.run(CnosDBDataMode::Tskv, "config_8902.toml", "127.0.0.1:8902");
            data.run(CnosDBDataMode::Query, "config_8912.toml", "127.0.0.1:8912");
        }
        _ => panic!("unsupported cluster: meta_num: {meta_num}, query_num: {query_num}, tskv_num: {tskv_num}"),
    }
    (meta, data)
}

/// Start the cnosdb singleton
pub fn start_singleton(config_file: &str, http_addr: &str) -> CnosDBData {
    let crate_dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let workspace_dir = crate_dir.parent().unwrap();
    let mut data = CnosDBData::new(workspace_dir);
    data.run(CnosDBDataMode::Singleton, config_file, http_addr);
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

pub fn modify_config_file(file_content: &str, pattern: &str, new: &str) -> String {
    let reg = Regex::new(pattern).unwrap();
    reg.replace_all(file_content, new).to_string()
}

pub fn change_config_file(config_path: &PathBuf, replace_table: Vec<(&str, &str)>) -> String {
    let config_old = std::fs::read_to_string(config_path).unwrap();
    let mut config_new = config_old.clone();

    for (from, to) in replace_table {
        config_new = config_new.replace(from, to)
    }
    std::fs::write(config_path, config_new).unwrap();
    config_old
}

pub fn workspace_dir() -> PathBuf {
    let crate_dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    crate_dir.parent().unwrap().into()
}

pub struct ConfigFile {
    path: PathBuf,
    original_content: String,
    new_content: String,
}

impl ConfigFile {
    pub fn new(path: PathBuf, original_content: String, new_content: String) -> Self {
        ConfigFile {
            path,
            original_content,
            new_content,
        }
    }
}

type TestCaseFn = fn(Option<&CnosDBMeta>, Option<&CnosDBData>);

/// Test case need to modify config file
pub struct TestCase {
    config_files: Vec<ConfigFile>,
    case: TestCaseFn,
    case_name: String,
}

impl TestCase {
    pub fn builder() -> TestCaseBuilder {
        TestCaseBuilder {
            config_files: None,
            case: None,
            case_name: None,
        }
    }

    pub fn run_cluster(&self, pass_meta: bool, pass_data: bool, meta_num: u32, bundle_num: u32) {
        println!("Test begin '{}'", self.case_name);
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .worker_threads(4)
            .build()
            .unwrap();
        let runtime = Arc::new(runtime);
        clean_env();
        let (meta, data) = start_bundle_cluster(runtime, meta_num, bundle_num);
        match (pass_meta, pass_data) {
            (true, true) => (self.case)(Some(&meta), Some(&data)),
            (true, false) => (self.case)(Some(&meta), None),
            (false, true) => (self.case)(None, Some(&data)),
            (false, false) => (self.case)(None, None),
        }
        println!("Test complete '{}'", self.case_name);
    }

    pub fn run_singleton(&self, node_num: u32) {
        if node_num == 0 || node_num > 3 {
            panic!("unsupported node_num: {node_num}");
        }
        println!("Test begin '{}'", self.case_name);
        clean_env();

        let mut nodes = vec![start_singleton("test_config_8902.toml", "127.0.0.1:8902")];
        if node_num > 1 {
            nodes.push(start_singleton("test_config_8912.toml", "127.0.0.1:8912"));
        }
        if node_num > 2 {
            nodes.push(start_singleton("test_config_8922.toml", "127.0.0.1:8922"));
        }

        (self.case)(None, None);
        println!("Test complete '{}'", self.case_name);
    }
}

impl Drop for TestCase {
    fn drop(&mut self) {
        for config_file in &self.config_files {
            fs::write(&config_file.path, &config_file.original_content).unwrap();
        }
        clean_env();
    }
}

pub struct TestCaseBuilder {
    config_files: Option<Vec<ConfigFile>>,
    case: Option<TestCaseFn>,
    case_name: Option<String>,
}

impl TestCaseBuilder {
    pub fn set_case_name(mut self, case_name: String) -> Self {
        self.case_name = Some(case_name);
        self
    }

    pub fn set_case(mut self, case: TestCaseFn) -> Self {
        self.case = Some(case);
        self
    }

    pub fn set_config_files(mut self, config_files: Vec<ConfigFile>) -> Self {
        self.config_files = Some(config_files);
        self
    }

    pub fn build(&mut self) -> TestCase {
        let case = self.case.take().expect("Test case function is needed");
        let case_name = self.case_name.take().expect("Test case name is needed");
        let config_files = if let Some(config_files) = self.config_files.take() {
            config_files
        } else {
            vec![]
        };
        for config_file in &config_files {
            fs::write(&config_file.path, &config_file.new_content).unwrap();
        }
        TestCase {
            config_files,
            case,
            case_name,
        }
    }
}
