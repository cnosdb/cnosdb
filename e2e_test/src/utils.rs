#![allow(unused)]

mod cnosdb;
mod http_client;

/// Test case context, used to store the state of the test cases.
pub mod global;

use core::panic;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use arrow_flight::sql::client::FlightSqlServiceClient;
use arrow_flight::utils::flight_data_to_batches;
use arrow_flight::FlightInfo;
use arrow_schema::ArrowError;
pub use cnosdb::*;
use datafusion::arrow::record_batch::RecordBatch;
use futures::TryStreamExt;
pub use http_client::*;
use sysinfo::{ProcessRefreshKind, RefreshKind, System};
use tonic::transport::{Channel, Endpoint};

use crate::{E2eError, E2eResult};

pub const CRATE_DIR: &str = env!("CARGO_MANIFEST_DIR");
#[cfg(feature = "debug_mode")]
pub const PROFILE: &str = "debug";
#[cfg(not(feature = "debug_mode"))]
pub const PROFILE: &str = "release";

pub fn get_workspace_dir() -> PathBuf {
    let crate_dir = std::path::PathBuf::from(CRATE_DIR);
    crate_dir
        .parent()
        .map(|p| p.to_path_buf())
        .unwrap_or(crate_dir)
}

#[macro_export]
macro_rules! assert_batches_one_of {
    ($batches:expr, $($expected:expr),*) => {
        let expected_lines = vec![$($expected.as_ref(),)*];
        let formatted = arrow::util::pretty::pretty_format_batches($batches)
        .unwrap()
        .to_string();
        let actual_lines: Vec<&str> = formatted.trim().lines().collect();
        assert!(
            expected_lines.iter().any(|x| x == &actual_lines),
            "\n\nexpected one of:\n\n{:#?}\nactual:\n\n{:#?}\n\n",
            expected_lines, actual_lines
        );
    };
}

#[macro_export]
macro_rules! headers {
    ($($k:expr => $v:expr),*) => {
        {
            let mut m = reqwest::header::HeaderMap::new();
            $(
                m.insert(
                    reqwest::header::HeaderName::from_static($k),
                    reqwest::header::HeaderValue::from_static($v),
                );
            )*
            m
        }
    };
}

#[macro_export]
macro_rules! check_response {
    ($resp:expr) => {
        match $resp {
            Ok(r) => {
                if r.status() != reqwest::StatusCode::OK {
                    match r.text() {
                        Ok(text) => panic!("serve responses error: '{text}'"),
                        Err(e) => panic!("failed to fetch response: {e}"),
                    };
                }
                r
            }
            Err(e) => {
                panic!("failed to do request: {e}");
            }
        }
    };
    ($resp:expr, $status:expr) => {
        match $resp {
            Ok(r) => {
                let r_status = r.status();
                if r_status != $status {
                    match r.text() {
                        Ok(text) => panic!(
                            "serve responses status {r_status} but expected {}: '{text}'",
                            $status
                        ),
                        Err(e) => panic!("failed to fetch response: {e}"),
                    };
                }
                r
            }
            Err(e) => {
                panic!("failed to do request: {e}");
            }
        }
    };
}

/// Execute command and print stdout/stderr during the execution.
pub fn execute_command(command: Command) -> E2eResult<()> {
    let mut cmd_str = command.get_program().to_string_lossy().into_owned();
    command.get_args().for_each(|arg| {
        cmd_str.push(' ');
        cmd_str.push_str(arg.to_string_lossy().as_ref());
    });
    let cmd_str = Arc::new(cmd_str);

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| E2eError::Command(format!("Failed to build tokio runtime: {e}")))?;
    let mut command = tokio::process::Command::from(command);

    use tokio::io::{AsyncBufReadExt, AsyncRead, BufReader};
    async fn read_line_and_print<R: AsyncRead + Unpin, CmdStr: AsRef<String>>(
        reader: R,
        reader_type: &str,
        cmd_str: CmdStr,
    ) -> E2eResult<()> {
        let cmd_str = cmd_str.as_ref();
        let mut reader = BufReader::new(reader);
        let mut buf = String::new();
        let mut n;
        loop {
            n = reader.read_line(&mut buf).await.map_err(|e| {
                E2eError::Command(format!("failed to read '{reader_type} of '{cmd_str}': {e}"))
            })?;
            if n == 0 {
                break;
            }
            print!("{}", buf);
            buf.clear();
        }
        Ok(())
    }

    println!("  - Executing command '{cmd_str}'");
    let cmd_str_inner = cmd_str.clone();
    let ret = runtime.block_on(async move {
        let cmd_str = cmd_str_inner;
        let mut handle = command
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|e| {
                E2eError::Command(format!("Failed to execute command '{cmd_str}': {e}"))
            })?;
        let stdout = handle
            .stdout
            .take()
            .ok_or_else(|| E2eError::Command(format!("failed to get stdout of '{cmd_str}'")))?;
        let stderr = handle
            .stderr
            .take()
            .ok_or_else(|| E2eError::Command(format!("failed to get stderr of '{cmd_str}'")))?;

        let cmd_str_stdout = cmd_str.clone();
        let jh_print_stdout = tokio::spawn(read_line_and_print(stdout, "stdout", cmd_str_stdout));
        let cmd_str_stderr = cmd_str.clone();
        let jh_print_stderr = tokio::spawn(read_line_and_print(stderr, "stderr", cmd_str_stderr));
        let _ = jh_print_stdout.await;
        let _ = jh_print_stderr.await;
        println!("  - Waiting for '{cmd_str}' to finish...");
        handle
            .wait()
            .await
            .map_err(|e| E2eError::Command(format!("Process of '{cmd_str}' not running: {e}")))
    })?;

    if !ret.success() {
        panic!("Failed to execute '{cmd_str}'");
    }

    Ok(())
}

/// Kill all processes with specified process name with signal 'KILL(9)'.
pub fn kill_process(process_name: &str) {
    println!("- Killing processes {process_name}...");
    let system = System::new_with_specifics(
        RefreshKind::nothing().with_processes(ProcessRefreshKind::nothing()),
    );
    for (pid, process) in system.processes() {
        if process.name() == process_name {
            match process.kill_with(sysinfo::Signal::Kill) {
                Some(true) => println!(
                    "- Killed process {pid} ('{}')",
                    process.name().to_string_lossy()
                ),
                Some(false) => println!(
                    "- Failed killing process {pid} ('{}')",
                    process.name().to_string_lossy()
                ),
                None => println!("- Kill with signal 'Kill' isn't supported on this platform"),
            }
        }
    }
}

#[cfg(unix)]
fn kill_child_process(mut proc: Child, force: bool) {
    let pid = proc.id().to_string();

    // Kill process
    let mut kill = Command::new("kill");
    let mut killing_thread = if force {
        println!("- Force killing child process {pid}...");
        kill.args(["-s", "KILL", &pid])
            .spawn()
            .expect("failed to run 'kill -s KILL {pid}'")
    } else {
        println!("- Killing child process {pid}...");
        kill.args(["-s", "TERM", &pid])
            .spawn()
            .expect("failed to run 'kill -s TERM {pid}'")
    };
    match killing_thread.wait() {
        Ok(kill_exit_code) => println!("- Killed process {pid}, exit status: {kill_exit_code}"),
        Err(e) => println!("- Process {pid} not running: {e}"),
    }

    // Remove defunct process
    if let Err(e) = proc.wait() {
        println!("- Process {pid} not running: {e}");
    }
    drop(proc);

    // Wait CnosDB shutdown.
    loop {
        let display_process = Command::new("kill")
            .args(["-0", &pid])
            .output()
            .expect("failed to run 'kill -0 {pid}'");
        if display_process.status.success() {
            println!("- Waiting for process {pid} to exit...");
            thread::sleep(Duration::from_secs(1));
        } else {
            println!("- Process {pid} exited");
            break;
        }
    }
}

#[cfg(windows)]
fn kill_child_process(proc: Child, force: bool) {
    let pid = proc.id().to_string();
    let mut kill = Command::new("taskkill.exe");
    let mut killing_thread = if force {
        println!("- Force killing child process {pid}...");
        kill.args(["/PID", &pid, "/F"])
            .spawn()
            .expect("failed to run 'taskkill.exe /PID {pid} /F'")
    } else {
        println!("- Killing child process {pid}...");
        kill.args(["/PID", &pid])
            .spawn()
            .expect("failed to run 'taskkill.exe /PID {pid}'")
    };
    match killing_thread.wait() {
        Ok(kill_exit_code) => println!("- Killed process {pid}, exit status: {kill_exit_code}"),
        Err(e) => println!("- Process {pid} not running: {e}"),
    }
}

pub(crate) struct DeferGuard<F: FnOnce()>(Option<F>);

impl<F: FnOnce()> DeferGuard<F> {
    pub(crate) fn new(f: F) -> Self {
        Self(Some(f))
    }
}

impl<F: FnOnce()> Drop for DeferGuard<F> {
    fn drop(&mut self) {
        if let Some(f) = self.0.take() {
            f();
        }
    }
}

#[macro_export]
macro_rules! defer {
    ($($code:tt)*) => {
        let _defer_guard = $crate::utils::DeferGuard::new(|| {
            $($code)*
        });
    };
}

pub async fn flight_channel(host: &str, port: u16) -> Result<Channel, ArrowError> {
    let endpoint = Endpoint::new(format!("http://{}:{}", host, port))
        .map_err(|e| ArrowError::IpcError(format!("Cannot create endpoint: {e}")))?
        .connect_timeout(Duration::from_secs(20))
        .timeout(Duration::from_secs(20))
        .tcp_nodelay(true) // Disable Nagle's Algorithm since we don't want packets to wait
        .tcp_keepalive(Option::Some(Duration::from_secs(3600)))
        .http2_keep_alive_interval(Duration::from_secs(300))
        .keep_alive_timeout(Duration::from_secs(20))
        .keep_alive_while_idle(true);

    let channel = endpoint
        .connect()
        .await
        .map_err(|e| ArrowError::IpcError(format!("Cannot connect to endpoint: {e}")))?;

    Ok(channel)
}

pub async fn flight_authed_client(port: u16) -> FlightSqlServiceClient<Channel> {
    let channel = flight_channel("localhost", port).await.unwrap();
    let mut client = FlightSqlServiceClient::new(channel);

    // 1. handshake, basic authentication
    let _ = client.handshake("root", "").await.unwrap();

    client
}

pub async fn flight_fetch_result_and_print(
    flight_info: FlightInfo,
    client: &mut FlightSqlServiceClient<Channel>,
) -> Vec<RecordBatch> {
    let mut batches = vec![];
    for ep in &flight_info.endpoint {
        if let Some(tkt) = &ep.ticket {
            let stream = client.do_get(tkt.clone()).await.unwrap();
            let new_batches = stream.try_collect::<Vec<_>>().await.unwrap();
            batches.extend(new_batches);
        };
    }

    batches
}

fn ls<P: AsRef<Path>>(path: P) -> std::io::Result<Vec<PathBuf>> {
    let path = path.as_ref();
    if !std::fs::metadata(path)?.is_dir() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("Path '{path:?}' is not a directory"),
        ));
    }
    let mut entries = std::fs::read_dir(path)?
        .map(|read_ret| read_ret.map(|e| e.path()))
        .collect::<Result<Vec<_>, std::io::Error>>()?;
    entries.sort();
    Ok(entries)
}

fn ls_names_with_filter<P: AsRef<Path>>(
    path: P,
    filter: impl Fn(&Path) -> bool,
) -> std::io::Result<Vec<String>> {
    let entries = ls(path)?;
    let mut entries = entries
        .iter()
        .filter(|p| filter(p))
        .filter_map(|p| p.file_name())
        .map(|file_name| file_name.to_string_lossy().into_owned())
        .collect::<Vec<_>>();
    Ok(entries)
}

pub fn ls_directories<P: AsRef<Path>>(path: P) -> std::io::Result<Vec<String>> {
    ls_names_with_filter(path, Path::is_dir)
}

pub fn ls_files<P: AsRef<Path>>(path: P) -> std::io::Result<Vec<String>> {
    ls_names_with_filter(path, Path::is_file)
}

/// Split CSV string into a vector of strings, handling double quotes and newlines.
///
/// ### Example
/// ```ignore
/// assert_eq!(
///     vec!["a,b,c".to_string(), "d,e,f".to_string()],
///     split_csv_into_vec("a,b,c\nd,e,f"),
/// )
/// ```
fn split_csv_into_vec(csv: &str) -> Vec<String> {
    let mut results = Vec::new();
    let mut buf = String::new();
    let mut inside_double_quotes = false;
    for c in csv.chars() {
        match c {
            '"' => {
                inside_double_quotes = !inside_double_quotes;
                buf.push(c);
            }
            '\n' if !inside_double_quotes => {
                results.push(buf.clone());
                buf.clear();
            }
            _ => {
                buf.push(c);
            }
        }
    }
    if !buf.is_empty() {
        results.push(buf);
    }
    results
}

mod test {
    use std::io::Write;
    use std::net::{TcpListener, TcpStream};
    use std::path::PathBuf;

    use reqwest::blocking::Client;
    use reqwest::Url;

    use crate::utils::global::E2E_TEST_BASE_DIR;
    use crate::utils::{ls, ls_directories, ls_files, ls_names_with_filter, split_csv_into_vec};

    #[test]
    #[should_panic]
    fn test_check_response() {
        let client = Client::new();

        let url_404 = Url::parse("http://localhost:8080/404").unwrap();
        let resp_404 = client.get(url_404).send();
        check_response!(resp_404);
    }

    fn handle_simple_http_request(mut stream: TcpStream) -> std::io::Result<()> {
        let response = b"HTTP/1.1 401 UNAUTHORIZED\r\nContent-Length: 2\r\n\r\nok";
        stream.write_all(response)
    }

    fn start_simple_http_server(bind_port: u16) -> std::thread::JoinHandle<()> {
        let bind_url = format!("0.0.0.0:{bind_port}");
        let listener = match TcpListener::bind(&bind_url) {
            Ok(l) => l,
            Err(e) => {
                panic!("Could not bind to {bind_url}: {e}")
            }
        };

        std::thread::spawn(move || {
            for stream in listener.incoming() {
                match stream {
                    Ok(stream) => {
                        if let Err(e) = handle_simple_http_request(stream) {
                            panic!("Failed to write response: {e}");
                        }
                    }
                    Err(e) => {
                        eprintln!("Failed to accept connection: {}", e);
                    }
                }
            }
        })
    }

    #[test]
    #[should_panic]
    fn test_check_response_401() {
        let client = Client::new();

        let jh = start_simple_http_server(8080);

        let url = Url::parse("http://localhost:8080").unwrap();
        let resp = client.get(url).send();
        check_response!(resp);
        jh.join().unwrap();
    }

    #[test]
    fn test_ls() {
        let test_dir = PathBuf::from(E2E_TEST_BASE_DIR).join("test_ls");
        std::fs::create_dir_all(test_dir.join("dir_1")).unwrap();
        std::fs::write(test_dir.join("a.txt"), "hello").unwrap();

        let entries = ls_directories(&test_dir).unwrap();
        assert_eq!(entries, vec!["dir_1".to_string()]);
        let entries = ls_files(test_dir).unwrap();
        assert_eq!(entries, vec!["a.txt".to_string()]);
    }

    #[test]
    fn test_split_csv_into_vec() {
        assert_eq!(vec!["a", "b", "c"], split_csv_into_vec("a\nb\nc"));
        assert_eq!(vec!["\"a\nb\"", "c"], split_csv_into_vec("\"a\nb\"\nc"));
        assert_eq!(
            vec![
                "hello world,\"this is\n    one sentence\n    with multiple lines\"",
                "hello everyone,c"
            ],
            split_csv_into_vec(
                r#"hello world,"this is
    one sentence
    with multiple lines"
hello everyone,c"#
            )
        );
        assert_eq!(
            vec![
                "hello world,\"this is\n    one \"\"sentence\n\"\"    with multiple lines\"",
                "hello everyone,c"
            ],
            split_csv_into_vec(
                r#"hello world,"this is
    one ""sentence
""    with multiple lines"
hello everyone,c"#
            )
        );
    }
}
