#![allow(unused)]

mod cnosdb;
mod http_client;

/// Test case context, used to store the state of the test cases.
pub mod global;

use core::panic;
use std::path::PathBuf;
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
    let system =
        System::new_with_specifics(RefreshKind::new().with_processes(ProcessRefreshKind::new()));
    for (pid, process) in system.processes() {
        if process.name() == process_name {
            match process.kill_with(sysinfo::Signal::Kill) {
                Some(true) => println!("- Killed process {pid} ('{}')", process.name()),
                Some(false) => println!("- Failed killing process {pid} ('{}')", process.name()),
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
        .map_err(|_| ArrowError::IoError("Cannot create endpoint".to_string()))?
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
        .map_err(|e| ArrowError::IoError(format!("Cannot connect to endpoint: {e}")))?;

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
            let flight_data = stream.try_collect::<Vec<_>>().await.unwrap();
            batches.extend(flight_data_to_batches(&flight_data).unwrap());
        };
    }

    batches
}

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
    use crate::utils::split_csv_into_vec;

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
