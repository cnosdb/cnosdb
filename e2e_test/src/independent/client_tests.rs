use std::io::{self, BufRead, BufReader, Write};
use std::process::{Child, ChildStdin, Command, Stdio};
use std::sync::{Arc, Mutex, RwLock};
use std::thread::{sleep, JoinHandle};
use std::time::{Duration, Instant};

use crate::utils::global::E2eContext;
use crate::utils::{execute_command, get_workspace_dir, Client};
use crate::{check_response, cluster_def};

/// Helper for running the `cnosdb-cli` command line interface.
pub struct CnosdbCliDef {
    args: Vec<String>,
}

impl CnosdbCliDef {
    pub fn new() -> Self {
        Self { args: vec![] }
    }

    pub fn with_args<S: ToString>(args: &[S]) -> Self {
        Self {
            args: args.iter().map(|s| s.to_string()).collect(),
        }
    }

    pub fn run(&self) -> CnosdbCommandLineInterfaceHelper {
        self.build();
        CnosdbCommandLineInterfaceHelper::new(self.execute())
    }

    fn build(&self) {
        let workspace_dir = get_workspace_dir();
        println!("- Building 'cnosdb-cli' at '{}'", workspace_dir.display());
        let mut command = Command::new("cargo");
        command.current_dir(&workspace_dir);
        command.args(["build", "--package", "client", "--bin", "cnosdb-cli"]);
        if let Err(e) = execute_command(command) {
            panic!("Failed to build cnosdb-cli: {e}");
        }
        println!(
            "- Build 'cnosdb-cli' at '{}' completed",
            workspace_dir.display()
        );
    }

    fn execute(&self) -> Child {
        let workspace_dir = get_workspace_dir();
        let exe_path = workspace_dir
            .join("target")
            .join("debug")
            .join("cnosdb-cli");
        Command::new(&exe_path)
            .current_dir(workspace_dir)
            .args(&self.args)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap_or_else(|e| panic!("fail to start '{} [args]': {e}", exe_path.display()))
    }
}

pub struct CnosdbCommandLineInterfaceHelper {
    subprocess: Child,

    stdin: ChildStdin,
    stdin_last_write: Instant,

    stdout_thread: Option<JoinHandle<()>>,
    stdout_contents: Arc<RwLock<String>>,
    stdout_last_write: Arc<Mutex<Instant>>,

    stderr_thread: Option<JoinHandle<()>>,
    stderr_contents: Arc<RwLock<String>>,
    stderr_last_write: Arc<Mutex<Instant>>,
}

impl CnosdbCommandLineInterfaceHelper {
    fn new(mut process: Child) -> Self {
        let stdin = process.stdin.take().expect("stdin was already captured");
        let stdout = process.stdout.take().expect("stdout was already captured");
        let stderr = process.stderr.take().expect("stderr was already captured");

        fn start_reading_thread<R: io::Read + Send + 'static>(
            thread_name: String,
            read: R,
        ) -> (JoinHandle<()>, Arc<RwLock<String>>, Arc<Mutex<Instant>>) {
            let contents = Arc::new(RwLock::new(String::new()));
            let instant = Arc::new(Mutex::new(Instant::now()));
            let contents_inner = contents.clone();
            let instant_inner = instant.clone();
            let join_handle = std::thread::Builder::new()
                .name(thread_name.clone())
                .spawn(move || {
                    let mut reader = BufReader::new(read);
                    let mut buf = String::new();
                    loop {
                        buf.clear();
                        let n = reader.read_line(&mut buf).unwrap();
                        if n == 0 {
                            println!("{thread_name}>>EOF");
                            *instant_inner.lock().unwrap() = Instant::now();
                            break;
                        }
                        print!("{thread_name}>>{buf}");
                        contents_inner.write().unwrap().push_str(&buf);
                        *instant_inner.lock().unwrap() = Instant::now();
                    }
                })
                .unwrap();
            (join_handle, contents, instant)
        }

        let (stdout_thread, stdout_contents, stdout_last_write) =
            start_reading_thread("stdout".to_string(), stdout);
        let (stderr_thread, stderr_contents, stderr_last_write) =
            start_reading_thread("stderr".to_string(), stderr);

        Self {
            subprocess: process,
            stdin,
            stdin_last_write: Instant::now(),
            stdout_thread: Some(stdout_thread),
            stdout_last_write,
            stdout_contents,
            stderr_thread: Some(stderr_thread),
            stderr_contents,
            stderr_last_write,
        }
    }

    pub fn write<B: AsRef<[u8]>>(&mut self, inputs: &[B]) -> io::Result<()> {
        for input in inputs {
            let input_bytes = input.as_ref();
            println!(
                "stdin<<{}",
                std::str::from_utf8(input_bytes).expect("utf8 bytes"),
            );
            self.stdin.write_all(input_bytes)?;
        }
        self.stdin_last_write = Instant::now();
        Ok(())
    }

    fn clear(&self) {
        self.stdout_contents.write().unwrap().clear();
        self.stderr_contents.write().unwrap().clear();
    }

    pub fn read(&self) -> io::Result<(String, String)> {
        Ok((
            self.stdout_contents.read().unwrap().clone(),
            self.stderr_contents.read().unwrap().clone(),
        ))
    }

    /// Get the content of stdout and stderr after the given duration of the last write to stdout and stderr.
    pub fn read_after_write(&self, duration: Duration) -> io::Result<(String, String)> {
        fn wait(tick_prev: Instant, tick_next: Arc<Mutex<Instant>>, duration: Duration) {
            let now = Instant::now();
            loop {
                if *tick_next.lock().unwrap() - tick_prev > duration {
                    break;
                }
                if now.elapsed() > duration {
                    break;
                }
                sleep(Duration::from_secs(1));
            }
        }
        wait(
            self.stdin_last_write,
            self.stdout_last_write.clone(),
            duration,
        );
        wait(
            self.stdin_last_write,
            self.stderr_last_write.clone(),
            duration,
        );

        let ret = (
            self.stdout_contents.read().unwrap().clone(),
            self.stderr_contents.read().unwrap().clone(),
        );
        self.clear();
        Ok(ret)
    }

    pub fn skip(&self) {
        sleep(Duration::from_secs(1));
        self.clear();
    }
}

impl Drop for CnosdbCommandLineInterfaceHelper {
    fn drop(&mut self) {
        // kill cnosdb-cli process
        self.subprocess.kill().expect("could not kill cli process");
        self.stdout_thread.take().map(|t| t.join());
        self.stderr_thread.take().map(|t| t.join());
    }
}

#[test]
fn simple_test() {
    let mut ctx = E2eContext::new("client_tests", "simple_test");
    let mut executor = ctx.build_executor(cluster_def::one_data(1));
    let host_port = executor.cluster_definition().data_cluster_def[0].http_host_port;
    let api_v1_sql_url = &format!("http://{host_port}/api/v1/sql?db=public");
    let api_v1_write_url = &format!("http://{host_port}/api/v1/write?db=public");
    let api_v1_sql_url_tenant_a =
        &format!("http://{host_port}/api/v1/sql?tenant=tenant_a&db=public");

    executor.startup();

    let client = executor.case_context().data_client(0);
    let client_user_a = Client::with_auth("user_a".to_string(), None);

    //bug test for 1533
    {
        // data prepare
        check_response!(client.post(api_v1_sql_url, "create table test(f1 bigint, tags(t1));"));
        check_response!(client.post(
            api_v1_sql_url,
            "insert into test(time,t1,f1)values(now(),'t11',1);",
        ));

        let resp = check_response!(client.post(api_v1_sql_url, "select t1,f1 from test;"));
        assert_eq!(resp.text().unwrap(), "t1,f1\nt11,1\n");

        let resp = client
            .post(api_v1_write_url, "test,t2=t22,f2=2 1642176000000000000")
            .unwrap();
        assert_eq!(
            resp.text().unwrap(),
            r#"{"error_code":"040004","error_message":"Error parsing message: unexpect end line start at '17'"}"#
        );

        let resp = check_response!(client.post(api_v1_sql_url, "select t1,f1 from test;"));
        assert_eq!(resp.text().unwrap(), "t1,f1\nt11,1\n");
    }

    //bug test for 1387
    {
        check_response!(client.post(api_v1_write_url, "ma,ta=a1 fa=1 1690510331000000000"));
        check_response!(client.post(api_v1_write_url, "ma,ta=a1 fb=2 1690510331000000000"));

        let resp = check_response!(client.post(api_v1_sql_url, "select * from ma"));
        assert_eq!(
            resp.text().unwrap(),
            "time,ta,fa,fb\n2023-07-28T02:12:11.000000000,a1,1.0,2.0\n",
        );
    }

    // bug test for #1311 #1459
    {
        check_response!(client.post(api_v1_sql_url, "CREATE TENANT tenant_a;"));
        check_response!(client.post(api_v1_sql_url, "CREATE USER user_a;"));
        check_response!(client.post(
            api_v1_sql_url,
            "ALTER TENANT tenant_a ADD USER user_a AS owner;",
        ));
    }
    {
        check_response!(client_user_a.post(api_v1_sql_url_tenant_a, "create database db1;"));
        check_response!(client_user_a.post(
            api_v1_sql_url_tenant_a,
            "CREATE TABLE db1.air_a (visibility DOUBLE,temperature DOUBLE,pressure DOUBLE,TAGS(station));",
        ));
        check_response!(client_user_a.post(
            api_v1_sql_url_tenant_a,
            "INSERT INTO db1.air_a (TIME, station, visibility, temperature, pressure) VALUES(1666165200290401000, 'XiaoMaiDao', 56, 69, 77);"
        ));
    }
    {
        check_response!(client.post(api_v1_sql_url, "drop tenant tenant_a;"));
        check_response!(client.post(api_v1_sql_url, "drop user user_a;"));
        check_response!(client.post(api_v1_sql_url, "CREATE TENANT tenant_a;"));
        check_response!(client.post(api_v1_sql_url, "CREATE USER user_a;"));
        check_response!(client.post(
            api_v1_sql_url,
            "ALTER TENANT tenant_a ADD USER user_a AS owner;",
        ));
    }
    {
        let resp = client_user_a
            .post(api_v1_sql_url_tenant_a, "select * from db1.air_a;")
            .unwrap();
        assert_eq!(
            resp.text().unwrap(),
            r#"{"error_code":"030019","error_message":"Table not found: \"tenant_a.db1.air_a\""}"#,
        );

        check_response!(client_user_a.post(api_v1_sql_url_tenant_a, "CREATE DATABASE db1;;"));
        check_response!(client_user_a.post(
            api_v1_sql_url_tenant_a,
            "CREATE TABLE db1.air_a (visibility DOUBLE,temperature DOUBLE,pressure DOUBLE,TAGS(station));"
        ));
        check_response!(client_user_a.post(
            api_v1_sql_url_tenant_a,
            "INSERT INTO db1.air_a (TIME, station, visibility, temperature, pressure) VALUES(1666165200290401000, 'XiaoMaiDao', 56, 69, 77);"
        ));
    }
}

#[test]
fn test_cnosdb_cli_start() {
    let mut ctx = E2eContext::new("client_tests", "client_start_test");
    let mut executor = ctx.build_executor(cluster_def::one_data(1));
    let host_port = executor.cluster_definition().data_cluster_def[0].http_host_port;
    let port = host_port.port().to_string();
    let api_v1_sql_url = &format!("http://{host_port}/api/v1/sql?db=public");

    executor.startup();

    //
    {
        let mut cli = CnosdbCliDef::with_args(&["--port", port.as_str()]).run();
        cli.write(&["1;\n"]).unwrap();
        let (stdout, stderr) = cli.read_after_write(Duration::from_secs(1)).unwrap();
        assert_eq!(
            stderr.lines().next().unwrap(),
            r#"422 Unprocessable Entity, details: {"error_code":"010009","error_message":"sql parser error: Expected an SQL statement, found: 1"}"#,
            "stdout: '{stdout}', stderr: '{stderr}'",
        );
    }

    let client = executor.case_context().data_client(0);

    //bug test for #1703
    {
        // Start cnosdb singleton with `auth_enabled = false`, alter password for root.
        check_response!(client.post(api_v1_sql_url, "drop user if exists user001",));
        sleep(Duration::from_secs(1));

        check_response!(client.post(api_v1_sql_url, "drop tenant if exists tenant001",));
        sleep(Duration::from_secs(1));

        check_response!(client.post(api_v1_sql_url, "create user user001",));
        sleep(Duration::from_secs(1));

        check_response!(client.post(api_v1_sql_url, "create tenant tenant001",));
        sleep(Duration::from_secs(1));

        check_response!(client.post(
            api_v1_sql_url,
            "alter tenant tenant001 add user user001 as owner",
        ));
        sleep(Duration::from_secs(1));

        let mut cli =
            CnosdbCliDef::with_args(&["-u", "user001", "-t", "tenant001", "--port", port.as_str()])
                .run();
        let test_data = get_workspace_dir().join("e2e_test/test_data/oceanic_station.txt");
        cli.write(&[format!("\\w {}\n", test_data.display())])
            .unwrap();
        let (stdout, stderr) = cli.read_after_write(Duration::from_secs(1)).unwrap();
        assert_eq!(
            stderr.lines().next().unwrap(),
            r#"422 Unprocessable Entity, body: {"error_code":"030017","error_message":"Database not found: \"public\""}"#,
            "stdout: '{stdout}', stderr: '{stderr}'",
        );
    }
}

//auto test about issue 1867
#[test]
fn test_cnosdb_cli_password_leak() {
    let mut ctx = E2eContext::new("client_tests", "test_cnosdb_cli_password_leak");

    {
        // cnosdb-cli does not support clear-text passwords in design。
        let cli = CnosdbCliDef::with_args(&["-p", "123"]).run();
        sleep(Duration::from_secs(1));
        let (stdout, stderr) = cli.read().unwrap();
        assert_eq!(
            stderr.lines().next().unwrap(),
            "error: unrecognized subcommand \'123\'",
            "stdout: '{stdout}', stderr: '{stderr}'",
        );
    }
    {
        let mut executor = ctx.build_executor(cluster_def::one_data(1));
        let port = executor.cluster_definition().data_cluster_def[0]
            .http_host_port
            .port()
            .to_string();

        executor.startup();

        let mut cli = CnosdbCliDef::with_args(&["--port", port.as_str()]).run();
        // Ignore cnosdb-cli startup messages.
        cli.skip();
        cli.write(&["show tables;\n"]).unwrap();
        let (stdout, stderr) = cli.read_after_write(Duration::from_secs(1)).unwrap();
        let expected = [
            "+------------+",
            "| table_name |",
            "+------------+",
            "+------------+",
            "Query took",
        ]
        .join("\n");
        assert!(
            stdout.starts_with(&expected),
            "stdout: '{stdout}', stderr: '{stderr}'",
        );
    }
}

/// Find the start and end index of a pattern in a string. If the pattern is not found, panic.
/// If start_pattern is None, the start index is 0.
/// If end_pattern is None, the end index is the end of the string.
fn find_in_str(s: &str, start_pattern: Option<&str>, end_pattern: Option<&str>) -> (usize, usize) {
    let start_idx = match start_pattern {
        Some(p) => match s.find(p) {
            Some(i) => i + p.len(),
            None => {
                panic!("Start pattern '{p}' not found in: '{s}'");
            }
        },
        None => 0,
    };
    let end_idx = match end_pattern {
        Some(p) => match s[start_idx..].find(p) {
            Some(i) => start_idx + i,
            None => {
                panic!("End pattern '{p}' not found in: '{s}'");
            }
        },
        None => s.len(),
    };
    (start_idx, end_idx)
}

/// Replace the substring in `src` that is between the start and end patterns specified in the `patterns` vector with the substring in `dst`.
/// The start and end patterns will not be replaced.
///
/// Use function `find_in_str()` to find the index of the start and end patterns.
fn replace_src_by_dst(
    mut src: String,
    dst: &str,
    patterns: Vec<(Option<&str>, Option<&str>)>,
) -> String {
    for (start_pattern, end_pattern) in patterns {
        let src_bounds = find_in_str(&src, start_pattern, end_pattern);
        let dst_bounds = find_in_str(dst, start_pattern, end_pattern);
        println!(
            "Replacing '{}' with '{}' in: '{src}'",
            &src[src_bounds.0..src_bounds.1],
            &dst[dst_bounds.0..dst_bounds.1],
        );
        src.replace_range(src_bounds.0..src_bounds.1, &dst[dst_bounds.0..dst_bounds.1]);
    }
    src
}

//auto test about issue 784
#[test]
fn client_join_generate_physical() {
    let mut ctx = E2eContext::new("client_tests", "client_join_generate_physical");
    let mut executor = ctx.build_executor(cluster_def::one_data(1));
    let host_port = executor.cluster_definition().data_cluster_def[0].http_host_port;
    let port = host_port.port().to_string();
    let api_v1_sql_url = &format!("http://{host_port}/api/v1/sql?db=public");
    let test_data_path = get_workspace_dir().join("e2e_test/test_data/oceanic_station.txt");

    executor.startup();

    let mut cli = CnosdbCliDef::with_args(&["--port", port.as_str()]).run();
    // Ignore cnosdb-cli startup messages.
    cli.skip();
    cli.write(&[&format!("\\w {}\n", test_data_path.display())])
        .unwrap();
    let (stdout, stderr) = cli.read_after_write(Duration::from_secs(1)).unwrap();
    assert!(
        stdout.starts_with("Query took"),
        "stdout: '{stdout}', stderr: '{stderr}'",
    );

    sleep(Duration::from_secs(1));

    let client = executor.case_context().data_client(0);
    let resp = check_response!(client.post(
        api_v1_sql_url,
        "WITH l as (
            SELECT date_trunc('day', time) AS day, avg (temperature) AS day_temperature, station
            FROM sea
            WHERE station = 'LianYunGang'
                AND time >= '2022-01-15T00:00:00'
                AND time < '2022-02-16T00:00:00'
            GROUP BY station, day
        ), x as (
            SELECT date_trunc('day', time) AS day, avg (speed) AS day_speed, station
            FROM wind
            WHERE station = 'XiaoMaiDao'
                AND time >= '2022-01-15T00:00:00'
                AND time < '2022-02-16T00:00:00'
            GROUP BY station, day
        )
        SELECT * FROM l JOIN x ON x.day= l.day"
    ));

    assert_eq!(
        resp.text().unwrap(),
        [
            "day,day_temperature,station,day,day_speed,station",
            "2022-02-06T00:00:00.000000000,66.94915254237289,LianYunGang,2022-02-06T00:00:00.000000000,65.55,XiaoMaiDao",
            "",
        ].join("\n"),
    );
}

#[test]
fn explain_time_count_tests() {
    let mut ctx = E2eContext::new("client_tests", "explain_time_count_tests");
    let mut executor = ctx.build_executor(cluster_def::one_data(1));
    let host_port = executor.cluster_definition().data_cluster_def[0].http_host_port;
    let api_v1_sql_url = &format!("http://{host_port}/api/v1/sql?db=public");

    executor.startup();

    let client = executor.case_context().data_client(0);
    check_response!(client.post(
        api_v1_sql_url,
        "CREATE TABLE IF NOT EXISTS m0(f0 STRING , TAGS(t0) );"
    ));

    check_response!(client.post(
        api_v1_sql_url,
        "INSERT m0(TIME, f0, t0) VALUES(30737363596320556,'2001-10-10','sd');",
    ));

    {
        let resp = check_response!(client.post(
            api_v1_sql_url,
            "explain select time from m0 where f0 >= '1997-01-31';"
        ));

        let mut expected_resp_lines = [
            r#"plan_type,plan"#,
            r#"logical_plan,"Projection: m0.time"#,
            r#"  TableScan: m0 projection=[time, f0], full_filters=[m0.f0 >= Utf8(""1997-01-31"")]""#,
            r#"physical_plan,"ProjectionExec: expr=[time@0 as time]"#,
            r#"  TskvExec: limit=None, predicate=ColumnDomains { column_to_domain: Some({Column { relation: None, name: ""f0"" }: Range(RangeValueSet { low_indexed_ranges: {Marker { data_type: Utf8, value: Some(Utf8(""1997-01-31"")), bound: Exactly }: Range { low: Marker { data_type: Utf8, value: Some(Utf8(""1997-01-31"")), bound: Exactly }, high: Marker { data_type: Utf8, value: None, bound: Below } }} })}) }, filter=Some(""f0@1 >= 1997-01-31""), split_num=1, projection=[time,f0]"#,
            r#"""#,
            "",
        ];
        let resp_string = resp.text().unwrap();
        let resp_lines = resp_string.split('\n').collect::<Vec<&str>>();
        assert_eq!(resp_lines.len(), expected_resp_lines.len());

        let expected_resp_lines_4 = replace_src_by_dst(
            expected_resp_lines[4].to_string(),
            resp_lines[4],
            vec![(Some("split_num="), Some(","))],
        );
        expected_resp_lines[4] = &expected_resp_lines_4;

        let expected_resp_string = expected_resp_lines.join("\n");
        assert_eq!(resp_string, expected_resp_string);
    }

    {
        let resp =
            check_response!(client.post(api_v1_sql_url, "explain select exact_count(*) from m0;"));

        let mut expected_resp_lines = [
            "plan_type,plan",
            "logical_plan,\"Aggregate: groupBy=[[]], aggr=[[COUNT(UInt8(1))]]",
            "  TableScan: m0 projection=[time]\"",
            "physical_plan,\"AggregateExec: mode=Final, gby=[], aggr=[COUNT(UInt8(1))]",
            "  CoalescePartitionsExec",
            "    AggregateExec: mode=Partial, gby=[], aggr=[COUNT(UInt8(1))]",
            "      RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            "        TskvExec: limit=None, predicate=ColumnDomains { column_to_domain: Some({}) }, filter=None, split_num=1, projection=[time]",
            "\"",
            "",
        ];
        let resp_string = resp.text().unwrap();
        let resp_lines = resp_string.split('\n').collect::<Vec<&str>>();
        assert_eq!(resp_lines.len(), expected_resp_lines.len());

        let expected_resp_lines_6 = replace_src_by_dst(
            expected_resp_lines[6].to_string(),
            resp_lines[6],
            vec![
                (Some("RoundRobinBatch("), Some(")")),
                (Some("input_partitions="), None),
            ],
        );
        expected_resp_lines[6] = &expected_resp_lines_6;

        let expected_resp_lines_7 = replace_src_by_dst(
            expected_resp_lines[7].to_string(),
            resp_lines[7],
            vec![(Some("split_num="), Some(","))],
        );
        expected_resp_lines[7] = &expected_resp_lines_7;

        let expected_resp_string = expected_resp_lines.join("\n");
        assert_eq!(resp_string, expected_resp_string);
    }
}

#[test]
fn test_cnosdb_cli_and_client() {
    let mut ctx = E2eContext::new("client_tests", "cnosdb_cli_and_client");
    let mut executor = ctx.build_executor(cluster_def::one_data(1));
    let host_port = executor.cluster_definition().data_cluster_def[0].http_host_port;
    let port = host_port.port().to_string();
    let api_v1_sql_url = &format!("http://{host_port}/api/v1/sql?db=public");

    executor.startup();

    // test for issue2019
    let mut cnosdb_cli: CnosdbCommandLineInterfaceHelper =
        CnosdbCliDef::with_args(&["--port", port.as_str()]).run();
    // Ignore cnosdb-cli startup messages.
    cnosdb_cli.skip();

    let client = executor.case_context().data_client(0);
    {
        // data prepare
        check_response!(client.post(
            api_v1_sql_url,
            "create table test(str_col string, tags(ta));"
        ));
        sleep(Duration::from_secs(1));
    }
    {
        cnosdb_cli
            .write(&["insert into test(str_col,ta)values('a1','str;str');\n"])
            .unwrap();
        sleep(Duration::from_secs(1));
    }
    {
        check_response!(client.post(
            api_v1_sql_url,
            "insert into test(str_col,ta)values('a2',';str');"
        ));
        sleep(Duration::from_secs(1));

        let resp = check_response!(client.post(
            api_v1_sql_url,
            "select str_col,ta from test order by str_col;"
        ));
        assert_eq!(resp.text().unwrap(), "str_col,ta\na1,str;str\na2,;str\n");
        sleep(Duration::from_secs(1));
    }
    {
        cnosdb_cli.skip();
        cnosdb_cli
            .write(&["select str_col,ta from test order by str_col;\n"])
            .unwrap();
        let (stdout, stderr) = cnosdb_cli.read_after_write(Duration::from_secs(1)).unwrap();
        let expected = [
            "+---------+---------+",
            "| str_col | ta      |",
            "+---------+---------+",
            "| a1      | str;str |",
            "| a2      | ;str    |",
            "+---------+---------+",
            "Query took",
        ]
        .join("\n");
        assert!(
            stdout.starts_with(&expected),
            "stdout: '{stdout}', stderr: '{stderr}'",
        );
    }
}

#[test]
fn test_cnosdb_cli_trim_cmd() {
    {
        let mut client = CnosdbCliDef::new().run();
        client.write(&["       \\?\t\t  \n"]).unwrap();
        let (stdout, stderr) = client.read_after_write(Duration::from_secs(1)).unwrap();
        assert!(
            stdout.contains("Query took"),
            "stdout: '{stdout}', stderr: '{stderr}'"
        );
    }
}

#[test]
fn cnosdb_cli_help_test() {
    {
        let cli = CnosdbCliDef::with_args(&["--help"]).run();
        let (stdout, stderr) = cli.read_after_write(Duration::from_secs(1)).unwrap();

        let expected = [
            "Command Line Client for Cnosdb.",
            "",
            "Usage: cnosdb-cli [OPTIONS] [COMMAND]",
            "",
            "Commands:",
            "  dump-ddl          Dump ddl to files, Support multi tenants",
            "  restore-dump-ddl  Restore database from files",
            "  help              Print this message or the help of the given subcommand(s)",
            "",
            "Options:",
            "  -H, --host <HOST>",
            "          Host of CnosDB server [default: localhost]",
            "  -P, --port <PORT>",
            "          Port of CnosDB server HTTP API [default: 8902]",
            "  -u, --user <USER>",
            "          Username to connect to CnosDB server [default: root]",
            "  -p, --password",
            "          Use password to connect to CnosDB server",
            "      --private-key-path <PRIVATE_KEY_PATH>",
            "          Rsa private key path for key pair authentication used to connect to the CnosDB",
            "  -d, --database <DATABASE>",
            "          Default database to connect to the CnosDB [default: public]",
            "  -t, --tenant <TENANT>",
            "          Default tenant to connect to the CnosDB [default: cnosdb]",
            "      --precision <PRECISION>",
            "          The precision of the unix timestamps, will be used as the url param 'precision' [possible values: ns, us, ms]",
            "      --target-partitions <TARGET_PARTITIONS>",
            "          Number of partitions for query execution. Increasing partitions can increase concurrency",
            "  -s, --stream-trigger-interval <STREAM_TRIGGER_INTERVAL>",
            "          Optionally, specify the micro batch stream trigger interval. e.g. once, 1m, 10s",
            "      --data-path <DATA_PATH>",
            "          Path to your data, default to current directory",
            "      --receive-data-encoding <RECEIVE_DATA_ENCODING>",
            "          HTTP response encoding. Support deflate, gzip, br, zstd",
            "      --send-data-encoding <SEND_DATA_ENCODING>",
            "          HTTP request encoding. Support deflate, gzip, br, zstd",
            "  -f, --file [<FILE>...]",
            "          Execute commands from file(s), then exit",
            "      --rc [<RC>...]",
            "          Run the provided files on startup instead of ~/.cnosdbrc",
            "      --format <FORMAT>",
            "          [default: table] [possible values: csv, tsv, table, json, nd-json]",
            "  -q, --quiet",
            "          Reduce printing other than the results and work quietly",
            "  -W, --write-line-protocol <FILE>",
            "          Write line protocol from file",
            "      --ssl",
            "          Use HTTPS connection",
            "      --unsafe-ssl",
            "          Allow unsafe HTTPS connections",
            "      --cacert <FILE>",
            "          Use the specified certificate file to verify the connection peer. The certificate(s) must be in PEM format",
            "      --proxy-url <URL>",
            "          Proxy URL, for HTTP or HTTPS requests",
            "      --chunked",
            "          Enable chunk mode, and CnosDB server uses http streaming output",
            "      --error-stop",
            "          Stop when an error is encounter",
            "      --process-cli-command",
            "          Enable client command",
            "  -h, --help",
            "          Print help",
            "  -V, --version",
            "          Print version",
            "",
        ].join("\n");

        assert_eq!(stdout, expected, "stdout: '{stdout}', stderr: '{stderr}'");
    }

    {
        let cli = CnosdbCliDef::with_args(&["dump-ddl", "--help"]).run();
        let (stdout, stderr) = cli.read_after_write(Duration::from_secs(1)).unwrap();
        let expected = [
            "Dump ddl to files, Support multi tenants",
            "",
            "Usage: cnosdb-cli dump-ddl [OPTIONS]",
            "",
            "Options:",
            "  -t, --tenant <TENANT>  Dump tenants",
            "  -h, --help             Print help",
            "",
        ]
        .join("\n");

        assert_eq!(stdout, expected, "stdout: '{stdout}', stderr: '{stderr}'");
    }

    {
        let cli = CnosdbCliDef::with_args(&["restore-dump-ddl", "--help"]).run();
        let (stdout, stderr) = cli.read_after_write(Duration::from_secs(1)).unwrap();
        let expected = [
            "Restore database from files",
            "",
            "Usage: cnosdb-cli restore-dump-ddl [OPTIONS] [FILES]...",
            "",
            "Arguments:",
            "  [FILES]...  Restore files",
            "",
            "Options:",
            "  -t, --tenant <TENANT>  Tenant wanna restore",
            "  -h, --help             Print help",
            "",
        ]
        .join("\n");

        assert_eq!(stdout, expected, "stdout: '{stdout}', stderr: '{stderr}'");
    }
}
