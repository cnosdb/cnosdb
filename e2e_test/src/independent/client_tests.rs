#![cfg(test)]
#![warn(dead_code)]
use std::io::{self, BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::thread::sleep;
use std::time::Duration;

use serial_test::serial;

use crate::utils::{get_workspace_dir, kill_all, run_singleton, Client, CnosdbDataTestHelper};
use crate::{check_response, cluster_def};

// use for cnosdb-cli
struct ClientDef {
    exe_path: PathBuf,
    // test_dir:PathBuf,
    args: Vec<&'static str>,
}

impl ClientDef {
    fn new(
        // test_base_dir: impl AsRef<Path>,
        args: Vec<&'static str>,
    ) -> Self {
        let workspace_dir = get_workspace_dir();
        ClientDef {
            exe_path: workspace_dir
                .join("target")
                .join("debug")
                .join("cnosdb-cli"),
            // test_dir:test_base_dir.as_ref().to_path_buf(),
            args,
        }
    }

    fn run(&self) -> CnosDBClientHelper {
        let child = self.execute();

        CnosDBClientHelper { subprocess: child }
    }

    fn execute(&self) -> Child {
        Command::new(&self.exe_path)
            .args(&self.args)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("fail to start ")
    }
}

struct CnosDBClientHelper {
    subprocess: Child,
}

impl CnosDBClientHelper {
    fn write(&mut self, inputs: &[&str]) -> Result<String, io::Error> {
        let mut res = String::new();

        if let Some(mut stdin) = self.subprocess.stdin.take() {
            for input in inputs {
                stdin.write_all(input.as_bytes())?;
            }
            stdin.write_all("\\q".as_bytes())?;
        }

        drop(self.subprocess.stdin.take());

        if let Some(stdout) = self.subprocess.stdout.take() {
            let reader = BufReader::new(stdout);
            for line in reader
                .lines()
                .skip(2)
                .map_while(Result::ok)
                .filter(|line| !line.is_empty())
            {
                res.push_str(&line);
            }
        }

        if res.is_empty() {
            if let Some(stderr) = self.subprocess.stderr.take() {
                let reader = BufReader::new(stderr);
                for line in reader
                    .lines()
                    .map_while(Result::ok)
                    .filter(|line| line.contains("error_code") || line.contains("error: "))
                {
                    res.push_str(&line);
                }
            }
        }
        Ok(res)
    }
}

impl Drop for CnosDBClientHelper {
    fn drop(&mut self) {
        // kill client process
        self.subprocess.kill().expect("could not kill cli process");
    }
}

fn new_server(test_dir: &str) -> CnosdbDataTestHelper {
    kill_all();
    let test_dir = Path::new(test_dir);
    let data_node_def = &cluster_def::one_data(1);

    let _ = std::fs::remove_dir_all(test_dir);
    std::fs::create_dir_all(test_dir).unwrap();

    let data: CnosdbDataTestHelper = run_singleton(test_dir, data_node_def, false, true);
    data
}

#[test]
#[serial]
fn simple_test() {
    let test_dir = "/tmp/e2e_test/client_tests/simple_test";
    // just open the server
    let server = new_server(test_dir);

    //bug test for 1533
    {
        // data prepare
        let url = "http://127.0.0.1:8902/api/v1/sql?db=public";
        server
            .client
            .post(url, "create table test(f1 bigint, tags(t1));")
            .unwrap();

        server
            .client
            .post(url, "insert into test(time,t1,f1)values(now(),'t11',1);")
            .unwrap();

        let resp = server.client.post(url, "select t1,f1 from test;").unwrap();

        println!("test");
        assert_eq!(resp.text().unwrap(), "t1,f1\nt11,1\n");

        let resp = server
            .client
            .post(
                "http://127.0.0.1:8902/api/v1/write?db=public",
                "test,t2=t22,f2=2 1642176000000000000",
            )
            .unwrap();

        println!("{:?}", resp.text().unwrap());

        let resp = server.client.post(url, "select t1,f1 from test;").unwrap();

        assert_eq!(resp.text().unwrap(), "t1,f1\nt11,1\n");
    }

    //bug test for 1387
    {
        let url = "http://127.0.0.1:8902/api/v1/write?db=public";

        check_response!(server.client.post(url, "ma,ta=a1 fa=1 1690510331000000000"));

        check_response!(server.client.post(url, "ma,ta=a1 fb=2 1690510331000000000"));

        let url = "http://127.0.0.1:8902/api/v1/sql?db=public";
        let resp = server.client.post(url, "select * from ma").unwrap();

        assert_eq!(
            resp.text().unwrap(),
            "time,ta,fa,fb\n2023-07-28T02:12:11.000000000,a1,1.0,2.0\n"
        );
    }

    // bug test for #1311 #1459
    {
        let url = "http://127.0.0.1:8902/api/v1/sql?db=public";
        check_response!(server.client.post(url, "CREATE TENANT tenant_a;"));

        check_response!(server.client.post(url, "CREATE USER user_a;"));

        check_response!(server
            .client
            .post(url, "ALTER TENANT tenant_a ADD USER user_a AS owner;"));
    }
    {
        let url = "http://127.0.0.1:8902/api/v1/sql?tenant=tenant_a&db=public";

        let client = Client::with_auth("user_a".to_string(), None);

        check_response!(client.post(url, "create database db1;"));

        check_response!(client
            .post(url, "CREATE TABLE db1.air_a (visibility DOUBLE,temperature DOUBLE,pressure DOUBLE,TAGS(station));"));

        check_response!( client
            .post(url, "INSERT INTO db1.air_a (TIME, station, visibility, temperature, pressure) VALUES(1666165200290401000, 'XiaoMaiDao', 56, 69, 77);"));
    }
    {
        let url = "http://127.0.0.1:8902/api/v1/sql?db=public";

        check_response!(server.client.post(url, "drop tenant tenant_a;"));

        check_response!(server.client.post(url, "drop user user_a;"));

        check_response!(server.client.post(url, "CREATE TENANT tenant_a;"));

        check_response!(server.client.post(url, "CREATE USER user_a;"));

        check_response!(server
            .client
            .post(url, "ALTER TENANT tenant_a ADD USER user_a AS owner;"));
    }
    {
        let url = "http://127.0.0.1:8902/api/v1/sql?tenant=tenant_a&db=public";

        let client = Client::with_auth("user_a".to_string(), None);

        let resp = client.post(url, "select * from db1.air_a;").unwrap();

        assert_eq!(
            resp.text().unwrap(),
            "{\"error_code\":\"010001\",\"error_message\":\"Datafusion: Error during planning: Table not found, tenant: tenant_a db: db1, table: air_a\"}",
        );

        check_response!(client.post(url, "CREATE DATABASE db1;;"));

        check_response!(client
            .post(url, "CREATE TABLE db1.air_a (visibility DOUBLE,temperature DOUBLE,pressure DOUBLE,TAGS(station));"));

        check_response!(client
            .post(url, "INSERT INTO db1.air_a (TIME, station, visibility, temperature, pressure) VALUES(1666165200290401000, 'XiaoMaiDao', 56, 69, 77);"));
    }
}

#[test]
#[serial]
fn client_start_test() {
    let test_dir = "/tmp/e2e_test/client_tests/client_start_test";
    let _server = new_server(test_dir);

    let args = vec![];
    let client_execute = ClientDef::new(args);
    //
    {
        let mut client = client_execute.run();
        let resp = client.write(&["1;\n"]);

        if let Ok(resp) = resp {
            assert_eq!(
                resp,
                "422 Unprocessable Entity, details: {\"error_code\":\"010009\",\"error_message\":\"sql parser error: Expected an SQL statement, found: 1\"}"
            )
        }
    }

    //bug test for #1703
    {
        // Start cnosdb singleton with `auth_enabled = false`, alter password for root.
        _server
            .client
            .post(
                "http://127.0.0.1:8902/api/v1/sql?db=public",
                "drop user if exists user001",
            )
            .unwrap();

        sleep(Duration::from_secs(1));
        _server
            .client
            .post(
                "http://127.0.0.1:8902/api/v1/sql?db=public",
                "drop tenant if exists tenant001",
            )
            .unwrap();
        sleep(Duration::from_secs(1));

        _server
            .client
            .post(
                "http://127.0.0.1:8902/api/v1/sql?db=public",
                "create user user001",
            )
            .unwrap();
        sleep(Duration::from_secs(1));

        _server
            .client
            .post(
                "http://127.0.0.1:8902/api/v1/sql?db=public",
                "create tenant tenant001",
            )
            .unwrap();
        sleep(Duration::from_secs(1));

        check_response!(_server.client.post(
            "http://127.0.0.1:8902/api/v1/sql?db=public",
            "alter tenant tenant001 add user user001 as owner",
        ));
        sleep(Duration::from_secs(1));

        let args = vec!["-u", "user001", "-t", "tenant001"];
        let mut client = ClientDef::new(args).run();

        let resp = client.write(&[&format!(
            "\\w {}/e2e_test/test_data/oceanic_station.txt\n",
            get_workspace_dir().as_os_str().to_str().unwrap()
        )]);

        if let Ok(resp) = resp {
            assert_eq!(
                resp,
                "422 Unprocessable Entity, body: {\"error_code\":\"030017\",\"error_message\":\"Database not found: \\\"public\\\"\"}"
            )
        }
    }
}

//auto test about issue 1867
#[test]
#[serial]
fn client_password_leak_test() {
    let test_dir = "/tmp/e2e_test/client_tests/client_password_leak_test";
    let _server = new_server(test_dir);

    let args = vec!["-p", "123"];
    let client_execute = ClientDef::new(args);
    let mut client = client_execute.run();
    let resp = client.write(&["show tables;\n"]);
    if let Ok(resp) = resp {
        assert_eq!(resp, "error: unrecognized subcommand \'123\'")
    }

    {
        let args = vec![];
        let client_execute = ClientDef::new(args);
        let mut client = client_execute.run();
        let resp = client.write(&["show tables;\n"]);
        if let Ok(resp) = resp {
            assert_eq!(
                &resp[..56],
                "+------------+| table_name |+------------++------------+"
            )
        }
    }
}

//auto test about issue 784
#[test]
#[serial]
fn client_join_generate_physical() {
    let test_dir = "/tmp/e2e_test/client_tests/client_join_generate_physical";
    let _server = new_server(test_dir);
    let args = vec![];
    let client_execute = ClientDef::new(args);
    let mut client = client_execute.run();

    let resp = client.write(&[&format!(
        "\\w {}/e2e_test/test_data/oceanic_station.txt\n",
        get_workspace_dir().as_os_str().to_str().unwrap()
    )]);

    if let Ok(resp) = resp {
        assert_eq!(&resp[0..10], "Query took")
    }

    sleep(Duration::from_secs(1));
    let resp = _server
        .client
        .post("http://127.0.0.1:8902/api/v1/sql?db=public", "WITH l as (SELECT date_trunc('day', time) AS day, avg (temperature) AS day_temperature, station
        FROM sea
        WHERE station = 'LianYunGang'
        AND time >= '2022-01-15T00:00:00'
        AND time
        < '2022-02-16T00:00:00'
        GROUP BY station, day),
        x as
        (
        SELECT date_trunc('day', time) AS day, avg (speed) AS day_speed, station
        FROM wind
        WHERE station = 'XiaoMaiDao'
        AND time >= '2022-01-15T00:00:00'
        AND time
        < '2022-02-16T00:00:00'
        GROUP BY station, day)
        SELECT * FROM l JOIN x ON x.day= l.day")
        .unwrap();

    assert_eq!(
        resp.text().unwrap(),
        "day,day_temperature,station,day,day_speed,station\n2022-02-06T00:00:00.000000000,66.94915254237289,LianYunGang,2022-02-06T00:00:00.000000000,65.55,XiaoMaiDao\n"
    );
}

#[test]
#[serial]
fn explain_time_count_tests() {
    let test_dir = "/tmp/e2e_test/client_tests/explain_time_count_tests";
    let server = new_server(test_dir);

    {
        let url = "http://127.0.0.1:8902/api/v1/sql?db=public";
        server
            .client
            .post(url, "CREATE TABLE IF NOT EXISTS m0(f0 STRING , TAGS(t0) );")
            .unwrap();

        server
            .client
            .post(
                url,
                "INSERT m0(TIME, f0, t0) VALUES(30737363596320556,'2001-10-10','sd');",
            )
            .unwrap();

        let resp = server
            .client
            .post(url, "explain select time from m0 where f0 >= '1997-01-31';")
            .unwrap();

        let resp = resp.text().unwrap();
        let result="plan_type,plan\nlogical_plan,\"Projection: m0.time\n  Filter: m0.f0 >= Utf8(\"\"1997-01-31\"\")\n    TableScan: m0 projection=[time, f0], partial_filters=[m0.f0 >= Utf8(\"\"1997-01-31\"\")]\"\nphysical_plan,\"ProjectionExec: expr=[time@0 as time]\n  CoalesceBatchesExec: target_batch_size=8192\n    FilterExec: f0@1 >= 1997-01-31\n      RepartitionExec: partitioning=RoundRobinBatch(3), input_partitions=7\n        TskvExec: limit=None, predicate=ColumnDomains { column_to_domain: Some({Column { relation: None, name: \"\"f0\"\" }: Range(RangeValueSet { low_indexed_ranges: {Marker { data_type: Utf8, value: Some(Utf8(\"\"1997-01-31\"\")), bound: Exactly }: Range { low: Marker { data_type: Utf8, value: Some(Utf8(\"\"1997-01-31\"\")), bound: Exactly }, high: Marker { data_type: Utf8, value: None, bound: Below } }} })}) }, filter=Some(\"\"f0@1 >= 1997-01-31\"\"), split_num=3, projection=[time,f0]\n\"\n";
        let mut result_string = result.to_string();
        let resp_chars: Vec<char> = resp.chars().collect();
        //modify three parameters that are not associated.
        result_string.replace_range(365..366, &resp_chars[365].to_string());
        result_string.replace_range(386..387, &resp_chars[386].to_string());
        result_string.replace_range(841..842, &resp_chars[841].to_string());

        assert_eq!(resp, result_string);
        let resp = server
            .client
            .post(url, "explain select COUNT(*) from m0;")
            .unwrap();

        let resp = resp.text().unwrap();
        let result="plan_type,plan\nlogical_plan,\"Aggregate: groupBy=[[]], aggr=[[COUNT(UInt8(1))]]\n  TableScan: m0 projection=[time]\"\nphysical_plan,\"AggregateExec: mode=Final, gby=[], aggr=[COUNT(UInt8(1))]\n  CoalescePartitionsExec\n    AggregateExec: mode=Partial, gby=[], aggr=[COUNT(UInt8(1))]\n      RepartitionExec: partitioning=RoundRobinBatch(1), input_partitions=7\n        TskvExec: limit=None, predicate=ColumnDomains { column_to_domain: Some({}) }, filter=None, split_num=3, projection=[time]\n\"\n";
        let mut result_string = result.to_string();
        let resp_chars: Vec<char> = resp.chars().collect();

        result_string.replace_range(328..329, &resp_chars[328].to_string());
        result_string.replace_range(349..350, &resp_chars[349].to_string());
        result_string.replace_range(460..461, &resp_chars[460].to_string());

        assert_eq!(resp, result_string);
    }
}

#[test]
#[serial]
fn split_test() {
    // test for issue2019
    let test_dir = "/tmp/e2e_test/client_tests/split_test";
    // just open the server
    let server = new_server(test_dir);

    {
        // data prepare
        let url = "http://127.0.0.1:8902/api/v1/sql?db=public";
        server
            .client
            .post(url, "create table test(str_col string, tags(ta));")
            .unwrap();
        sleep(Duration::from_secs(1));
    }
    {
        let args = vec![];
        let client_execute = ClientDef::new(args);
        {
            let mut client = client_execute.run();
            client
                .write(&["insert into test(str_col,ta)values('a1','str;str');\n"])
                .unwrap();
        }
        sleep(Duration::from_secs(1));
    }
    {
        let url = "http://127.0.0.1:8902/api/v1/sql?db=public";
        check_response!(server
            .client
            .post(url, "insert into test(str_col,ta)values('a2',';str');"));
        sleep(Duration::from_secs(1));

        let resp = server
            .client
            .post(url, "select str_col,ta from test order by str_col;")
            .unwrap();
        sleep(Duration::from_secs(1));

        assert_eq!(resp.text().unwrap(), "str_col,ta\na1,str;str\na2,;str\n");
    }
}

#[serial]
#[test]
fn cmd_trim_test() {
    let test_dir = "/tmp/e2e_test/client_tests/cmd_trim_test";
    let _server = new_server(test_dir);

    let args = vec![];
    let client_execute = ClientDef::new(args);
    {
        let mut client = client_execute.run();
        let resp = client.write(&["       \\?\n"]);

        if let Ok(resp) = resp {
            assert!(resp.contains("Query took"));
        }
    }
}

#[serial]
#[test]
fn client_help_test() {
    let test_dir = "/tmp/e2e_test/client_tests/cmd_trim_test";
    let _server = new_server(test_dir);

    {
        let args = vec!["--help"];
        let client_execute = ClientDef::new(args);

        let mut client = client_execute.run();
        let resp = client.write(&[]);

        assert_eq!(resp.unwrap(), "Usage: cnosdb-cli [OPTIONS] [COMMAND]Commands:  dump-ddl          Dump ddl to files, Support multi tenants  restore-dump-ddl  Restore database from files  help              Print this message or the help of the given subcommand(s)Options:  -H, --host <HOST>          Host of CnosDB server [default: localhost]  -P, --port <PORT>          Port of CnosDB server HTTP API [default: 8902]  -u, --user <USER>          Username to connect to CnosDB server [default: root]  -p, --password          Use password to connect to CnosDB server      --private-key-path <PRIVATE_KEY_PATH>          Rsa private key path for key pair authentication used to connect to the CnosDB  -d, --database <DATABASE>          Default database to connect to the CnosDB [default: public]  -t, --tenant <TENANT>          Default tenant to connect to the CnosDB [default: cnosdb]      --precision <PRECISION>          The precision of the unix timestamps, will be used as the url param 'precision' [possible values: ns, us, ms]      --target-partitions <TARGET_PARTITIONS>          Number of partitions for query execution. Increasing partitions can increase concurrency  -s, --stream-trigger-interval <STREAM_TRIGGER_INTERVAL>          Optionally, specify the micro batch stream trigger interval. e.g. once, 1m, 10s       --data-path <DATA_PATH>          Path to your data, default to current directory      --receive-data-encoding <RECEIVE_DATA_ENCODING>          HTTP response encoding. Support deflate, gzip, br, zstd      --send-data-encoding <SEND_DATA_ENCODING>          HTTP request encoding. Support deflate, gzip, br, zstd  -f, --file [<FILE>...]          Execute commands from file(s), then exit      --rc [<RC>...]          Run the provided files on startup instead of ~/.cnosdbrc       --format <FORMAT>          [default: table] [possible values: csv, tsv, table, json, nd-json]  -q, --quiet          Reduce printing other than the results and work quietly  -W, --write-line-protocol <FILE>          Write line protocol from file      --ssl          Use HTTPS connection      --unsafe-ssl          Allow unsafe HTTPS connections      --cacert <FILE>          Use the specified certificate file to verify the connection peer. The certificate(s) must be in PEM format      --chunked          Enable chunk mode, and CnosDB server uses http streaming output      --error-stop          Stop when an error is encounter      --process-cli-command          Enable client command  -h, --help          Print help  -V, --version          Print version");
    }

    {
        let args = vec!["dump-ddl", "--help"];
        let client_execute = ClientDef::new(args);

        let mut client = client_execute.run();
        let resp = client.write(&[]);

        assert_eq!(resp.unwrap(), "Usage: cnosdb-cli dump-ddl [OPTIONS]Options:  -t, --tenant <TENANT>  Dump tenants  -h, --help             Print help");
    }

    {
        let args = vec!["restore-dump-ddl", "--help"];
        let client_execute = ClientDef::new(args);

        let mut client = client_execute.run();
        let resp = client.write(&[]);

        assert_eq!(resp.unwrap(), "Usage: cnosdb-cli restore-dump-ddl [OPTIONS] [FILES]...Arguments:  [FILES]...  Restore filesOptions:  -t, --tenant <TENANT>  Tenant wanna restore  -h, --help             Print help");
    }
}
