#![cfg(test)]
#![warn(dead_code)]
use std::io::{self, BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};

use serial_test::serial;

use crate::utils::{get_workspace_dir, kill_all, run_singleton, Client, CnosdbDataTestHelper};
use crate::{assert_response_is_ok, cluster_def};

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
        // get the stdio stream
        let stdin = self.subprocess.stdin.as_mut().take();
        let stdout = self.subprocess.stdout.as_mut().take();
        let stderr = self.subprocess.stderr.as_mut().take();

        if let Some(stdin) = stdin {
            // write input
            for input in inputs {
                stdin.write_all(input.as_bytes())?;
            }
            stdin.write_all("\\q".as_bytes())?;
            // stdin.
        }

        drop(self.subprocess.stdin.take());
        let mut res = String::new();
        if let Some(stdout) = stdout {
            let reader = BufReader::new(stdout);
            reader
                .lines()
                .skip(2)
                .map(|result| result.unwrap())
                .filter(|line| !line.is_empty())
                .for_each(|line| res.push_str(&line));
        }

        if !res.is_empty() {
            return Ok(res);
        }

        if let Some(stderr) = stderr {
            let reader = BufReader::new(stderr);
            reader
                .lines()
                .map(|result| result.unwrap())
                .filter(|line| line.contains("error_code"))
                .for_each(|line| res.push_str(&line));
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

        let resp = server
            .client
            .post(url, "ma,ta=a1 fa=1 1690510331000000000")
            .unwrap();

        assert_response_is_ok!(resp);

        let resp = server
            .client
            .post(url, "ma,ta=a1 fb=2 1690510331000000000")
            .unwrap();

        assert_response_is_ok!(resp);

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
        let resp = server.client.post(url, "CREATE TENANT tenant_a;").unwrap();

        assert_response_is_ok!(resp);

        let resp = server.client.post(url, "CREATE USER user_a;").unwrap();

        assert_response_is_ok!(resp);

        let resp = server
            .client
            .post(url, "ALTER TENANT tenant_a ADD USER user_a AS owner;")
            .unwrap();

        assert_response_is_ok!(resp);
    }
    {
        let url = "http://127.0.0.1:8902/api/v1/sql?tenant=tenant_a&db=public";

        let client = Client::with_auth("user_a".to_string(), None);

        let resp = client.post(url, "create database db1;").unwrap();

        assert_response_is_ok!(resp);

        let resp = client
        .post(url, "CREATE TABLE db1.air_a (visibility DOUBLE,temperature DOUBLE,pressure DOUBLE,TAGS(station));")
        .unwrap();

        assert_response_is_ok!(resp);

        let resp = client
        .post(url, "INSERT INTO db1.air_a (TIME, station, visibility, temperature, pressure) VALUES(1666165200290401000, 'XiaoMaiDao', 56, 69, 77);")
        .unwrap();

        assert_response_is_ok!(resp);
    }
    {
        let url = "http://127.0.0.1:8902/api/v1/sql?db=public";

        let resp = server.client.post(url, "drop tenant tenant_a;").unwrap();

        assert_response_is_ok!(resp);

        let resp = server.client.post(url, "drop user user_a;").unwrap();

        assert_response_is_ok!(resp);

        let resp = server.client.post(url, "CREATE TENANT tenant_a;").unwrap();

        assert_response_is_ok!(resp);

        let resp = server.client.post(url, "CREATE USER user_a;").unwrap();

        assert_response_is_ok!(resp);

        let resp = server
            .client
            .post(url, "ALTER TENANT tenant_a ADD USER user_a AS owner;")
            .unwrap();

        assert_response_is_ok!(resp);
    }
    {
        let url = "http://127.0.0.1:8902/api/v1/sql?tenant=tenant_a&db=public";

        let client = Client::with_auth("user_a".to_string(), None);

        let resp = client.post(url, "select * from db1.air_a;").unwrap();

        assert_eq!(
            resp.text().unwrap(),
            "{\"error_code\":\"010001\",\"error_message\":\"Datafusion: Error during planning: Table not found, tenant: tenant_a db: db1, table: air_a\"}",
        );

        let resp = client.post(url, "CREATE DATABASE db1;;").unwrap();

        assert_response_is_ok!(resp);

        let resp = client
        .post(url, "CREATE TABLE db1.air_a (visibility DOUBLE,temperature DOUBLE,pressure DOUBLE,TAGS(station));")
        .unwrap();

        assert_response_is_ok!(resp);

        let resp = client
        .post(url, "INSERT INTO db1.air_a (TIME, station, visibility, temperature, pressure) VALUES(1666165200290401000, 'XiaoMaiDao', 56, 69, 77);")
        .unwrap();

        assert_response_is_ok!(resp);
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

        _server
            .client
            .post(
                "http://127.0.0.1:8902/api/v1/sql?db=public",
                "drop tenant if exists tenant001",
            )
            .unwrap();

        _server
            .client
            .post(
                "http://127.0.0.1:8902/api/v1/sql?db=public",
                "create user user001",
            )
            .unwrap();

        _server
            .client
            .post(
                "http://127.0.0.1:8902/api/v1/sql?db=public",
                "create tenant tenant001",
            )
            .unwrap();

        let resp = _server
            .client
            .post(
                "http://127.0.0.1:8902/api/v1/sql?db=public",
                "alter tenant tenant001 add user user001 as owner",
            )
            .unwrap();

        assert_response_is_ok!(resp);

        let args = vec!["-u", "user001", "-t", "tenant001"];
        let mut client = ClientDef::new(args).run();

        let resp = client.write(&[&format!(
            "\\w {}/e2e_test/test_data/oceanic_station.txt\n",
            get_workspace_dir().as_os_str().to_str().unwrap()
        )]);

        if let Ok(resp) = resp {
            assert_eq!(
                resp,
                "422 Unprocessable Entity, body: {\"error_code\":\"050001\",\"error_message\":\"Meta request error: Database not found: \\\"public\\\"\"}"
            )
        }
    }
}
