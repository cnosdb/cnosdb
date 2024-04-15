#![cfg(test)]
#![warn(dead_code)]
use std::io::{self, BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};

use serial_test::serial;

use crate::utils::{get_workspace_dir, kill_all, run_singleton, CnosdbDataTestHelper};
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
fn split_test() {
    // test for issue2019
    let test_dir = "/tmp/e2e_test/client_tests/split_test";
    // just open the server
    let server = new_server(test_dir);
    {
        // data prepare
        let url = "http://127.0.0.1:8902/api/v1/sql?db=public";
        let resp = server
            .client
            .post(url, "create table test(str_col string, tags(ta));")
            .unwrap();

        assert_response_is_ok!(resp);
    }
    {
        let args = vec![];
        let client_execute = ClientDef::new(args);
        {
            let mut client = client_execute.run();
            client
                .write(&["insert into test(time, str_col,ta)values(now(), 'a1','str;str');\n"])
                .unwrap();
        }
    }
    {
        let url = "http://127.0.0.1:8902/api/v1/sql?db=public";
        let resp = server
            .client
            .post(
                url,
                "insert into test(time, str_col,ta)values(now(), 'a2',';str');",
            )
            .unwrap();

        assert_response_is_ok!(resp);

        let resp = server
            .client
            .post(url, "select str_col,ta from test order by str_col;")
            .unwrap();
        assert_eq!(resp.text().unwrap(), "str_col,ta\na1,str;str\na2,;str\n");
    }
}
