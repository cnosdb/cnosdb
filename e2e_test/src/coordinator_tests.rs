#![cfg(feature = "coorfinator_e2e_test")]
#![allow(dead_code)]
use std::path::Path;
use std::process::{Child, Command, Output, Stdio};
use std::sync::atomic::{self, AtomicBool};
use std::sync::Arc;
use std::time::Duration;
use std::{io, thread};

use meta::client;
use meta::store::command;
use models::schema::{DatabaseSchema, Tenant};
use sysinfo::{ProcessExt, System, SystemExt};

#[cfg(feature = "coorfinator_e2e_test")]
#[cfg(test)]
mod tests {

    use meta::client;
    use meta::store::command;
    use models::schema::{DatabaseOptions, DatabaseSchema, Duration, Precision};

    // use tonic::body;
    use super::*;
    #[cfg(feature = "coorfinator_e2e_test")]
    #[tokio::test]
    #[ignore = "for debug test cases only"]
    async fn test_20230602_1551() {
        let output = exec_curl("curl www.baidu.com", "").unwrap();
        // let output = query("tenant1", "tenant1db1");
        println!(
            "status: {} \nstdout: {} \nstderr: {}",
            output.status,
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr),
        );
    }

    #[cfg(feature = "coorfinator_e2e_test")]
    #[tokio::test]
    #[ignore = "for debug test cases only"]
    async fn test_20230602_1638() {
        clean_env();
        start_cluster();
        // let opt: Output = start_cluster();
        // assert!(opt.status.code() == Some(0));
        prepare().await;

        // std::thread::sleep(std::time::Duration::from_secs(3600));

        let curl = format!(
            r#"curl -u root: -XPOST
            http://127.0.0.1:8902/api/v1/write?tenant=tenant{}&db=tenant{}db1
            -w %{{http_code}} -s -o /dev/null"#,
            1, 1
        );
        println!("{curl}");
        let body = format!("tb1,tag1=tag1,tag2=tag2 field1={}", 1);
        let output = exec_curl(&curl, &body).unwrap();
        println!("output: {:?}", output);
        println!("status: {}", output.status);
        println!("stdout: {:?}", String::from_utf8_lossy(&output.stdout));
        println!("stderr: {}", String::from_utf8_lossy(&output.stderr));
    }

    #[tokio::test]
    async fn test_multi_tenants_write_data() {
        // clean env
        clean_env();
        // start cluster
        start_cluster();
        // create  tenants
        prepare().await;
        if !prepare_data().await {
            eprintln!("Failed to prepare data");
            std::process::exit(1);
        }
        for i in 0..5 {
            let tenant = format!("tenant{}", i);
            let db = format!("tenant{}db1", i);
            let output = query(&tenant, &db);
            let result_csv = String::from_utf8_lossy(&output.stdout);
            println!("Result CSV: {}", &result_csv);
            let line_10 = result_csv.lines().nth(6);
            assert_eq!(line_10, Some("100"));
        }

        // clean env
        clean_env();
    }

    #[tokio::test]
    async fn test_replica() {
        // clean env
        clean_env();
        // start cluster
        start_cluster();
        // database option schema
        let dboption = DatabaseOptions::new(
            Duration::new("365"),
            Some(2),
            Duration::new("365"),
            Some(2),
            Some(Precision::NS),
        );
        let dbschema = DatabaseSchema::new_with_options("cnosdb", "db1", dboption);
        let cli = client::MetaHttpClient::new("127.0.0.1:8901".to_string());

        let req = command::WriteCommand::CreateDB(
            "cluster_xxx".to_string(),
            "cnosdb".to_string(),
            dbschema,
        );
        // create db
        let rsp = cli
            .write::<command::TenaneMetaDataResp>(&req)
            .await
            .unwrap();
        println!("rsp : {:?}", rsp);
        println!("rsp : {:?}", rsp.status.code);
        assert!(rsp.status.code == 0);
        let cmd = "curl http://127.0.0.1:8901/debug";
        let output = exec_curl(cmd, "").unwrap();
        assert!(output.status.code() == Some(0));
        print!("output: {}", String::from_utf8_lossy(&output.stdout));
        assert!(String::from_utf8_lossy(&output.stdout).contains("\"replica\":2"));
        // todo: check replica
        // clean env
        clean_env();
    }

    #[tokio::test]
    async fn test_shard() {
        // clean env
        clean_env();
        // start cluster
        start_cluster();
        // database option schema
        let dboption = DatabaseOptions::new(
            Duration::new("365"),
            Some(2),
            Duration::new("365"),
            Some(2),
            Some(Precision::NS),
        );
        let dbschema = DatabaseSchema::new_with_options("cnosdb", "db1", dboption);
        let cli = client::MetaHttpClient::new("127.0.0.1:8901".to_string());

        let req = command::WriteCommand::CreateDB(
            "cluster_xxx".to_string(),
            "cnosdb".to_string(),
            dbschema,
        );
        // create db
        let _rsp = cli
            .write::<command::TenaneMetaDataResp>(&req)
            .await
            .unwrap();
        let cmd = "curl http://127.0.0.1:8901/debug";
        let output = exec_curl(cmd, "").unwrap();
        assert!(output.status.code() == Some(0));
        // let api = "http://127.0.0.1:8901/debug".to_string();
        // let output = Command::new("curl")
        //     .args([&api])
        //     .output()
        //     .expect("failed to execute process");
        assert!(output.status.code() == Some(0));
        print!("output: {}", String::from_utf8_lossy(&output.stdout));
        assert!(String::from_utf8_lossy(&output.stdout).contains("\"shard_num\":2"));

        // clean env
        clean_env();
    }

    #[tokio::test]
    async fn test_ttl() {
        // clean env
        clean_env();
        // start cluster
        start_cluster();
        let dboption = DatabaseOptions::new(
            Duration::new("1"),
            Some(2),
            Duration::new("365"),
            Some(2),
            Some(Precision::NS),
        );
        let dbschema = DatabaseSchema::new_with_options("cnosdb", "db1", dboption);
        let cli = client::MetaHttpClient::new("127.0.0.1:8901".to_string());

        let req = command::WriteCommand::CreateDB(
            "cluster_xxx".to_string(),
            "cnosdb".to_string(),
            dbschema,
        );
        // create db
        let _rsp = cli
            .write::<command::TenaneMetaDataResp>(&req)
            .await
            .unwrap();
        let cmd = "curl http://127.0.0.1:8901/debug";
        let output = exec_curl(cmd, "").unwrap();
        assert!(output.status.code() == Some(0));
        print!("output: {}", String::from_utf8_lossy(&output.stdout));
        assert!(String::from_utf8_lossy(&output.stdout).contains("\"time_num\":1"));
        // write data
        let cmd = "curl -i -u root: -XPOST http://127.0.0.1:8902/api/v1/write?db=db1";
        let body = "tb1,tag1=tag1,tag2=tag2 field1=1 1683259054000000000";
        let output = exec_curl(cmd, body).unwrap();
        assert!(output.status.code() == Some(0));
        assert!(
            String::from_utf8_lossy(&output.stdout).contains("write expired time data not permit")
        );
        let cmd = "curl -i -u root: -XPOST http://127.0.0.1:8902/api/v1/write?db=db1";
        let body = "tb1,tag1=tag1,tag2=tag2 field1=2";
        let output = exec_curl(cmd, body).unwrap();
        assert!(output.status.code() == Some(0));
        print!("output: {}", String::from_utf8_lossy(&output.stdout));
        assert!(String::from_utf8_lossy(&output.stdout).contains("200 OK"));
        // clean env
        clean_env();
    }

    #[tokio::test]
    async fn test_balance() {
        // clean env
        clean_env();
        // start cluster
        start_cluster();
        // database option schema
        let dboption = DatabaseOptions::new(
            Duration::new("1"),
            Some(2),
            Duration::new("365"),
            Some(2),
            Some(Precision::NS),
        );
        let dbschema = DatabaseSchema::new_with_options("cnosdb", "db1", dboption);
        let cli = client::MetaHttpClient::new("127.0.0.1:8901".to_string());

        let req = command::WriteCommand::CreateDB(
            "cluster_xxx".to_string(),
            "cnosdb".to_string(),
            dbschema,
        );
        // create db
        let _rsp = cli
            .write::<command::TenaneMetaDataResp>(&req)
            .await
            .unwrap();

        let api = "http://127.0.0.1:8902/api/v1/write?db=db1".to_string();
        println!("before write data");
        for j in 0..10 {
            println!("writeing data");
            let body = format!("tb1,tag1=tag1,tag2=tag2 field1={}", j);
            println!("body: {}", &body);
            let cmd = format!("curl -i -u root: -XPOST {}", &api);
            println!("cmd: {}", &cmd);
            let output = exec_curl(&cmd, &body).unwrap();
            println!("status: {:?}", output);
        }
        println!("after write data");
        // todo check balance
        // clean env
        clean_env();
    }
}

#[allow(dead_code)]
async fn prepare() {
    for i in 0..5 {
        let tenant = format!("tenant{}", i);
        let db = format!("tenant{}db1", i);
        let req = command::WriteCommand::CreateTenant(
            "cluster_xxx".to_string(),
            tenant.clone(),
            models::schema::TenantOptions::default(),
        );

        let cli = client::MetaHttpClient::new("127.0.0.1:8901".to_string());
        // create tenant
        let _rsp = cli
            .write::<command::CommonResp<Tenant>>(&req)
            .await
            .unwrap();
        println!("create tenant rsp: {:?}", _rsp);
        // db req
        let req = command::WriteCommand::CreateDB(
            "cluster_xxx".to_string(),
            tenant.clone(),
            DatabaseSchema::new(&tenant, &db),
        );
        thread::sleep(Duration::from_secs(3));
        println!("db req{:?}", req);
        // create db
        let rsp = cli
            .write::<command::TenaneMetaDataResp>(&req)
            .await
            .unwrap();

        println!("create database rsp: {:?}", rsp);
    }
}

#[allow(dead_code)]
fn kill_process(process_name: &str) {
    let system = System::new_all();
    for (pid, process) in system.processes() {
        if process.name() == process_name {
            println!("{}: {}", pid, process.name());
            let output = Command::new("kill")
                .args(["-9", &(pid.to_string())])
                .output()
                .expect("failed to execute process");
            println!("status: {}", output.status);
        }
    }
}

// clean env
#[allow(dead_code)]
fn clean_env() {
    println!("Cleaning environment...");
    kill_process("cnosdb");
    kill_process("cnosdb-meta");
    let _ = std::fs::remove_dir_all("/tmp/cnosdb");
    println!("Clean environment completed.");
}

// prepare data
#[allow(dead_code)]
async fn prepare_data() -> bool {
    let mut handles: Vec<thread::JoinHandle<()>> = vec![];
    let has_failed = Arc::new(AtomicBool::new(false));
    for i in 0..5 {
        let curl = format!(
            r#"curl -u root: -XPOST -w %{{http_code}} -s -o /dev/null
            http://127.0.0.1:8902/api/v1/write?tenant=tenant{}&db=tenant{}db1"#,
            i, i
        );
        let has_failed = has_failed.clone();
        let handle: thread::JoinHandle<()> = thread::spawn(move || {
            println!("thread {} started", i);
            for j in 0..100 {
                if has_failed.load(atomic::Ordering::SeqCst) {
                    break;
                }
                let body = format!("tb1,tag1=tag1,tag2=tag2 field1={}", j);
                let mut write_fail_count = 0;
                while write_fail_count < 3 {
                    let output = match exec_curl(&curl, &body) {
                        Ok(o) => o,
                        Err(e) => {
                            write_fail_count += 1;
                            eprintln!("Failed to execute curl process: {}", e);
                            continue;
                        }
                    };
                    println!(
                        "status: {} \nstdout: {} \nstderr: {}",
                        output.status,
                        String::from_utf8_lossy(&output.stdout),
                        String::from_utf8_lossy(&output.stderr),
                    );
                    if output.stdout == b"200" {
                        break;
                    } else {
                        eprintln!("Received unexpected ouput: {}", unsafe {
                            std::str::from_utf8_unchecked(&output.stdout)
                        });
                        write_fail_count += 1;
                    }
                }
                if write_fail_count >= 3 {
                    eprintln!("Failed to write '{}' after retried 3 times", &body);
                    has_failed.store(true, atomic::Ordering::SeqCst);
                    break;
                }
            }
        });
        handles.push(handle);
    }
    for handle in handles {
        handle.join().unwrap();
    }
    !has_failed.load(atomic::Ordering::SeqCst)
}

// query data
#[allow(dead_code)]
fn query(tenant: &str, db: &str) -> Output {
    let curl = format!(
        r#"curl -i -u root: -XPOST
        http://127.0.0.1:8902/api/v1/sql?tenant={}&db={}"#,
        tenant, db
    );
    let body = "select count(*) from tb1";
    exec_curl(&curl, body).unwrap()
}

#[test]
#[ignore = "for debug test cases only"]
fn test_20230602_1551() {
    let output = query("tenant", "db");
    println!(
        "status: {} \nstdout: {} \nstderr: {}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
}

// execute curl command
fn exec_curl(cmd: &str, body: &str) -> io::Result<Output> {
    let cmd_args = cmd
        .split(&[' ', '\n', '\r'])
        .filter(|s| !s.is_empty())
        .collect::<Vec<&str>>();
    let mut command = Command::new(cmd_args[0]);
    if body.is_empty() {
        command.args(cmd_args[1..].iter());
    } else {
        command.args(cmd_args[1..].iter().chain(["-d", body].iter()));
    }
    println!("Executing command 'curl': {:?}", command);
    command.output()
}

// wait cnosdb startup, check ping api
fn wait_cnosdb_startup(host: &str) {
    let pint_api = format!("http://{host}/api/v1/ping");
    let startup_time = std::time::Instant::now();
    loop {
        if let Ok(output) = exec_curl(format!("curl {pint_api}").as_str(), "") {
            if output.status.success() {
                break;
            }
            eprintln!(
                "Execution 'curl {pint_api}' returned failure after {:?} seconds",
                startup_time.elapsed().as_secs()
            );
            thread::sleep(Duration::from_secs(3));
        } else {
            eprintln!(
                "Execution 'curl {pint_api}' failed after {:?} seconds",
                startup_time.elapsed().as_secs()
            );
            thread::sleep(Duration::from_secs(1));
        }
    }
}

// start meta node
fn start_meta<P: AsRef<Path>>(workspace_dir: P) -> [Child; 3] {
    let workspace_dir = workspace_dir.as_ref();
    let mut cargo_build = Command::new("cargo");
    let output = cargo_build
        .current_dir(workspace_dir)
        .args(["build", "--package", "meta"])
        .output()
        .expect("failed to execute cargo build");
    if !output.status.success() {
        panic!("Failed to build cnosdb-meta: {:?}", output);
    }

    let meta_exec = format!("{}/target/debug/cnosdb-meta", workspace_dir.display());
    println!("cnosdb-meta executable: {meta_exec}");
    let meta_config_dir = format!("{}/meta/config", workspace_dir.display());
    println!("cnosdb-meta config: {meta_config_dir}");
    let meta1 = Command::new(&meta_exec)
        .args(["-c", format!("{meta_config_dir}/config_8901.toml").as_str()])
        .stderr(Stdio::null())
        .stdout(Stdio::null())
        .spawn()
        .expect("failed to execute cnosdb-meta");
    let meta2 = Command::new(&meta_exec)
        .args(["-c", format!("{meta_config_dir}/config_8911.toml").as_str()])
        .stderr(Stdio::null())
        .stdout(Stdio::null())
        .spawn()
        .expect("failed to execute cnosdb-meta");
    let meta3 = Command::new(meta_exec)
        .args(["-c", format!("{meta_config_dir}/config_8921.toml").as_str()])
        .stderr(Stdio::null())
        .stdout(Stdio::null())
        .spawn()
        .expect("failed to execute cnosdb-meta");
    thread::sleep(Duration::from_secs(3));

    Command::new("curl")
        .args([
            "-s",
            "127.0.0.1:8901/init",
            "-H",
            "Content-Type: application/json",
            "-d",
            "{}",
        ])
        .output()
        .expect("failed to execute process");
    thread::sleep(Duration::from_secs(1));
    Command::new("curl")
        .args([
            "-s",
            "127.0.0.1:8901/add-learner",
            "-H",
            "Content-Type: application/json",
            "-d",
            "[2, \"127.0.0.1:8911\"]",
        ])
        .output()
        .expect("failed to execute process");
    thread::sleep(Duration::from_secs(1));
    Command::new("curl")
        .args([
            "-s",
            "127.0.0.1:8901/add-learner",
            "-H",
            "Content-Type: application/json",
            "-d",
            "[3, \"127.0.0.1:8921\"]",
        ])
        .output()
        .expect("failed to execute process");
    thread::sleep(Duration::from_secs(1));
    Command::new("curl")
        .args([
            "-s",
            "127.0.0.1:8901/8901/change-membership",
            "-H",
            "Content-Type: application/json",
            "-d",
            "[1, 2, 3]",
        ])
        .output()
        .expect("failed to execute process");
    thread::sleep(Duration::from_secs(1));

    [meta1, meta2, meta3]
}

// start data node
fn start_data<P: AsRef<Path>>(workspace_dir: P) -> [Child; 2] {
    let workspace_dir = workspace_dir.as_ref();
    let mut cargo_build = Command::new("cargo");
    let output = cargo_build
        .current_dir(workspace_dir)
        .args(["build", "--bin", "cnosdb"])
        .output()
        .expect("failed to execute cargo build");
    if !output.status.success() {
        panic!("Failed to build cnosdb: {:?}", output)
    }

    let data_exec = format!("{}/target/debug/cnosdb", workspace_dir.display());
    println!("cnosdb executable: {data_exec}");
    let data_config_dir = format!("{}/config", workspace_dir.display());
    println!("cnosdb config: {data_config_dir}");
    let data1 = Command::new(&data_exec)
        .args([
            "run",
            "--config",
            format!("{data_config_dir}/config_8902.toml").as_str(),
        ])
        .stderr(Stdio::null())
        .stdout(Stdio::null())
        .spawn()
        .expect("failed to execute cnosdb process");
    let data2 = Command::new(data_exec)
        .args([
            "run",
            "--config",
            format!("{data_config_dir}/config_8912.toml").as_str(),
        ])
        .stderr(Stdio::null())
        .stdout(Stdio::null())
        .spawn()
        .expect("failed to execute cnosdb process");

    wait_cnosdb_startup("127.0.0.1:8902");
    wait_cnosdb_startup("127.0.0.1:8912");

    [data1, data2]
}

// start cluster
#[allow(dead_code)]
#[test]
fn start_cluster() {
    let crate_dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let workspace_dir = crate_dir.parent().unwrap();

    start_meta(workspace_dir);
    start_data(workspace_dir);
    // sleep 3 seconds
    thread::sleep(Duration::from_secs(3));
}
