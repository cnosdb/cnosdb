// #![cfg(feature = "cn_e2e_tests")]
#![allow(dead_code)]
use std::process::{Command, Output};
// use std::sync::Arc;
use std::thread;

// pub use arrow_schema::datatype;
use meta::{client, store::command};
use models::schema::{DatabaseSchema, Tenant};
use sysinfo::{ProcessExt, System, SystemExt};

// #[cfg(feature = "cn_e2e_test")]
#[cfg(test)]
mod tests {
    // use std::os::unix::thread;
    use std::process::{Command, Output};

    use meta::client;
    use meta::store::command;
    use models::schema::{DatabaseOptions, DatabaseSchema, Duration, Precision};

    use super::*;

    #[tokio::test]
    async fn test_multi_tenants_write_data() {
        // clean env
        kill_process("cnosdb-meta");
        kill_process("cnosdb");
        // start cluster
        let opt: Output = start_cluster().await;
        assert!(opt.status.code() == Some(0));
        // create  tenants
        prepare().await;
        prepare_data().await;
        for i in 0..5 {
            let tenant = format!("tenant{}", i);
            let db = format!("tenant{}db1", i);
            let output = query(tenant, db).await;
            assert!(output.status.code() == Some(0));
            println!("output: {}", String::from_utf8_lossy(&output.stdout));
            assert!(String::from_utf8_lossy(&output.stdout).contains("100"));
            assert!(String::from_utf8_lossy(&output.stdout).contains("200 OK"));
        }

        // clean env
        kill_process("cnosdb-meta");
        kill_process("cnosdb");
    }

    #[tokio::test]
    async fn test_replica() {
        // clean env
        kill_process("cnosdb-meta");
        kill_process("cnosdb");
        // start cluster
        let opt: Output = start_cluster().await;
        assert!(opt.status.code() == Some(0));
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
        let api = "http://127.0.0.1:8901/debug".to_string();
        let output = Command::new("curl")
            .args([&api])
            .output()
            .expect("failed to execute process");
        assert!(output.status.code() == Some(0));
        print!("output: {}", String::from_utf8_lossy(&output.stdout));
        assert!(String::from_utf8_lossy(&output.stdout).contains("\"replica\":2"));
        // todo: check replica
        // clean env
        kill_process("cnosdb-meta");
        kill_process("cnosdb");
    }

    #[tokio::test]
    async fn test_shard() {
        // clean env
        kill_process("cnosdb-meta");
        kill_process("cnosdb");
        // start cluster
        let opt: Output = start_cluster().await;
        assert!(opt.status.code() == Some(0));
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
        let api = "http://127.0.0.1:8901/debug".to_string();
        let output = Command::new("curl")
            .args([&api])
            .output()
            .expect("failed to execute process");
        assert!(output.status.code() == Some(0));
        print!("output: {}", String::from_utf8_lossy(&output.stdout));
        assert!(String::from_utf8_lossy(&output.stdout).contains("\"shard_num\":2"));

        // clean env
        kill_process("cnosdb-meta");
        kill_process("cnosdb");
    }

    #[tokio::test]
    async fn test_ttl() {
        // clean env
        kill_process("cnosdb-meta");
        kill_process("cnosdb");
        // start cluster
        let opt: Output = start_cluster().await;
        assert!(opt.status.code() == Some(0));
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
        let api = "http://127.0.0.1:8901/debug".to_string();
        let output = Command::new("curl")
            .args([&api])
            .output()
            .expect("failed to execute process");
        assert!(output.status.code() == Some(0));
        print!("output: {}", String::from_utf8_lossy(&output.stdout));
        assert!(String::from_utf8_lossy(&output.stdout).contains("\"time_num\":1"));
        // write data
        let api: String = "http://127.0.0.1:8902/api/v1/write?db=db1".to_string();
        let output = Command::new("curl")
            .args([
                "-i",
                "-u",
                "root:",
                "-XPOST",
                &api,
                "-d",
                "tb1,tag1=tag1,tag2=tag2 field1=1 1683259054000000000",
            ])
            .output()
            .expect("failed to execute process");
        assert!(output.status.code() == Some(0));
        assert!(
            String::from_utf8_lossy(&output.stdout).contains("write expired time data not permit")
        );

        let output = Command::new("curl")
            .args([
                "-i",
                "-u",
                "root:",
                "-XPOST",
                &api,
                "-d",
                "tb1,tag1=tag1,tag2=tag2 field1=2",
            ])
            .output()
            .expect("failed to execute process");
        assert!(output.status.code() == Some(0));
        print!("output: {}", String::from_utf8_lossy(&output.stdout));
        assert!(String::from_utf8_lossy(&output.stdout).contains("200 OK"));
        // clean env
        kill_process("cnosdb-meta");
        kill_process("cnosdb");
    }

    #[tokio::test]
    async fn test_balance() {
        // clean env
        kill_process("cnosdb-meta");
        kill_process("cnosdb");
        // start cluster
        let opt: Output = start_cluster().await;
        assert!(opt.status.code() == Some(0));
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
        for j in 0..10000 {
            let output = Command::new("curl")
                .args([
                    "-i",
                    "-u",
                    "root:",
                    "-XPOST",
                    &api,
                    "-d",
                    &format!("tb1,tag1=tag1,tag2=tag2 field1={}", j),
                ])
                .output()
                .expect("failed to execute process");
            println!("status: {}", output.status);
            println!("stdout: {}", String::from_utf8_lossy(&output.stdout));
            println!("stderr: {}", String::from_utf8_lossy(&output.stderr));
        }
        // todo check balance
        // clean env
        kill_process("cnosdb-meta");
        kill_process("cnosdb");
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
        // db req
        let req = command::WriteCommand::CreateDB(
            "cluster_xxx".to_string(),
            tenant.clone(),
            DatabaseSchema::new(&tenant, &db),
        );
        // create db
        let rsp = cli
            .write::<command::TenaneMetaDataResp>(&req)
            .await
            .unwrap();

        println!("rsp: {:?}", rsp);
    }
}

#[allow(dead_code)]
async fn start_cluster() -> std::process::Output {
    let output: Output = Command::new("bash")
        .args(["-c", "cd ../ && ./run_cluster.sh"])
        .output()
        .expect("failed to execute process");
    output
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

#[allow(dead_code)]
async fn prepare_data() {
    let mut handles: Vec<thread::JoinHandle<()>> = vec![];
    for i in 0..5 {
        let api: String = format!(
            "http://127.0.0.1:8902/api/v1/write?tenant=tenant{}&&db=tenant{}db1",
            i, i
        );
        let handle: thread::JoinHandle<()> = thread::spawn(move || {
            println!("thread {} started", i);
            for j in 0..100 {
                let output = Command::new("curl")
                    .args([
                        "-i",
                        "-u",
                        "root:",
                        "-XPOST",
                        &api,
                        "-d",
                        &format!("tb1,tag1=tag1,tag2=tag2 field1={}", j),
                    ])
                    .output()
                    .expect("failed to execute process");
                // assert!(output.status.code() == Some(200));
                println!("status: {}", output.status);
                println!("stdout: {}", String::from_utf8_lossy(&output.stdout));
                println!("stderr: {}", String::from_utf8_lossy(&output.stderr));
            }
        });
        handles.push(handle);
    }
    for handle in handles {
        handle.join().unwrap();
    }
}

#[allow(dead_code)]
async fn query(tenant: String, db: String) -> std::process::Output {
    let api = format!(
        "http://127.0.0.1:8902/api/v1/sql?tenant={}&db={}",
        tenant, db
    );
    let output = Command::new("curl")
        .args([
            "-i",
            "-u",
            "root:",
            "-XPOST",
            &api,
            "-d",
            "select count(*) from tb1",
        ])
        .output()
        .expect("failed to execute process");
    output
}
