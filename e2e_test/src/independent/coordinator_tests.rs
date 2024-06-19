#![allow(dead_code)]
#![cfg(test)]

use std::collections::HashMap;
use std::ops::Sub;
use std::path::PathBuf;
use std::process::{Command, Output};
use std::sync::atomic::{self, AtomicBool};
use std::sync::Arc;
use std::time::Duration;
use std::{io, thread};

use meta::store::command::WriteCommand;
use models::meta_data::TenantMetaData;
use models::oid::UuidGenerator;
use models::schema::database_schema::{DatabaseOptions, DatabaseSchema, Precision};
use models::schema::tenant::{Tenant, TenantOptions};
use models::schema::utils::Duration as CnosDuration;
use serial_test::serial;
use sysinfo::System;
use walkdir::WalkDir;

use crate::utils::{run_cluster, CnosdbDataTestHelper, CnosdbMetaTestHelper};
use crate::{cluster_def, E2eError, E2eResult};

const DEFAULT_CLUSTER: &str = "cluster_xxx";
const DEFAULT_TABLE: &str = "test_table";

fn tenant_name(code: i32) -> String {
    format!("tenant_{code}")
}

fn database_name(code: i32) -> String {
    format!("tenant_{code}_database")
}

impl CnosdbMetaTestHelper {
    fn prepare_test_data(&self) {
        for i in 0..5 {
            // Create tenant tenant_{i}
            let name = tenant_name(i);
            let oid = UuidGenerator::default().next_id();
            let tenant = Tenant::new(oid, name.clone(), TenantOptions::default());
            let create_tenant_req = WriteCommand::CreateTenant(DEFAULT_CLUSTER.to_string(), tenant);
            println!("Creating tenant: {:?}", &create_tenant_req);
            let create_tenant_res = self
                .runtime
                .block_on(self.meta_client.write::<()>(&create_tenant_req));
            create_tenant_res.unwrap();

            thread::sleep(Duration::from_secs(3));

            // Create database tenant_{i}_database
            let database = database_name(i);
            let create_database_req = WriteCommand::CreateDB(
                DEFAULT_CLUSTER.to_string(),
                name.clone(),
                DatabaseSchema::new(&name, &database),
            );
            println!("Creating database: {:?}", &create_database_req);
            let create_database_res = self.runtime.block_on(
                self.meta_client
                    .write::<TenantMetaData>(&create_database_req),
            );
            create_database_res.unwrap();
        }
    }
}

impl CnosdbDataTestHelper {
    /// Generate write line protocol `{DEFAULT_TABLE},tag_a=a1,tag_b=b1 value={}` and write to cnosdb.
    fn prepare_test_data(&self) -> E2eResult<()> {
        let mut handles: Vec<thread::JoinHandle<()>> = vec![];
        let has_failed = Arc::new(AtomicBool::new(false));
        for i in 0..5 {
            let tenant = tenant_name(i);
            let database = database_name(i);
            let url = format!("http://127.0.0.1:8902/api/v1/write?tenant={tenant}&db={database}");
            // let curl_write = format!(
            //     "curl -u root: -XPOST -w %{{http_code}} -s -o /dev/null http://127.0.0.1:8902/api/v1/write?tenant={tenant}&db={database}",
            // );
            let has_failed = has_failed.clone();
            let client = self.client.clone();
            let handle: thread::JoinHandle<()> = thread::spawn(move || {
                println!("Write data thread-{i} started");
                for j in 0..100 {
                    if has_failed.load(atomic::Ordering::SeqCst) {
                        break;
                    }
                    let body = format!("{DEFAULT_TABLE},tag_a=a1,tag_b=b1 value={}", j);
                    let mut write_fail_count = 0;
                    // Try write and retry at most 3 times if failed..
                    while write_fail_count < 3 {
                        let resp = match client.post(&url, &body) {
                            Ok(r) => r,
                            Err(e) => {
                                write_fail_count += 1;
                                eprintln!("Failed to write: {}", e);
                                continue;
                            }
                        };
                        if resp.status().as_u16() == 200 {
                            break;
                        } else {
                            let message = resp
                                .text()
                                .unwrap_or_else(|e| format!("Receive non-UTF-8 character: {e}"));
                            eprintln!("Received unexpected ouput: {message}",);
                            write_fail_count += 1;
                        }
                    }
                    if write_fail_count >= 3 {
                        eprintln!("Failed to write '{}' after retried 3 times", &body);
                        has_failed.store(true, atomic::Ordering::SeqCst);
                        break;
                    }
                }
                println!("Write data thread-{i} finished");
            });
            handles.push(handle);
        }
        for handle in handles {
            handle.join().unwrap();
        }
        if has_failed.load(atomic::Ordering::SeqCst) {
            Err(E2eError::DataWrite(String::new()))
        } else {
            Ok(())
        }
    }
}

/// Clean test environment.
///
/// 1. Kill all 'cnosdb' and 'cnosdb-meta' process,
/// 2. Remove directory '/tmp/cnosdb'.
fn clean_env() {
    println!("Cleaning environment...");
    kill_process("cnosdb");
    kill_process("cnosdb-meta");
    println!(" - Removing directory '/tmp/cnosdb'");
    let _ = std::fs::remove_dir_all("/tmp/cnosdb");
    println!("Clean environment completed.");
}

/// Kill all processes with specified process name.
fn kill_process(process_name: &str) {
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

mod self_tests {
    use super::*;
    use crate::cluster_def;
    use crate::utils::run_cluster;

    #[test]
    #[ignore = "run this test when developing"]
    fn test_exec_curl() {
        let output = exec_curl("curl www.baidu.com", "").unwrap();
        // let output = query("tenant1", "tenant1db1");
        println!(
            "status: {} \nstdout: {} \nstderr: {}",
            output.status,
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr),
        );
    }

    #[test]
    #[ignore = "run this test when developing"]
    fn test_initialization() {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .worker_threads(4)
            .build()
            .unwrap();
        let runtime = Arc::new(runtime);

        clean_env();
        {
            let (meta, data) = run_cluster(
                "",
                runtime,
                &cluster_def::three_meta_two_data_bundled(),
                true,
                true,
            );
            let meta = meta.unwrap();
            let data = data.unwrap();
            meta.prepare_test_data();
            data.prepare_test_data().unwrap();

            let tenant = tenant_name(1);
            let database = database_name(1);

            let res = data
                .client
                .post(
                    format!("http://127.0.0.1:8902/api/v1/write?tenant={tenant}&db={database}"),
                    "tb1,ta=a1,tb=b1 fa=1",
                )
                .unwrap();
            println!("{}", res.text().unwrap());
        }
        clean_env();
    }
}

#[test]
#[serial]
fn test_multi_tenants_write_data() {
    println!("Test begin 'test_multi_tenants_write_data'");
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(4)
        .build()
        .unwrap();
    let runtime = Arc::new(runtime);

    clean_env();
    {
        let (meta, data) = run_cluster(
            "",
            runtime,
            &cluster_def::three_meta_two_data_bundled(),
            true,
            true,
        );
        let meta = meta.unwrap();
        let data = data.unwrap();
        meta.prepare_test_data();
        data.prepare_test_data().unwrap();

        for i in 0..5 {
            let tenant = tenant_name(i);
            let db = database_name(i);
            let result_csv = match data.client.post(
                format!("http://127.0.0.1:8902/api/v1/sql?tenant={tenant}&db={db}"),
                format!("select count(*) from {DEFAULT_TABLE}").as_str(),
            ) {
                Ok(r) => r.text().unwrap(),
                Err(e) => {
                    panic!("Failed to do query: {e}");
                }
            };
            println!("- Result text: {}", &result_csv);
            let line_10 = result_csv.lines().nth(1);
            assert_eq!(line_10, Some("100"));
        }
    }
    clean_env();
    println!("Test complete 'test_multi_tenants_write_data'");
}

#[test]
#[serial]
fn test_replica() {
    println!("Test begin 'test_replica'");
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(4)
        .build()
        .unwrap();
    let runtime = Arc::new(runtime);

    clean_env();
    {
        let (meta, _data) = run_cluster(
            "",
            runtime.clone(),
            &cluster_def::three_meta_two_data_bundled(),
            true,
            true,
        );
        let meta = meta.unwrap();

        let tenant_name = "cnosdb";
        let database_name = "db_test_replica";
        let duration = CnosDuration::new_with_day(100);
        let vnode_duration = CnosDuration::new_with_day(50);
        let shard_num = 8;
        let replica = 2;
        let precision = Precision::NS;

        // Create database.
        let db_options = DatabaseOptions::new(
            Some(duration),
            Some(shard_num),
            Some(vnode_duration),
            Some(replica),
            Some(precision),
        );
        let db_schema = DatabaseSchema::new_with_options(tenant_name, database_name, db_options);
        let create_db_req = WriteCommand::CreateDB(
            DEFAULT_CLUSTER.to_string(),
            tenant_name.to_string(),
            db_schema,
        );
        let create_db_res =
            runtime.block_on(meta.meta_client.write::<TenantMetaData>(&create_db_req));
        create_db_res.unwrap();

        // Get meta data from debug API.
        let meta_data = meta.query();
        let meta_key = format!("* /{DEFAULT_CLUSTER}/tenants/{tenant_name}/dbs/{database_name}: ");
        let mut meta_value = &meta_data[..0];
        let expected_meta = format!("\"replica\":{replica}");
        let mut ok = false;
        for l in meta_data.lines() {
            if l.starts_with(&meta_key) {
                meta_value = &l[meta_key.len()..];
                ok = meta_value.contains(&expected_meta);
                break;
            }
        }
        assert!(
            ok,
            "{meta_key} {meta_value} does not contains {expected_meta}"
        );
    }
    clean_env();
    println!("Test complete 'test_replica'");
}

#[test]
#[serial]
fn test_shard() {
    println!("Test begin 'test_shard'");
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(4)
        .build()
        .unwrap();
    let runtime = Arc::new(runtime);

    clean_env();
    {
        let (meta, _data) = run_cluster(
            "",
            runtime.clone(),
            &cluster_def::three_meta_two_data_bundled(),
            true,
            true,
        );
        let meta = meta.unwrap();

        let tenant_name = "cnosdb";
        let database_name = "db_test_shard";
        let duration = CnosDuration::new_with_day(100);
        let vnode_duration = CnosDuration::new_with_day(50);
        let shard_num = 8;
        let replica = 1;
        let precision = Precision::NS;

        // Create database.
        let db_options = DatabaseOptions::new(
            Some(duration),
            Some(shard_num),
            Some(vnode_duration),
            Some(replica),
            Some(precision),
        );
        let db_schema = DatabaseSchema::new_with_options(tenant_name, database_name, db_options);
        let create_db_req = WriteCommand::CreateDB(
            DEFAULT_CLUSTER.to_string(),
            tenant_name.to_string(),
            db_schema,
        );
        let create_db_res =
            runtime.block_on(meta.meta_client.write::<TenantMetaData>(&create_db_req));
        create_db_res.unwrap();

        // Get meta data from debug API.
        let meta_data = meta.query();
        let meta_key = format!("* /{DEFAULT_CLUSTER}/tenants/{tenant_name}/dbs/{database_name}: ");
        let mut meta_value = &meta_data[..0];
        let expected_meta = format!("\"shard_num\":{shard_num}");
        let mut ok = false;
        for l in meta_data.lines() {
            if l.starts_with(&meta_key) {
                meta_value = &l[meta_key.len()..];
                ok = meta_value.contains(&expected_meta);
                break;
            }
        }
        assert!(
            ok,
            "{meta_key} {meta_value} does not contains {expected_meta}"
        );
    }
    clean_env();
    println!("Test complete 'test_shard'");
}

#[test]
#[serial]
fn test_ttl() {
    println!("Test begin 'test_ttl'");
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(4)
        .build()
        .unwrap();
    let runtime = Arc::new(runtime);

    clean_env();
    {
        let (meta, data) = run_cluster(
            "",
            runtime.clone(),
            &cluster_def::three_meta_two_data_bundled(),
            true,
            true,
        );
        let meta = meta.unwrap();
        let data = data.unwrap();

        let tenant_name = "cnosdb";
        let database_name = "db_test_ttl";
        let duration = CnosDuration::new_with_day(100);
        let vnode_duration = CnosDuration::new_with_day(50);
        let shard_num = 1;
        let replica = 1;
        let precision = Precision::NS;

        let chrono_now = chrono::Utc::now();
        let chrono_duration = chrono::Duration::days(100);

        // Create database.
        let db_options = DatabaseOptions::new(
            Some(duration.clone()),
            Some(shard_num),
            Some(vnode_duration),
            Some(replica),
            Some(precision),
        );
        let db_schema = DatabaseSchema::new_with_options(tenant_name, database_name, db_options);
        let create_db_req = WriteCommand::CreateDB(
            DEFAULT_CLUSTER.to_string(),
            tenant_name.to_string(),
            db_schema,
        );
        let create_db_res =
            runtime.block_on(meta.meta_client.write::<TenantMetaData>(&create_db_req));
        create_db_res.unwrap();

        // Get meta data from debug API.
        let meta_data = meta.query();
        let meta_key = format!("* /{DEFAULT_CLUSTER}/tenants/{tenant_name}/dbs/{database_name}: ");
        let mut meta_value = &meta_data[..0];
        let expected_meta = [
            format!("\"time_num\":{}", duration.time_num),
            String::from("\"unit\":\"Day\""),
        ];
        let mut ok_num = 0_usize;
        for l in meta_data.lines() {
            if l.starts_with(&meta_key) {
                meta_value = &l[meta_key.len()..];
                for m in expected_meta.iter() {
                    if meta_value.contains(m) {
                        ok_num += 1;
                    }
                }
                break;
            }
        }
        assert!(
            ok_num == expected_meta.len(),
            "{meta_key} {meta_value} does not contains {:?}",
            expected_meta
        );

        let url =
            format!("http://127.0.0.1:8902/api/v1/write?&tenant={tenant_name}&db={database_name}");

        // Insert the valid data.
        let now = chrono_now.timestamp_nanos_opt().unwrap();
        let resp = data
            .client
            .post(&url, format!("tab_1,ta=a1 fa=1 {now}").as_str())
            .unwrap();
        assert!(resp.status().is_success());

        // Insert the exored-time data.
        let past = chrono_now
            .sub(chrono_duration)
            .timestamp_nanos_opt()
            .unwrap();
        let resp = data
            .client
            .post(&url, format!("tab_1,ta=a2 fa=2 {past}").as_str())
            .unwrap();
        assert_eq!(resp.status().as_u16(), 422);
        assert!(resp
            .text()
            .unwrap()
            .contains("write expired time data not permit"));
    }
    clean_env();
    println!("Test complete 'test_ttl'");
}

#[test]
#[serial]
fn test_balance() {
    println!("Test begin 'test_balance'");
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(4)
        .build()
        .unwrap();
    let runtime = Arc::new(runtime);

    clean_env();
    {
        let (meta, data) = run_cluster(
            "",
            runtime.clone(),
            &cluster_def::three_meta_two_data_bundled(),
            true,
            true,
        );
        let meta = meta.unwrap();
        let data = data.unwrap();

        let tenant_name = "cnosdb";
        let database_name = "db_test_balance";
        let duration = CnosDuration::new_with_day(100);
        let vnode_duration = CnosDuration::new_with_day(50);
        let shard_num = 2;
        let replica = 2;
        let precision = Precision::NS;

        // Create database.
        let db_options = DatabaseOptions::new(
            Some(duration),
            Some(shard_num),
            Some(vnode_duration),
            Some(replica),
            Some(precision),
        );
        let db_schema = DatabaseSchema::new_with_options(tenant_name, database_name, db_options);
        let create_db_req = WriteCommand::CreateDB(
            DEFAULT_CLUSTER.to_string(),
            tenant_name.to_string(),
            db_schema,
        );
        let create_db_res =
            runtime.block_on(meta.meta_client.write::<TenantMetaData>(&create_db_req));
        create_db_res.unwrap();

        // Write some data.
        let url =
            format!("http://127.0.0.1:8902/api/v1/write?&tenant={tenant_name}&db={database_name}");
        println!("- Writing data.");
        for j in 0..10 {
            let body = format!("tab_1,ta=a1,tb=b1 value={}", j);
            data.client.post(&url, &body).unwrap();
        }
        println!("- Write data completed.");

        // Check balance
        println!("Getting meta...");
        thread::sleep(Duration::from_secs(3));
        let mut shard_vnode_node_ids: Vec<Vec<(u32, u64)>> = Vec::new();
        let meta_data = meta.query();
        let meta_key =
            format!("* /{DEFAULT_CLUSTER}/tenants/{tenant_name}/dbs/{database_name}/buckets/");
        let mut ok = false;
        for l in meta_data.lines() {
            if l.starts_with(&meta_key) {
                ok = true;
                let mut i = 0;
                // Shards loop
                'shard: loop {
                    let mut vnode_node_ids: Vec<(u32, u64)> = Vec::new();
                    // "vnodes":[{"id":6,"node_id":1001,"status":"Running"},{...},...]
                    if let Some(vnodes_i) = l[i..].find("\"vnodes\"") {
                        i += vnodes_i;
                        let mut found_vnodes = 0;
                        // Replications loop
                        loop {
                            // Get id (vnode id).
                            let vnode_id = if let Some(vnode_id_i) = l[i..].find("\"id\"") {
                                i += vnode_id_i + 5; // + len("id":)
                                let vnode_id = if let Some(vnode_id_end_i) =
                                    l[i..].find(|c| c == ',' || c == '}')
                                {
                                    let vnode_id = l[i..i + vnode_id_end_i].parse::<u32>().unwrap();
                                    i += vnode_id_end_i;
                                    vnode_id
                                } else {
                                    break 'shard;
                                };
                                found_vnodes += 1;
                                vnode_id
                            } else {
                                break 'shard;
                            };
                            // Get node_id
                            if let Some(node_id_i) = l[i..].find("\"node_id\"") {
                                i += node_id_i + 10; // + len("node_id":)
                                if let Some(node_id_end_i) = l[i..].find(|c| c == ',' || c == '}') {
                                    let node_id = l[i..i + node_id_end_i].parse::<u64>().unwrap();
                                    vnode_node_ids.push((vnode_id, node_id));
                                }
                            } else {
                                break 'shard;
                            }

                            if found_vnodes == replica {
                                // Found {replica} vnodes, break to find next shard
                                shard_vnode_node_ids.push(vnode_node_ids);
                                break;
                            }
                        }
                    } else {
                        break;
                    }
                }
            }
        }
        println!("- Shard - vnode - node IDs: {:?}", shard_vnode_node_ids);
        assert!(ok, "{meta_data} does not contains {meta_key}");

        println!("Checking balance...");
        let url =
            format!("http://127.0.0.1:8902/api/v1/sql?&tenant={tenant_name}&db={database_name}");
        // Shards loop
        for vnode_node_ids in shard_vnode_node_ids.iter() {
            // Replications loop
            let mut vnode_sizes: HashMap<u32, u64> = HashMap::new();
            for (vnode_id, node_id) in vnode_node_ids.iter() {
                println!("- Flush & compaction vnode {vnode_id}");
                let resp = data
                    .client
                    .post(&url, format!("compact vnode {vnode_id};").as_str())
                    .unwrap();
                assert!(
                    resp.status().is_success(),
                    "compact vnode {vnode_id} failed {}",
                    resp.text().unwrap()
                );
                let dir = PathBuf::from("/tmp/cnosdb")
                    .join(node_id.to_string())
                    .join("db")
                    .join("data")
                    .join(format!("{tenant_name}.{database_name}"))
                    .join(vnode_id.to_string());
                if std::fs::metadata(&dir).is_err() {
                    // If dir not exists, or could not read, ignore this shard.
                    break;
                }
                // Check balance by file size.
                let vnode_size = WalkDir::new(dir)
                    .min_depth(1)
                    .max_depth(3)
                    .into_iter()
                    .filter_map(|entry| entry.ok())
                    .filter_map(|entry| entry.metadata().ok())
                    .filter(|metadata| metadata.is_file())
                    .fold(0, |acc, m| acc + m.len());
                vnode_sizes.insert(*vnode_id, vnode_size);
                // TODO: check balance by data count.
            }

            println!("- Vnode sizes: {:?}", vnode_sizes);
            // TODO: check if balanced by comparing vnode_sizes.
        }
    }
    clean_env();
    println!("Test complete 'test_balance'");
}

/// Execute curl command
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
