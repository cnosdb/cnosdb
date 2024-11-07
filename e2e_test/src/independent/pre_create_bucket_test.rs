#![allow(dead_code)]
#![cfg(test)]
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use crate::check_response;
use crate::cluster_def::{three_meta_two_data_separated, CnosdbClusterDefinition};
use crate::utils::{
    kill_all, run_cluster_with_customized_configs, CnosdbDataTestHelper, CnosdbMetaTestHelper,
};

fn start_cluster(
    test_dir: &PathBuf,
    runtime: Arc<tokio::runtime::Runtime>,
) -> (Option<CnosdbMetaTestHelper>, Option<CnosdbDataTestHelper>) {
    run_cluster_with_customized_configs(
        test_dir,
        runtime,
        &CnosdbClusterDefinition::with_ids(&[1], &[1, 2]),
        true,
        true,
        vec![],
        vec![
            Some(Box::new(|c| {
                c.global.pre_create_bucket = true;
            })),
            Some(Box::new(|c| {
                c.global.pre_create_bucket = true;
            })),
        ],
    )
}

fn start_single_cluster(
    test_dir: &PathBuf,
    runtime: Arc<tokio::runtime::Runtime>,
) -> (Option<CnosdbMetaTestHelper>, Option<CnosdbDataTestHelper>) {
    run_cluster_with_customized_configs(
        test_dir,
        runtime,
        &CnosdbClusterDefinition::with_ids(&[], &[1]),
        true,
        true,
        vec![],
        vec![Some(Box::new(|c| {
            c.global.pre_create_bucket = true;
        }))],
    )
}

fn start_separated_cluster(
    test_dir: &PathBuf,
    runtime: Arc<tokio::runtime::Runtime>,
) -> (Option<CnosdbMetaTestHelper>, Option<CnosdbDataTestHelper>) {
    run_cluster_with_customized_configs(
        test_dir,
        runtime,
        &three_meta_two_data_separated(),
        true,
        true,
        vec![],
        vec![
            Some(Box::new(|c| {
                c.global.pre_create_bucket = true;
            })),
            Some(Box::new(|c| {
                c.global.pre_create_bucket = true;
            })),
        ],
    )
}

#[test]
#[ignore]
fn pre_create_bucket_test() {
    println!("Test begin precreate bucket test");

    let test_dir = PathBuf::from("/tmp/e2e_test/independent/precreate_bucket_test");
    let _ = std::fs::remove_dir_all(&test_dir);
    std::fs::create_dir_all(&test_dir).unwrap();

    kill_all();

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(4)
        .build()
        .unwrap();
    let runtime = Arc::new(runtime);
    let (_meta, data) = start_cluster(&test_dir, runtime.clone());
    let data = data.unwrap();
    let client = data.client.clone();
    check_response!(client.post(
        "http:////127.0.0.1:8902/api/v1/sql?db=public",
        "create database current_time with vnode_duration '1m' replica 2",
    ));
    check_response!(client.post(
        "http:////127.0.0.1:8902/api/v1/sql?db=public",
        "create database past_time with vnode_duration '1m' replica 2",
    ));
    check_response!(client.post(
        "http:////127.0.0.1:8902/api/v1/sql?db=public",
        "create database future_time with vnode_duration '1m' replica 2",
    ));
    check_response!(client.post(
        "http:////127.0.0.1:8902/api/v1/sql?db=current_time",
        "CREATE TABLE air (visibility DOUBLE,temperature DOUBLE,pressure DOUBLE,TAGS(station));"
    ));
    check_response!(client.post(
        "http:////127.0.0.1:8902/api/v1/sql?db=future_time",
        "CREATE TABLE air (visibility DOUBLE,temperature DOUBLE,pressure DOUBLE,TAGS(station));"
    ));
    check_response!(client.post(
        "http:////127.0.0.1:8902/api/v1/sql?db=past_time",
        "CREATE TABLE air (visibility DOUBLE,temperature DOUBLE,pressure DOUBLE,TAGS(station));"
    ));
    check_response!(client.post(
        "http:////127.0.0.1:8902/api/v1/sql?db=current_time",
        "INSERT INTO air (TIME, station, visibility, temperature, pressure) VALUES (now(), 'XiaoMaiDao1', 56, 69, 411);"
    ));
    check_response!(client.post(
        "http:////127.0.0.1:8902/api/v1/sql?db=past_time",
        "INSERT INTO air (TIME, station, visibility, temperature, pressure) VALUES (now() - interval '30 day', 'XiaoMaiDao1', 56, 69, 411);"
    ));
    check_response!(client.post(
        "http:////127.0.0.1:8902/api/v1/sql?db=future_time",
        "INSERT INTO air (TIME, station, visibility, temperature, pressure) VALUES (now() + interval '30 day', 'XiaoMaiDao1', 56, 69, 411);"
    ));
    let mut resp = client
        .post(
            "http:////127.0.0.1:8902/api/v1/sql?db=current_time",
            "show replicas;",
        )
        .unwrap();
    let mut count = resp.text().unwrap().matches("current_time").count();
    assert_eq!(count, 1);
    resp = client
        .post(
            "http:////127.0.0.1:8902/api/v1/sql?db=past_time",
            "show replicas;",
        )
        .unwrap();
    count = resp.text().unwrap().matches("past_time").count();
    assert_eq!(count, 1);
    resp = client
        .post(
            "http:////127.0.0.1:8902/api/v1/sql?db=future_time",
            "show replicas;",
        )
        .unwrap();
    count = resp.text().unwrap().matches("future_time").count();
    assert_eq!(count, 1);
    std::thread::sleep(Duration::from_secs(60));
    resp = client
        .post(
            "http:////127.0.0.1:8902/api/v1/sql?db=current_time",
            "show replicas;",
        )
        .unwrap();
    count = resp.text().unwrap().matches("current_time").count();
    assert_eq!(count, 1);
    resp = client
        .post(
            "http:////127.0.0.1:8902/api/v1/sql?db=past_time",
            "show replicas;",
        )
        .unwrap();
    count = resp.text().unwrap().matches("past_time").count();
    assert_eq!(count, 1);
    resp = client
        .post(
            "http:////127.0.0.1:8902/api/v1/sql?db=future_time",
            "show replicas;",
        )
        .unwrap();
    count = resp.text().unwrap().matches("future_time").count();
    assert_eq!(count, 1);

    kill_all();
    let _ = std::fs::remove_dir_all(&test_dir);
    println!("Test end precreate bucket test");
}

#[test]
#[ignore]
fn pre_create_bucket_test_single() {
    println!("Test begin precreate bucket test single");

    let test_dir = PathBuf::from("/tmp/e2e_test/independent/precreate_bucket_test_single");
    let _ = std::fs::remove_dir_all(&test_dir);
    std::fs::create_dir_all(&test_dir).unwrap();

    kill_all();

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(4)
        .build()
        .unwrap();
    let runtime = Arc::new(runtime);
    let (_meta, data) = start_single_cluster(&test_dir, runtime.clone());
    let data = data.unwrap();
    let client = data.client.clone();
    check_response!(client.post(
        "http:////127.0.0.1:8902/api/v1/sql?db=public",
        "create database current_time with vnode_duration '1m'",
    ));
    check_response!(client.post(
        "http:////127.0.0.1:8902/api/v1/sql?db=public",
        "create database past_time with vnode_duration '1m'",
    ));
    check_response!(client.post(
        "http:////127.0.0.1:8902/api/v1/sql?db=public",
        "create database future_time with vnode_duration '1m'",
    ));
    check_response!(client.post(
        "http:////127.0.0.1:8902/api/v1/sql?db=current_time",
        "CREATE TABLE air (visibility DOUBLE,temperature DOUBLE,pressure DOUBLE,TAGS(station));"
    ));
    check_response!(client.post(
        "http:////127.0.0.1:8902/api/v1/sql?db=future_time",
        "CREATE TABLE air (visibility DOUBLE,temperature DOUBLE,pressure DOUBLE,TAGS(station));"
    ));
    check_response!(client.post(
        "http:////127.0.0.1:8902/api/v1/sql?db=past_time",
        "CREATE TABLE air (visibility DOUBLE,temperature DOUBLE,pressure DOUBLE,TAGS(station));"
    ));
    check_response!(client.post(
        "http:////127.0.0.1:8902/api/v1/sql?db=current_time",
        "INSERT INTO air (TIME, station, visibility, temperature, pressure) VALUES (now(), 'XiaoMaiDao1', 56, 69, 411);"
    ));
    check_response!(client.post(
        "http:////127.0.0.1:8902/api/v1/sql?db=past_time",
        "INSERT INTO air (TIME, station, visibility, temperature, pressure) VALUES (now() - interval '30 day', 'XiaoMaiDao1', 56, 69, 411);"
    ));
    check_response!(client.post(
        "http:////127.0.0.1:8902/api/v1/sql?db=future_time",
        "INSERT INTO air (TIME, station, visibility, temperature, pressure) VALUES (now() + interval '30 day', 'XiaoMaiDao1', 56, 69, 411);"
    ));
    let mut resp = client
        .post(
            "http:////127.0.0.1:8902/api/v1/sql?db=current_time",
            "show replicas;",
        )
        .unwrap();
    let mut count = resp.text().unwrap().matches("current_time").count();
    assert_eq!(count, 1);
    resp = client
        .post(
            "http:////127.0.0.1:8902/api/v1/sql?db=past_time",
            "show replicas;",
        )
        .unwrap();
    count = resp.text().unwrap().matches("past_time").count();
    assert_eq!(count, 1);
    resp = client
        .post(
            "http:////127.0.0.1:8902/api/v1/sql?db=future_time",
            "show replicas;",
        )
        .unwrap();
    count = resp.text().unwrap().matches("future_time").count();
    assert_eq!(count, 1);
    std::thread::sleep(Duration::from_secs(60));
    resp = client
        .post(
            "http:////127.0.0.1:8902/api/v1/sql?db=current_time",
            "show replicas;",
        )
        .unwrap();
    count = resp.text().unwrap().matches("current_time").count();
    assert_eq!(count, 1);
    resp = client
        .post(
            "http:////127.0.0.1:8902/api/v1/sql?db=past_time",
            "show replicas;",
        )
        .unwrap();
    count = resp.text().unwrap().matches("past_time").count();
    assert_eq!(count, 1);
    resp = client
        .post(
            "http:////127.0.0.1:8902/api/v1/sql?db=future_time",
            "show replicas;",
        )
        .unwrap();
    count = resp.text().unwrap().matches("future_time").count();
    assert_eq!(count, 1);

    kill_all();
    let _ = std::fs::remove_dir_all(&test_dir);
    println!("Test end precreate bucket test single");
}

#[test]
#[ignore]
fn pre_create_bucket_test_separated() {
    println!("Test begin precreate bucket test separated");

    let test_dir = PathBuf::from("/tmp/e2e_test/independent/precreate_bucket_test_separated");
    let _ = std::fs::remove_dir_all(&test_dir);
    std::fs::create_dir_all(&test_dir).unwrap();

    kill_all();

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(4)
        .build()
        .unwrap();
    let runtime = Arc::new(runtime);
    let (_meta, data) = start_separated_cluster(&test_dir, runtime.clone());
    let data = data.unwrap();
    let client = data.client.clone();
    check_response!(client.post(
        "http:////127.0.0.1:8912/api/v1/sql?db=public",
        "create database current_time with vnode_duration '1m'",
    ));
    check_response!(client.post(
        "http:////127.0.0.1:8912/api/v1/sql?db=public",
        "create database past_time with vnode_duration '1m'",
    ));
    check_response!(client.post(
        "http:////127.0.0.1:8912/api/v1/sql?db=public",
        "create database future_time with vnode_duration '1m'",
    ));
    check_response!(client.post(
        "http:////127.0.0.1:8912/api/v1/sql?db=current_time",
        "CREATE TABLE air (visibility DOUBLE,temperature DOUBLE,pressure DOUBLE,TAGS(station));"
    ));
    check_response!(client.post(
        "http:////127.0.0.1:8912/api/v1/sql?db=future_time",
        "CREATE TABLE air (visibility DOUBLE,temperature DOUBLE,pressure DOUBLE,TAGS(station));"
    ));
    check_response!(client.post(
        "http:////127.0.0.1:8912/api/v1/sql?db=past_time",
        "CREATE TABLE air (visibility DOUBLE,temperature DOUBLE,pressure DOUBLE,TAGS(station));"
    ));
    check_response!(client.post(
        "http:////127.0.0.1:8912/api/v1/sql?db=current_time",
        "INSERT INTO air (TIME, station, visibility, temperature, pressure) VALUES (now(), 'XiaoMaiDao1', 56, 69, 411);"
    ));
    check_response!(client.post(
        "http:////127.0.0.1:8912/api/v1/sql?db=past_time",
        "INSERT INTO air (TIME, station, visibility, temperature, pressure) VALUES (now() - interval '30 day', 'XiaoMaiDao1', 56, 69, 411);"
    ));
    check_response!(client.post(
        "http:////127.0.0.1:8912/api/v1/sql?db=future_time",
        "INSERT INTO air (TIME, station, visibility, temperature, pressure) VALUES (now() + interval '30 day', 'XiaoMaiDao1', 56, 69, 411);"
    ));
    let mut resp = client
        .post(
            "http:////127.0.0.1:8912/api/v1/sql?db=current_time",
            "show replicas;",
        )
        .unwrap();
    let mut count = resp.text().unwrap().matches("current_time").count();
    assert_eq!(count, 1);
    resp = client
        .post(
            "http:////127.0.0.1:8912/api/v1/sql?db=past_time",
            "show replicas;",
        )
        .unwrap();
    count = resp.text().unwrap().matches("past_time").count();
    assert_eq!(count, 1);
    resp = client
        .post(
            "http:////127.0.0.1:8912/api/v1/sql?db=future_time",
            "show replicas;",
        )
        .unwrap();
    count = resp.text().unwrap().matches("future_time").count();
    assert_eq!(count, 1);
    std::thread::sleep(Duration::from_secs(60));
    resp = client
        .post(
            "http:////127.0.0.1:8912/api/v1/sql?db=current_time",
            "show replicas;",
        )
        .unwrap();
    count = resp.text().unwrap().matches("current_time").count();
    assert_eq!(count, 1);
    resp = client
        .post(
            "http:////127.0.0.1:8912/api/v1/sql?db=past_time",
            "show replicas;",
        )
        .unwrap();
    count = resp.text().unwrap().matches("past_time").count();
    assert_eq!(count, 1);
    resp = client
        .post(
            "http:////127.0.0.1:8912/api/v1/sql?db=future_time",
            "show replicas;",
        )
        .unwrap();
    count = resp.text().unwrap().matches("future_time").count();
    assert_eq!(count, 1);

    kill_all();
    let _ = std::fs::remove_dir_all(&test_dir);
    println!("Test end precreate bucket test separated");
}
