#![cfg(test)]

use std::sync::Arc;

use datafusion::{arrow, assert_batches_eq};
use serial_test::serial;

use crate::utils::{flight_authed_client, flight_fetch_result_and_print, kill_all, run_cluster};
use crate::{check_response, cluster_def};

#[test]
#[serial]
fn grpc_gzip_test() {
    println!("Test begin auth_test");

    let test_dir = "/tmp/e2e_test/grpc_gzip_test";
    let _ = std::fs::remove_dir_all(test_dir);
    std::fs::create_dir_all(test_dir).unwrap();

    kill_all();

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(4)
        .build()
        .unwrap();
    let runtime = Arc::new(runtime);

    let mut data_node_def = cluster_def::one_meta_three_data();

    for node in data_node_def.data_cluster_def.iter_mut() {
        node.grpc_enable_gzip = true;
    }

    let (_meta_server, data_server) =
        run_cluster(test_dir, runtime.clone(), &data_node_def, true, true);

    let server = data_server.unwrap();

    check_response!(server.client.post(
        "http://127.0.0.1:8902/api/v1/sql?db=public",
        "create database db1 with replica 3",
    ));

    check_response!(server.client.post(
        "http://127.0.0.1:8902/api/v1/sql?db=db1",
        "create table tb1 (v bigint)",
    ));

    check_response!(server
        .client
        .post(
            "http://127.0.0.1:8902/api/v1/sql?db=db1",
            "insert tb1 values (1, 1), (2, 2), (3, 3), (4, 4), (5, 5), (6, 6), (7, 7), (8, 8), (9, 9), (10, 10)",
        ));

    runtime.block_on(async {
        let mut client = flight_authed_client().await;
        client.set_header("db", "db1");
        let flight_info = client
            .execute("select * from tb1;".to_string(), None)
            .await
            .unwrap();

        let actual = flight_fetch_result_and_print(flight_info, &mut client).await;

        let expected = [
            "+-------------------------------+----+",
            "| time                          | v  |",
            "+-------------------------------+----+",
            "| 1970-01-01T00:00:00.000000001 | 1  |",
            "| 1970-01-01T00:00:00.000000002 | 2  |",
            "| 1970-01-01T00:00:00.000000003 | 3  |",
            "| 1970-01-01T00:00:00.000000004 | 4  |",
            "| 1970-01-01T00:00:00.000000005 | 5  |",
            "| 1970-01-01T00:00:00.000000006 | 6  |",
            "| 1970-01-01T00:00:00.000000007 | 7  |",
            "| 1970-01-01T00:00:00.000000008 | 8  |",
            "| 1970-01-01T00:00:00.000000009 | 9  |",
            "| 1970-01-01T00:00:00.000000010 | 10 |",
            "+-------------------------------+----+",
        ];

        assert_batches_eq!(expected, &actual);

        client.close().await.unwrap();
    });
}
