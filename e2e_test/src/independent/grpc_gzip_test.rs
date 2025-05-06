use datafusion::assert_batches_eq;

use crate::utils::global::E2eContext;
use crate::utils::{flight_authed_client, flight_fetch_result_and_print};
use crate::{check_response, cluster_def};

#[test]
fn grpc_gzip_test() {
    println!("Test begin auth_test");

    let mut ctx = E2eContext::new("grpc_gzip_test", "grpc_gzip_test");
    let mut executor = ctx.build_executor(cluster_def::one_meta_three_data());
    let data_node_def = &executor.cluster_definition().data_cluster_def[0];
    let http_host_port = data_node_def.http_host_port;
    let flight_port = data_node_def.flight_service_port.unwrap_or(8904);

    executor.set_update_data_config_fn_vec(vec![
        Some(Box::new(|config| {
            config.service.grpc_enable_gzip = true;
        })),
        Some(Box::new(|config| {
            config.service.grpc_enable_gzip = true;
        })),
        Some(Box::new(|config| {
            config.service.grpc_enable_gzip = true;
        })),
    ]);
    executor.startup();

    let client = executor.case_context().data_client(0);
    check_response!(client.post(
        format!("http://{http_host_port}/api/v1/sql?db=public"),
        "create database db1 with replica 3",
    ));
    check_response!(client.post(
        format!("http://{http_host_port}/api/v1/sql?db=db1"),
        "create table tb1 (v bigint)",
    ));
    check_response!(client
        .post(
            format!("http://{http_host_port}/api/v1/sql?db=db1"),
            "insert tb1 values (1, 1), (2, 2), (3, 3), (4, 4), (5, 5), (6, 6), (7, 7), (8, 8), (9, 9), (10, 10)",
        ));

    ctx.runtime().block_on(async {
        let mut client = flight_authed_client(flight_port).await;
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
