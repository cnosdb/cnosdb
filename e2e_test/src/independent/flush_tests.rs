use std::fs::File;
use std::io::{BufReader, Read};

use serial_test::serial;

use crate::utils::get_workspace_dir;
use crate::utils::global::E2eContext;
use crate::{check_response, cluster_def};

#[test]
#[serial]
// fix for 1814
fn divisor_test() {
    println!("Test begin auth_test");

    let mut ctx = E2eContext::new("flush_tests", "divisor_test");
    let mut executor = ctx.build_executor(cluster_def::one_data(1));
    let host_port = executor.cluster_definition().data_cluster_def[0].http_host_port;

    executor.startup();

    let client = executor.case_context().data_client(0);

    // change cache size max_buffer_size=2M
    {
        check_response!(client.post(
            format!("http://{host_port}/api/v1/sql?db=test"),
            "create database test with max_memcache_size '2MiB';",
        ));
    }

    let data_dir = get_workspace_dir().join("e2e_test").join("test_data");
    {
        let mut buf = String::new();
        let mut file = BufReader::new(File::open(data_dir.join("log_0.txt")).unwrap());
        file.read_to_string(&mut buf).unwrap();

        check_response!(client.post(format!("http://{host_port}/api/v1/write?db=test"), &buf));
    }
    {
        let mut buf = String::new();
        let mut file = BufReader::new(File::open(data_dir.join("log_1.txt")).unwrap());
        file.read_to_string(&mut buf).unwrap();

        check_response!(client.post(format!("http://{host_port}/api/v1/write?db=test"), &buf));
    }

    let resp = check_response!(client.post(
        format!("http://{host_port}/api/v1/sql?db=test"),
        "select exact_count(*) from log;",
    ));
    assert_eq!(resp.text().unwrap(), "COUNT(UInt8(1))\n17280\n")
}
