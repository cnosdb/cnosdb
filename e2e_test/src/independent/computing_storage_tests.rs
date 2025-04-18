use reqwest::StatusCode;

use crate::case::step::{ControlStep, LineProtocol, RequestStep, Sql, StepPtr, StepResult};
use crate::utils::global::E2eContext;
use crate::{cluster_def, E2eError};

//auto test about issue 669 799 842
#[test]
fn separated_start_test() {
    let mut ctx = E2eContext::new("computing_storage_tests", "separated_start_test");
    let mut executor = ctx.build_executor(cluster_def::one_meta_two_data_separated());
    let http_addr_1 = executor.cluster_definition().data_cluster_def[0].http_host_port;
    let http_addr_2 = executor.cluster_definition().data_cluster_def[1].http_host_port;

    let steps: Vec<StepPtr> = vec![
        RequestStep::new_boxed(
            "write data to node 1 (invalid)",
            LineProtocol::build_request_with_str(
                format!("http://{http_addr_1}/api/v1/write?db=public"),
                "start_test,ta=a fa=1 1",
                Err(E2eError::Api {
                    status: StatusCode::NOT_FOUND,
                    url: None,
                    req: None,
                    resp: None,
                }),
            ),
            None,
            None,
        ),
        RequestStep::new_boxed(
            "write data to node 2",
            LineProtocol::build_request_with_str(
                format!("http://{http_addr_2}/api/v1/write?db=public"),
                "start_test fa=1 1",
                Ok(()),
            ),
            None,
            None,
        ),
        RequestStep::new_boxed(
            "select data from node 1 (invalid)",
            Sql::build_request_with_str(
                format!("http://{http_addr_1}/api/v1/sql?db=public"),
                "select * from start_test",
                StepResult::Err(E2eError::Api {
                    status: StatusCode::NOT_FOUND,
                    url: None,
                    req: None,
                    resp: None,
                }),
                false,
                false,
            ),
            None,
            None,
        ),
        RequestStep::new_boxed(
            "select data from node 2",
            Sql::build_request_with_str(
                format!("http://{http_addr_2}/api/v1/sql?db=public"),
                "select* from start_test",
                Ok(vec!["time,fa", "1970-01-01T00:00:00.000000001,1.0"]),
                false,
                false,
            ),
            None,
            None,
        ),
    ];
    executor.execute_steps(&steps);
}

//auto test about issue 923
#[test]
fn meta_primary_crash_test() {
    let mut ctx = E2eContext::new("computing_storage_tests", "meta_primary_crash_test");
    let mut executor = ctx.build_executor(cluster_def::three_meta_two_data_bundled());
    let http_addr_2 = executor.cluster_definition().data_cluster_def[1].http_host_port;

    let steps: Vec<StepPtr> = vec![
        ControlStep::new_boxed_sleep("sleep 10s", 10),
        ControlStep::new_boxed_stop_meta_node("stop meta node", 0),
        ControlStep::new_boxed_sleep("sleep 50s", 50),
        RequestStep::new_boxed(
            "write data to node 2",
            LineProtocol::build_request_with_str(
                format!("http://{http_addr_2}/api/v1/write?db=public"),
                "start_test fa=1 1",
                Ok(()),
            ),
            None,
            None,
        ),
        ControlStep::new_boxed_sleep("sleep 1s", 1),
        RequestStep::new_boxed(
            "select data from node 2",
            Sql::build_request_with_str(
                format!("http://{http_addr_2}/api/v1/sql?db=public"),
                "select* from start_test",
                Ok(vec!["time,fa", "1970-01-01T00:00:00.000000001,1.0"]),
                false,
                false,
            ),
            None,
            None,
        ),
    ];
    executor.execute_steps(&steps);
}
