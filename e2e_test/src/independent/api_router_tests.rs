use reqwest::StatusCode;

use crate::case::step::{ControlStep, LineProtocol, RequestStep, Sql, StepPtr, StepResult};
use crate::utils::global::E2eContext;
use crate::{cluster_def, E2eError};

#[test]
fn api_router() {
    let mut ctx = E2eContext::new("api_router_tests", "api_router");
    let mut executor = ctx.build_executor(cluster_def::one_meta_two_data_separated());
    let cluster_definition = executor.cluster_definition();
    let host_port_1 = cluster_definition.data_cluster_def[0].http_host_port;
    let host_port_2 = cluster_definition.data_cluster_def[1].http_host_port;

    let steps: Vec<StepPtr> = vec![
        RequestStep::new_boxed(
            "write data to data-1(tskv node)",
            LineProtocol::build_request_with_str(
                format!("http://{host_port_1}/api/v1/write?db=public"),
                "api_router,ta=a fa=1 1",
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
        ControlStep::new_boxed_sleep("sleep 3s", 3),
        RequestStep::new_boxed(
            "write data to data-2(query node)",
            LineProtocol::build_request_with_str(
                format!("http://{host_port_2}/api/v1/write?db=public"),
                "api_router,ta=a fa=1 1",
                Ok(()),
            ),
            None,
            None,
        ),
        ControlStep::new_boxed_sleep("sleep 3s", 3),
        RequestStep::new_boxed(
            "query data from data-1(tskv node)",
            Sql::build_request_with_str(
                format!("http://{host_port_1}/api/v1/sql?db=public"),
                "select * from api_router",
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
        ControlStep::new_boxed_sleep("sleep 3s", 3),
        RequestStep::new_boxed(
            "query data from data-2(query node)",
            Sql::build_request_with_str(
                format!("http://{host_port_2}/api/v1/sql?db=public"),
                "select * from api_router",
                Ok(vec!["time,ta,fa", "1970-01-01T00:00:00.000000001,a,1.0"]),
                false,
                false,
            ),
            None,
            None,
        ),
    ];
    executor.execute_steps(&steps);
}
