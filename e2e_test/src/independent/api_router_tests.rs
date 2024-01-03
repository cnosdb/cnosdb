#![cfg(test)]

use reqwest::StatusCode;
use serial_test::serial;

use crate::case::{CnosdbRequest, E2eExecutor, Step};
use crate::{cluster_def, E2eError};

#[test]
#[serial]
fn api_router() {
    let executor = E2eExecutor::new_cluster(
        "api_router_tests",
        "api_router",
        cluster_def::one_meta_two_data_separated(),
    );
    executor.execute_steps(&[
        Step::CnosdbRequest {
            req: CnosdbRequest::Write {
                url: "http://127.0.0.1:8902/api/v1/write?db=public",
                req: "api_router,ta=a fa=1 1",
                resp: Err(E2eError::Api {
                    status: StatusCode::NOT_FOUND,
                    url: None,
                    req: None,
                    resp: None,
                }),
            },
            auth: None,
        },
        Step::CnosdbRequest {
            req: CnosdbRequest::Write {
                url: "http://127.0.0.1:8912/api/v1/write?db=public",
                req: "api_router,ta=a fa=1 1",
                resp: Ok(()),
            },
            auth: None,
        },
        Step::CnosdbRequest {
            req: CnosdbRequest::Query {
                url: "http://127.0.0.1:8902/api/v1/sql?db=public",
                sql: "select * from api_router",
                resp: Err(E2eError::Api {
                    status: StatusCode::NOT_FOUND,
                    url: None,
                    req: None,
                    resp: None,
                }),
                sorted: false,
                regex: false,
            },
            auth: None,
        },
        Step::CnosdbRequest {
            req: CnosdbRequest::Query {
                url: "http://127.0.0.1:8912/api/v1/sql?db=public",
                sql: "select* from api_router",
                resp: Ok(vec!["time,ta,fa", "1970-01-01T00:00:00.000000001,a,1.0"]),
                sorted: false,
                regex: false,
            },
            auth: None,
        },
    ]);
}
