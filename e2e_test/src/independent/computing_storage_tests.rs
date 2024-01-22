#![cfg(test)]

use reqwest::StatusCode;
use serial_test::serial;

use crate::case::{CnosdbRequest, E2eExecutor, Step};
use crate::{cluster_def, E2eError};

//auto test about issue 669 799 842
#[test]
#[serial]
fn separated_start_test() {
    let executor = E2eExecutor::new_cluster(
        "computing_stroage_tests",
        "separated_start_test",
        cluster_def::one_meta_two_data_separated(),
    );
    executor.execute_steps(&[
        Step::CnosdbRequest {
            req: CnosdbRequest::Write {
                url: "http://127.0.0.1:8902/api/v1/write?db=public",
                req: "start_test,ta=a fa=1 1",
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
                req: "start_test fa=1 1",
                resp: Ok(()),
            },
            auth: None,
        },
        Step::CnosdbRequest {
            req: CnosdbRequest::Query {
                url: "http://127.0.0.1:8902/api/v1/sql?db=public",
                sql: "select * from start_test",
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
                sql: "select* from start_test",
                resp: Ok(vec!["time,fa", "1970-01-01T00:00:00.000000001,1.0"]),
                sorted: false,
                regex: false,
            },
            auth: None,
        },
    ]);
}

//auto test about issue 923
#[test]
#[serial]
fn meta_primary_crash_test() {
    let executor = E2eExecutor::new_cluster(
        "computing_stroage_tests",
        "meta_primary_crash_test",
        cluster_def::three_meta_two_data_bundled(),
    );

    executor.execute_steps(&[
        Step::Sleep(10),
        Step::StopMetaNode(0),
        Step::Sleep(50),
        Step::CnosdbRequest {
            req: CnosdbRequest::Write {
                url: "http://127.0.0.1:8912/api/v1/write?db=public",
                req: "start_test fa=1 1",
                resp: Ok(()),
            },
            auth: None,
        },
        Step::CnosdbRequest {
            req: CnosdbRequest::Query {
                url: "http://127.0.0.1:8912/api/v1/sql?db=public",
                sql: "select* from start_test",
                resp: Ok(vec!["time,fa", "1970-01-01T00:00:00.000000001,1.0"]),
                sorted: false,
                regex: false,
            },
            auth: None,
        },
    ]);
}
