#![cfg(test)]

use std::process::Command;

use reqwest::StatusCode;
use serial_test::serial;

use crate::case::step::{
    Control, ControlStep, RequestStep, ShellStep, SqlDdl, SqlInsert, SqlQuery, Step,
};
use crate::case::{CnosdbAuth, CnosdbRequest, E2eExecutor, StepLegacy};
use crate::{cluster_def, E2eError};

#[test]
#[serial]
fn case1() {
    let url = "http://127.0.0.1:8902/api/v1/sql?tenant=cnosdb&db=public";

    let executor = E2eExecutor::new_singleton("restart_tests", "case_1", cluster_def::one_data(1));
    executor.execute_steps_legacy(&[
        StepLegacy::CnosdbRequest {
            req: CnosdbRequest::Query {
                url,
                sql: "CREATE TABLE air (visibility DOUBLE, temperature DOUBLE, pressure DOUBLE, TAGS(station))",
                resp: Ok(vec![]),
                sorted: false,
            },
            auth: None,
        },
        StepLegacy::CnosdbRequest {
            req: CnosdbRequest::Query { url, sql: "SHOW TABLES", resp: Ok(vec!["table_name", "air"]), sorted: false },
            auth: None,
        },
        StepLegacy::RestartDataNode(0),
        StepLegacy::CnosdbRequest {
            req: CnosdbRequest::Query { url, sql: "SHOW TABLES", resp: Ok(vec!["table_name", "air"]), sorted: false },
            auth: None,
        },
        StepLegacy::CnosdbRequest {
            req: CnosdbRequest::Insert {
                url,
                sql: "INSERT INTO air (time, station, visibility, temperature, pressure) VALUES
                    ('2023-01-01 01:10:00', 'XiaoMaiDao', 79, 80, 63),
                    ('2023-01-01 01:20:00', 'XiaoMaiDao', 80, 60, 63),
                    ('2023-01-01 01:30:00', 'XiaoMaiDao', 81, 70, 61)",
                resp: Ok(()),
            },
            auth: None,
        },
        StepLegacy::CnosdbRequest {
            req: CnosdbRequest::Query {
                url,
                sql: "SELECT * FROM air order by time",
                resp: Ok(vec![
                    "time,station,visibility,temperature,pressure",
                    "2023-01-01T01:10:00.000000000,XiaoMaiDao,79.0,80.0,63.0",
                    "2023-01-01T01:20:00.000000000,XiaoMaiDao,80.0,60.0,63.0",
                    "2023-01-01T01:30:00.000000000,XiaoMaiDao,81.0,70.0,61.0",
                ]),
                sorted: false,
            },
            auth: None,
        },
        StepLegacy::RestartDataNode(0),
        StepLegacy::CnosdbRequest {
            req: CnosdbRequest::Query {
                url,
                sql: "SELECT * FROM air order by time",
                resp: Ok(vec![
                    "time,station,visibility,temperature,pressure",
                    "2023-01-01T01:10:00.000000000,XiaoMaiDao,79.0,80.0,63.0",
                    "2023-01-01T01:20:00.000000000,XiaoMaiDao,80.0,60.0,63.0",
                    "2023-01-01T01:30:00.000000000,XiaoMaiDao,81.0,70.0,61.0",
                ]),
                sorted: false,
            },
            auth: None,
        },
        StepLegacy::CnosdbRequest {
            req: CnosdbRequest::Ddl { url, sql: "DROP TABLE air", resp: Ok(()) },
            auth: None,
        },
        StepLegacy::RestartDataNode(0),
        StepLegacy::CnosdbRequest {
            req: CnosdbRequest::Query { url, sql: "SHOW TABLES", resp: Ok(vec![]), sorted: false },
            auth: None,
        },
    ]);
}

#[test]
#[serial]
#[ignore = "Dropping a field in a database that had not wrote data will get error 'Query: Coordinator: Unreachable'"]
fn case2() {
    let url = "http://127.0.0.1:8902/api/v1/sql?tenant=cnosdb&db=public";

    let executor = E2eExecutor::new_singleton("restart_tests", "case_2", cluster_def::one_data(1));
    executor.execute_steps_legacy(&[
        StepLegacy::CnosdbRequest {
            req: CnosdbRequest::Ddl {
                url,
                sql: "CREATE TABLE air (
                        visibility DOUBLE,
                        temperature DOUBLE,
                        pressure DOUBLE,
                        TAGS(station))",
                resp: Ok(()),
            },
            auth: None,
        },
        StepLegacy::CnosdbRequest {
            req: CnosdbRequest::Ddl {
                url,
                sql: "ALTER TABLE air ADD FIELD humidity DOUBLE",
                resp: Ok(()),
            },
            auth: None,
        },
        StepLegacy::RestartDataNode(0),
        StepLegacy::CnosdbRequest {
            req: CnosdbRequest::Query {
                url,
                sql: "DESC TABLE air",
                resp: Ok(vec![
                    "column_name,data_type,column_type,compression_codec",
                    "humidity,DOUBLE,FIELD,DEFAULT",
                    "pressure,DOUBLE,FIELD,DEFAULT",
                    "station,STRING,TAG,DEFAULT",
                    "temperature,DOUBLE,FIELD,DEFAULT",
                    "time,TIMESTAMP(NANOSECOND),TIME,DEFAULT",
                    "visibility,DOUBLE,FIELD,DEFAULT",
                ]),
                sorted: false,
            },
            auth: None,
        },
        StepLegacy::CnosdbRequest {
            req: CnosdbRequest::Ddl {
                url,
                sql: "ALTER TABLE air ALTER humidity SET CODEC(QUANTILE)",
                resp: Ok(()),
            },
            auth: None,
        },
        StepLegacy::RestartDataNode(0),
        StepLegacy::CnosdbRequest {
            req: CnosdbRequest::Query {
                url,
                sql: "DESC TABLE air",
                resp: Ok(vec![
                    "column_name,data_type,column_type,compression_codec",
                    "humidity,DOUBLE,FIELD,QUANTILE",
                    "pressure,DOUBLE,FIELD,DEFAULT",
                    "station,STRING,TAG,DEFAULT",
                    "temperature,DOUBLE,FIELD,DEFAULT",
                    "time,TIMESTAMP(NANOSECOND),TIME,DEFAULT",
                    "visibility,DOUBLE,FIELD,DEFAULT",
                ]),
                sorted: true,
            },
            auth: None,
        },
        StepLegacy::CnosdbRequest {
            req: CnosdbRequest::Ddl {
                url,
                sql: "ALTER TABLE air DROP humidity",
                resp: Ok(()),
            },
            auth: None,
        },
        StepLegacy::RestartDataNode(0),
        StepLegacy::CnosdbRequest {
            req: CnosdbRequest::Query {
                url,
                sql: "DESC TABLE air",
                resp: Ok(vec![
                    "column_name,data_type,column_type,compression_codec",
                    "pressure,DOUBLE,FIELD,DEFAULT",
                    "station,STRING,TAG,DEFAULT",
                    "temperature,DOUBLE,FIELD,DEFAULT",
                    "time,TIMESTAMP(NANOSECOND),TIME,DEFAULT",
                    "visibility,DOUBLE,FIELD,DEFAULT",
                ]),
                sorted: true,
            },
            auth: None,
        },
        StepLegacy::CnosdbRequest {
            req: CnosdbRequest::Ddl {
                url,
                sql: "ALTER TABLE air ADD TAG height",
                resp: Ok(()),
            },
            auth: None,
        },
        StepLegacy::RestartDataNode(0),
        StepLegacy::CnosdbRequest {
            req: CnosdbRequest::Query {
                url,
                sql: "DESC TABLE air",
                resp: Ok(vec![
                    "column_name,data_type,column_type,compression_codec",
                    "height,STRING,TAG,DEFAULT",
                    "pressure,DOUBLE,FIELD,DEFAULT",
                    "station,STRING,TAG,DEFAULT",
                    "temperature,DOUBLE,FIELD,DEFAULT",
                    "time,TIMESTAMP(NANOSECOND),TIME,DEFAULT",
                    "visibility,DOUBLE,FIELD,DEFAULT",
                ]),
                sorted: true,
            },
            auth: None,
        },
    ]);
}

#[test]
#[serial]
fn case3() {
    let url = "http://127.0.0.1:8902/api/v1/sql?tenant=cnosdb&db=public";

    let executor = E2eExecutor::new_singleton("restart_tests", "case_3", cluster_def::one_data(1));
    executor.execute_steps_legacy(&[
        StepLegacy::CnosdbRequest {
            req: CnosdbRequest::Ddl {
                url,
                sql: "CREATE DATABASE oceanic_station",
                resp: Ok(()),
            },
            auth: None,
        },
        StepLegacy::RestartDataNode(0),
        StepLegacy::CnosdbRequest {
            req: CnosdbRequest::Query {
                url,
                sql: "SHOW DATABASES",
                resp: Ok(vec![
                    "database_name",
                    "oceanic_station",
                    "public",
                    "usage_schema",
                ]),
                sorted: true,
            },
            auth: None,
        },
        StepLegacy::CnosdbRequest {
            req: CnosdbRequest::Ddl {
                url,
                sql: "ALTER DATABASE oceanic_station SET VNODE_DURATION '1000d'",
                resp: Ok(()),
            },
            auth: None,
        },
        StepLegacy::RestartDataNode(0),
        StepLegacy::CnosdbRequest {
            req: CnosdbRequest::Query {
                url,
                sql: "DESC DATABASE oceanic_station",
                resp: Ok(vec![
                    "ttl,shard,vnode_duration,replica,precision",
                    "INF,1,1000 Days,1,NS",
                ]),
                sorted: false,
            },
            auth: None,
        },
        StepLegacy::CnosdbRequest {
            req: CnosdbRequest::Ddl {
                url,
                sql: "DROP DATABASE oceanic_station",
                resp: Ok(()),
            },
            auth: None,
        },
        StepLegacy::RestartDataNode(0),
        StepLegacy::CnosdbRequest {
            req: CnosdbRequest::Query {
                url,
                sql: "SHOW DATABASES",
                resp: Ok(vec!["database_name", "public", "usage_schema"]),
                sorted: true,
            },
            auth: None,
        },
    ]);
}

#[test]
#[serial]
fn case4() {
    let url_cnosdb_public = "http://127.0.0.1:8902/api/v1/sql?tenant=cnosdb&db=public";
    let url_test_ = "http://127.0.0.1:8902/api/v1/sql?tenant=test";

    let executor = E2eExecutor::new_singleton("restart_tests", "case_4", cluster_def::one_data(1));
    executor.execute_steps_legacy(&[
        StepLegacy::CnosdbRequest {
            req: CnosdbRequest::Ddl {
                url: url_cnosdb_public,
                sql: "CREATE USER IF NOT EXISTS tester",
                resp: Ok(()),
            },
            auth: None,
        },
        StepLegacy::RestartDataNode(0),
        StepLegacy::CnosdbRequest {
            req: CnosdbRequest::Query {
                url: url_cnosdb_public,
                sql: "SELECT * FROM cluster_schema.users WHERE user_name = 'tester'",
                resp: Ok(vec![
                    "user_name,is_admin,user_options",
                    r#"tester,false,"{""password"":""*****""}""#,
                ]),
                sorted: false,
            },
            auth: None,
        },
        StepLegacy::CnosdbRequest {
            req: CnosdbRequest::Ddl {
                url: url_cnosdb_public,
                sql: "alter user tester set granted_admin = true",
                resp: Ok(()),
            },
            auth: None,
        },
        StepLegacy::RestartDataNode(0),
        StepLegacy::CnosdbRequest {
            req: CnosdbRequest::Query {
                url: url_cnosdb_public,
                sql: "SELECT * FROM cluster_schema.users WHERE user_name = 'tester'",
                resp: Ok(vec![
                    "user_name,is_admin,user_options",
                    r#"tester,true,"{""password"":""*****"",""granted_admin"":true}""#,
                ]),
                sorted: false,
            },
            auth: None,
        },
        StepLegacy::CnosdbRequest {
            req: CnosdbRequest::Ddl {
                url: url_cnosdb_public,
                sql: "alter user tester set granted_admin = false",
                resp: Ok(()),
            },
            auth: None,
        },
        StepLegacy::RestartDataNode(0),
        StepLegacy::CnosdbRequest {
            req: CnosdbRequest::Query {
                url: url_cnosdb_public,
                sql: "SELECT * FROM cluster_schema.users WHERE user_name = 'tester'",
                resp: Ok(vec![
                    "user_name,is_admin,user_options",
                    r#"tester,false,"{""password"":""*****"",""granted_admin"":false}""#,
                ]),
                sorted: false,
            },
            auth: None,
        },
        StepLegacy::CnosdbRequest {
            req: CnosdbRequest::Ddl {
                url: url_cnosdb_public,
                sql: "ALTER USER tester SET COMMENT = 'bbb'",
                resp: Ok(()),
            },
            auth: None,
        },
        StepLegacy::RestartDataNode(0),
        StepLegacy::CnosdbRequest {
            req: CnosdbRequest::Query {
                url: url_cnosdb_public,
                sql: "SELECT * FROM cluster_schema.users WHERE user_name = 'tester'",
                resp: Ok(vec![
                    "user_name,is_admin,user_options",
                    r#"tester,false,"{""password"":""*****"",""comment"":""bbb"",""granted_admin"":false}""#,
                ]),
                sorted: false,
            },
            auth: None,
        },
        StepLegacy::CnosdbRequest {
            req: CnosdbRequest::Ddl {
                url: url_cnosdb_public,
                sql: "DROP USER tester",
                resp: Ok(()),
            },
            auth: None,
        },
        StepLegacy::RestartDataNode(0),
        StepLegacy::CnosdbRequest {
            req: CnosdbRequest::Query {
                url: url_cnosdb_public,
                sql: "SELECT * FROM cluster_schema.users WHERE user_name = 'tester'",
                resp: Ok(vec![]),
                sorted: false,
            },
            auth: None,
        },
        StepLegacy::CnosdbRequest {
            req: CnosdbRequest::Ddl {
                url: url_cnosdb_public,
                sql: "CREATE TENANT test",
                resp: Ok(()),
            },
            auth: None,
        },
        StepLegacy::RestartDataNode(0),
        StepLegacy::CnosdbRequest {
            req: CnosdbRequest::Query {
                url: url_cnosdb_public,
                sql: "SELECT * FROM cluster_schema.tenants WHERE tenant_name = 'test'",
                resp: Ok(vec![
                    "tenant_name,tenant_options",
                    "test,\"{\"\"comment\"\":null,\"\"limiter_config\"\":null}\"",
                ]),
                sorted: false,
            },
            auth: None,
        },
        StepLegacy::CnosdbRequest {
            req: CnosdbRequest::Ddl {
                url: url_cnosdb_public,
                sql: "ALTER TENANT test SET COMMENT = 'abc'",
                resp: Ok(()),
            },
            auth: None,
        },
        StepLegacy::RestartDataNode(0),
        StepLegacy::CnosdbRequest {
            req: CnosdbRequest::Query {
                url: url_cnosdb_public,
                sql: "SELECT * FROM cluster_schema.tenants WHERE tenant_name = 'test'",
                resp: Ok(vec![
                    "tenant_name,tenant_options",
                    "test,\"{\"\"comment\"\":\"\"abc\"\",\"\"limiter_config\"\":null}\"",
                ]),
                sorted: false,
            },
            auth: None,
        },
        StepLegacy::CnosdbRequest {
            req: CnosdbRequest::Ddl {
                url: url_test_,
                sql: "CREATE DATABASE db1",
                resp: Ok(()),
            },
            auth: None,
        },
        StepLegacy::CnosdbRequest {
            req: CnosdbRequest::Ddl {
                url: url_cnosdb_public,
                sql: "DROP TENANT test",
                resp: Ok(()),
            },
            auth: None,
        },
        StepLegacy::RestartDataNode(0),
        StepLegacy::CnosdbRequest {
            req: CnosdbRequest::Ddl {
                url: url_cnosdb_public,
                sql: "CREATE TENANT test",
                resp: Ok(()),
            },
            auth: None,
        },
        StepLegacy::CnosdbRequest {
            req: CnosdbRequest::Query {
                url: url_test_,
                sql: "SHOW DATABASES",
                resp: Ok(vec![]),
                sorted: false,
            },
            auth: None,
        },
    ]);
}

#[test]
#[serial]
fn case5() {
    let url_cnosdb_public = "http://127.0.0.1:8902/api/v1/sql?tenant=cnosdb&db=public";
    let url_test_ = "http://127.0.0.1:8902/api/v1/sql?tenant=test";

    let executor = E2eExecutor::new_singleton("restart_tests", "case_5", cluster_def::one_data(1));
    executor.execute_steps_legacy(&[
        StepLegacy::CnosdbRequest {
            req: CnosdbRequest::Ddl {
                url: url_cnosdb_public,
                sql: "CREATE USER tester",
                resp: Ok(()),
            },
            auth: None,
        },
        StepLegacy::CnosdbRequest {
            req: CnosdbRequest::Ddl {
                url: url_cnosdb_public,
                sql: "CREATE TENANT test",
                resp: Ok(()),
            },
            auth: None,
        },
        StepLegacy::CnosdbRequest {
            req: CnosdbRequest::Ddl {
                url: url_test_,
                sql: "CREATE ROLE r1 INHERIT member",
                resp: Ok(()),
            },
            auth: None,
        },
        StepLegacy::RestartDataNode(0),
        StepLegacy::CnosdbRequest {
            req: CnosdbRequest::Query {
                url: url_test_,
                sql: "SELECT * FROM information_schema.roles WHERE role_name = 'r1'",
                resp: Ok(vec!["role_name,role_type,inherit_role", "r1,custom,member"]),
                sorted: false,
            },
            auth: None,
        },
        StepLegacy::CnosdbRequest {
            req: CnosdbRequest::Ddl {
                url: url_test_,
                sql: "ALTER TENANT test ADD USER tester AS r1",
                resp: Ok(()),
            },
            auth: None,
        },
        StepLegacy::RestartDataNode(0),
        StepLegacy::CnosdbRequest {
            req: CnosdbRequest::Query {
                url: url_test_,
                sql: "SELECT 1",
                resp: Ok(vec!["Int64(1)", "1"]),
                sorted: false,
            },
            auth: Some(CnosdbAuth {
                username: "tester".to_string(),
                password: Some("".to_string()),
            }),
        },
        StepLegacy::CnosdbRequest {
            req: CnosdbRequest::Ddl {
                url: url_test_,
                sql: "ALTER TENANT test REMOVE USER tester",
                resp: Ok(()),
            },
            auth: None,
        },
        StepLegacy::RestartDataNode(0),
        StepLegacy::CnosdbRequest {
            req: CnosdbRequest::Query {
                url: url_test_,
                sql: "SELECT 1",
                resp: Err(E2eError::Api {
                    status: StatusCode::UNPROCESSABLE_ENTITY,
                    url: None,
                    req: None,
                    resp: Some(r#"{"error_code":"010016","error_message":"Auth error: The member tester of tenant test not found"}"#.to_string()),
                }),
                sorted: false,
            },
            auth: Some(CnosdbAuth {
                username: "tester".to_string(),
                password: Some("".to_string()),
            }),
        },
        StepLegacy::CnosdbRequest {
            req: CnosdbRequest::Ddl {
                url: url_test_,
                sql: "CREATE DATABASE db1",
                resp: Ok(()),
            },
            auth: None,
        },
        StepLegacy::CnosdbRequest {
            req: CnosdbRequest::Ddl {
                url: url_test_,
                sql: "GRANT WRITE ON DATABASE db1 TO ROLE r1",
                resp: Ok(()),
            },
            auth: None,
        },
        StepLegacy::RestartDataNode(0),
        StepLegacy::CnosdbRequest {
            req: CnosdbRequest::Query {
                url: url_test_,
                sql: "SELECT * FROM information_schema.database_privileges WHERE role_name = 'r1'",
                resp: Ok(vec![
                    "tenant_name,database_name,privilege_type,role_name",
                    "test,db1,Write,r1",
                ]),
                sorted: false,
            },
            auth: None,
        },
        StepLegacy::CnosdbRequest {
            req: CnosdbRequest::Ddl {
                url: url_test_,
                sql: "REVOKE WRITE ON DATABASE db1 FROM r1",
                resp: Ok(()),
            },
            auth: None,
        },
        StepLegacy::RestartDataNode(0),
        StepLegacy::CnosdbRequest {
            req: CnosdbRequest::Query {
                url: url_test_,
                sql: "SELECT * FROM information_schema.database_privileges WHERE role_name = 'r1'",
                resp: Ok(vec![]),
                sorted: false,
            },
            auth: None,
        },
        StepLegacy::CnosdbRequest {
            req: CnosdbRequest::Ddl {
                url: url_test_,
                sql: "GRANT ALL ON DATABASE db1 TO ROLE r1",
                resp: Ok(()),
            },
            auth: None,
        },
        StepLegacy::CnosdbRequest {
            req: CnosdbRequest::Ddl {
                url: url_test_,
                sql: "DROP ROLE r1",
                resp: Ok(()),
            },
            auth: None,
        },
        StepLegacy::RestartDataNode(0),
        StepLegacy::CnosdbRequest {
            req: CnosdbRequest::Query {
                url: url_test_,
                sql: "SELECT * FROM information_schema.roles WHERE role_name = 'r1'",
                resp: Ok(vec![]),
                sorted: false,
            },
            auth: None,
        },
        StepLegacy::CnosdbRequest {
            req: CnosdbRequest::Ddl {
                url: url_test_,
                sql: "CREATE ROLE r1 INHERIT member",
                resp: Ok(()),
            },
            auth: None,
        },
        StepLegacy::CnosdbRequest {
            req: CnosdbRequest::Query {
                url: url_test_,
                sql: "SELECT * FROM information_schema.database_privileges WHERE role_name = 'r1'",
                resp: Ok(vec![]),
                sorted: false,
            },
            auth: None,
        },
    ]);
}

#[test]
fn case6() {
    let url_cnosdb_public = "http://127.0.0.1:8902/api/v1/sql?tenant=cnosdb&db=public";

    let executor = E2eExecutor::new_singleton("restart_tests", "case_6", cluster_def::one_data(1));
    let steps: Vec<Box<dyn Step>> = vec![
        Box::new(RequestStep::new(
            1,
            "create table air",
            SqlDdl::with_str(
                url_cnosdb_public,
                "CREATE TABLE air (visibility DOUBLE, temperature DOUBLE, pressure DOUBLE, TAGS(station))",
                Ok(()),
            ),
            None,
            None,
        )),
        Box::new(RequestStep::new(
            1,
            "insert rows to table air",
            SqlInsert::with_str(
                url_cnosdb_public,
                "INSERT INTO air (time, station, visibility, temperature, pressure) VALUES
                ('2023-01-01 01:10:00', 'XiaoMaiDao', 79, 80, 63),
                ('2023-01-01 01:20:00', 'XiaoMaiDao', 80, 60, 63),
                ('2023-01-01 01:30:00', 'XiaoMaiDao', 81, 70, 61)",
                Ok(()),
            ),
            None,
            None,
        )),
        Box::new(RequestStep::new(
            1,
            "select table air",
            SqlQuery::with_str(
                url_cnosdb_public,
                "SELECT time, station, visibility, temperature, pressure FROM air ORDER BY time",
                Ok(vec![
                    "time,station,visibility,temperature,pressure",
                    "2023-01-01T01:10:00.000000000,XiaoMaiDao,79.0,80.0,63.0",
                    "2023-01-01T01:20:00.000000000,XiaoMaiDao,80.0,60.0,63.0",
                    "2023-01-01T01:30:00.000000000,XiaoMaiDao,81.0,70.0,61.0",
                ]),
                false
            ),
            None,
            None,
        )),
        Box::new(ControlStep::new(1, "sleep", Control::Sleep(5))),
        Box::new(ShellStep::with_fn(1, "get vnode id", Box::new(|c| {
            let database_path =c.data_dir(0).join("data").join("cnosdb.public");
            let mut command = Command::new("ls");
            command.arg(database_path);
            command
        }), true, None, Some(Box::new(|c, resp| {
            if resp.is_empty() {
                panic!("Cannot find any vnode in database directory");
            } else {
                c.context_variables_mut().insert("vnode_id".to_string(), resp[0].clone());
            }
        })))),
        Box::new(RequestStep::new(
            1,
            "compact vnode",
            SqlDdl::with_fn(
                Box::new(|_| url_cnosdb_public.to_string()),
                Box::new(|c| format!("COMPACT VNODE {};", c.context_variables().get("vnode_id").expect("Context variable 'vnode_id' has been set"))),
                Ok(()),
            ),
            None,
            None,
        )),
        Box::new(ControlStep::new(1, "wait for a while", Control::Sleep(5))),
        Box::new(ControlStep::new(1, "restart data node", Control::RestartDataNode(0))),
        Box::new(ControlStep::new(1, "wait for a while", Control::Sleep(5))),
        Box::new(RequestStep::new(
            1,
            "select table air",
            SqlQuery::with_str(
                url_cnosdb_public,
                "SELECT time, station, visibility, temperature, pressure FROM air ORDER BY time",
                Ok(vec![
                    "time,station,visibility,temperature,pressure",
                    "2023-01-01T01:10:00.000000000,XiaoMaiDao,79.0,80.0,63.0",
                    "2023-01-01T01:20:00.000000000,XiaoMaiDao,80.0,60.0,63.0",
                    "2023-01-01T01:30:00.000000000,XiaoMaiDao,81.0,70.0,61.0",
                ]),
                false
            ),
            None,
            None,
        )),
    ];

    executor.execute_steps(&steps);
}
