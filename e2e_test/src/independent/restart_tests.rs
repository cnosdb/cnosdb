use std::fmt::Write;
use std::time::Duration;

use rand::distributions::Alphanumeric;
use rand::Rng;
use reqwest::StatusCode;

use crate::case::step::{
    ControlStep, FunctionStep, RequestStep, Sql, SqlNoResult, StepPtr, StepResult,
};
use crate::case::{CaseFlowControl, CnosdbAuth};
use crate::cluster_def::CnosdbClusterDefinition;
use crate::utils::global::E2eContext;
use crate::{check_response, cluster_def, E2eError};

#[test]
fn case1() {
    let mut ctx = E2eContext::new("restart_tests", "case_1");
    let mut executor = ctx.build_executor(cluster_def::one_data(1));

    let url = &format!(
        "http://{}/api/v1/sql?tenant=cnosdb&db=public",
        executor.cluster_definition().data_cluster_def[0].http_host_port
    );

    let steps: Vec<StepPtr> = vec![
        RequestStep::new_boxed("create table", SqlNoResult::build_request_with_str(
            url,
            "CREATE TABLE air (visibility DOUBLE, temperature DOUBLE, pressure DOUBLE, TAGS(station))",
            Ok(()),
        ), None, None),
        RequestStep::new_boxed("show tables after create table", Sql::build_request_with_str(
            url,
            "SHOW TABLES",
            Ok(vec!["table_name", "air"]),
            false, false,
        ), None, None),
        ControlStep::new_boxed_restart_data_node("restart", 0),
        RequestStep::new_boxed("show tables after restart", Sql::build_request_with_str(
            url,
            "SHOW TABLES",
            Ok(vec!["table_name", "air"]),
            false, false,
        ), None, None),
        RequestStep::new_boxed("insert data", SqlNoResult::build_request_with_str(
            url,
            "INSERT INTO air (time, station, visibility, temperature, pressure) VALUES
            ('2023-01-01 01:10:00', 'XiaoMaiDao', 79, 80, 63),
            ('2023-01-01 01:20:00', 'XiaoMaiDao', 80, 60, 63),
            ('2023-01-01 01:30:00', 'XiaoMaiDao', 81, 70, 61)",
            Ok(()),
        ), None, None),
        RequestStep::new_boxed("compact manually", SqlNoResult::build_request_with_str(
            url,
            "compact database public",
            Ok(()),
        ), None, None),
        RequestStep::new_boxed("select data", Sql::build_request_with_str(
            url,
            "SELECT * FROM air order by time",
            Ok(vec![
                "time,station,visibility,temperature,pressure",
                "2023-01-01T01:10:00.000000000,XiaoMaiDao,79.0,80.0,63.0",
                "2023-01-01T01:20:00.000000000,XiaoMaiDao,80.0,60.0,63.0",
                "2023-01-01T01:30:00.000000000,XiaoMaiDao,81.0,70.0,61.0",
            ]), false, false,
        ), None, None),
        ControlStep::new_boxed_restart_data_node("restart", 0),
        RequestStep::new_boxed("replace old data with new data", SqlNoResult::build_request_with_str(
            url,
            "INSERT INTO air (time, station, visibility, temperature, pressure) VALUES
            ('2023-01-01 01:10:00', 'XiaoMaiDao', 89, 90, 73),
            ('2023-01-01 01:20:00', 'XiaoMaiDao', 90, 70, 73),
            ('2023-01-01 01:30:00', 'XiaoMaiDao', 91, 80, 71)",
            Ok(()),
        ), None, None),
        RequestStep::new_boxed("compact manually", SqlNoResult::build_request_with_str(
            url,
            "compact database public",
            Ok(()),
        ), None, None),
        RequestStep::new_boxed("select new data", Sql::build_request_with_str(
            url,
            "SELECT * FROM air order by time",
            Ok(vec![
                "time,station,visibility,temperature,pressure",
                "2023-01-01T01:10:00.000000000,XiaoMaiDao,89.0,90.0,73.0",
                "2023-01-01T01:20:00.000000000,XiaoMaiDao,90.0,70.0,73.0",
                "2023-01-01T01:30:00.000000000,XiaoMaiDao,91.0,80.0,71.0",
            ]), false, false,
        ), None, None),
        RequestStep::new_boxed("drop table", SqlNoResult::build_request_with_str(
            url,
            "DROP TABLE air",
            Ok(()),
        ), None, None),
        ControlStep::new_boxed_restart_data_node("name", 0),

        RequestStep::new_boxed("select data after drop table", Sql::build_request_with_str(
            url,
            "SELECT * FROM air",
            StepResult::Err(E2eError::Api {
                status: StatusCode::UNPROCESSABLE_ENTITY,
                url: None,
                req: None,
                resp: Some(r#"{"error_code":"030019","error_message":"Table not found: \"cnosdb.public.air\""}"#.to_string()),
            }), false, false,
        ), None, None),

        RequestStep::new_boxed("show tables after drop table", Sql::build_request_with_str(
            url,
            "SHOW TABLES",
            Ok(vec!["table_name",]),
            false, false,
        ), None, None),
    ];
    executor.execute_steps(&steps);
}

#[test]
fn case2() {
    let mut ctx = E2eContext::new("restart_tests", "case_2");
    let mut executor = ctx.build_executor(cluster_def::one_data(1));

    let url = &format!(
        "http://{}/api/v1/sql?tenant=cnosdb&db=public",
        executor.cluster_definition().data_cluster_def[0].http_host_port
    );

    let steps: Vec<StepPtr> = vec![
        RequestStep::new_boxed("create table", SqlNoResult::build_request_with_str(
                url,
                "CREATE TABLE air (visibility DOUBLE, temperature DOUBLE, pressure DOUBLE, TAGS(station))",
                Ok(()),
            ), None, None,
        ),
        RequestStep::new_boxed("alter table", SqlNoResult::build_request_with_str(
                url,
                "ALTER TABLE air ADD FIELD humidity DOUBLE",
                Ok(()),
            ), None, None,
        ),
        ControlStep::new_boxed_restart_data_node("restart", 0),
        RequestStep::new_boxed("describe table after restart", Sql::build_request_with_str(
                url,
                "DESC TABLE air",
                Ok(vec![
                    "column_name,data_type,column_type,compression_codec",
                    "humidity,DOUBLE,FIELD,DEFAULT",
                    "pressure,DOUBLE,FIELD,DEFAULT",
                    "station,STRING,TAG,DEFAULT",
                    "temperature,DOUBLE,FIELD,DEFAULT",
                    "time,TIMESTAMP(NANOSECOND),TIME,DEFAULT",
                    "visibility,DOUBLE,FIELD,DEFAULT",
                ]),
                true, false,
            ), None, None
        ),
        RequestStep::new_boxed("alter table set column codec", SqlNoResult::build_request_with_str(
                url,
                "ALTER TABLE air ALTER humidity SET CODEC(QUANTILE)",
                Ok(()),
            ), None, None
        ),
        ControlStep::new_boxed_restart_data_node("restart", 0),
        RequestStep::new_boxed("describe table after restart", Sql::build_request_with_str(
                url,
                "DESC TABLE air",
                Ok(vec![
                    "column_name,data_type,column_type,compression_codec",
                    "humidity,DOUBLE,FIELD,QUANTILE",
                    "pressure,DOUBLE,FIELD,DEFAULT",
                    "station,STRING,TAG,DEFAULT",
                    "temperature,DOUBLE,FIELD,DEFAULT",
                    "time,TIMESTAMP(NANOSECOND),TIME,DEFAULT",
                    "visibility,DOUBLE,FIELD,DEFAULT",
                ]), true, false,
            ), None, None,
        ),
        RequestStep::new_boxed("alter table drop column", SqlNoResult::build_request_with_str(
                url,
                "ALTER TABLE air DROP humidity",
                Ok(()),
            ), None, None,
        ),
        ControlStep::new_boxed_restart_data_node("restart", 0),
        RequestStep::new_boxed("descrbie table after restart", Sql::build_request_with_str(
                url,
                "DESC TABLE air",
                Ok(vec![
                    "column_name,data_type,column_type,compression_codec",
                    "pressure,DOUBLE,FIELD,DEFAULT",
                    "station,STRING,TAG,DEFAULT",
                    "temperature,DOUBLE,FIELD,DEFAULT",
                    "time,TIMESTAMP(NANOSECOND),TIME,DEFAULT",
                    "visibility,DOUBLE,FIELD,DEFAULT",
                ]), true, false,
            ), None, None
        ),
        RequestStep::new_boxed("alter table add tag", SqlNoResult::build_request_with_str(
                url,
                "ALTER TABLE air ADD TAG height",
                Ok(()),
            ), None, None
        ),
        ControlStep::new_boxed_restart_data_node("restart", 0),
        RequestStep::new_boxed("describe table after restart", Sql::build_request_with_str(
                url,
                "DESC TABLE air",
                Ok(vec![
                    "column_name,data_type,column_type,compression_codec",
                    "height,STRING,TAG,DEFAULT",
                    "pressure,DOUBLE,FIELD,DEFAULT",
                    "station,STRING,TAG,DEFAULT",
                    "temperature,DOUBLE,FIELD,DEFAULT",
                    "time,TIMESTAMP(NANOSECOND),TIME,DEFAULT",
                    "visibility,DOUBLE,FIELD,DEFAULT",
                ]), true, false,
            ), None, None
        ),
    ];
    executor.execute_steps(&steps);
}

#[test]
fn case3() {
    let mut ctx = E2eContext::new("restart_tests", "case_3");
    let mut executor = ctx.build_executor(cluster_def::one_data(1));
    executor.add_update_data_config_fn(Box::new(|c| {
        c.cache.partition = 4;
    }));

    let url = &format!(
        "http://{}/api/v1/sql?tenant=cnosdb&db=public",
        executor.cluster_definition().data_cluster_def[0].http_host_port
    );

    let steps: Vec<StepPtr> = vec![
        RequestStep::new_boxed("create database", SqlNoResult::build_request_with_str(
                url,
                "CREATE DATABASE oceanic_station",
                Ok(()),
            ), None, None,
        ),
        ControlStep::new_boxed_restart_data_node("restart", 0),
        RequestStep::new_boxed("show databases after restart", Sql::build_request_with_str(
                url,
                "SHOW DATABASES",
                Ok(vec![
                    "cluster_schema",
                    "database_name",
                    "oceanic_station",
                    "public",
                    "usage_schema",
                ]), true, false,
            ), None, None,
        ),
        RequestStep::new_boxed("alter database", SqlNoResult::build_request_with_str(
                url,
                "ALTER DATABASE oceanic_station SET VNODE_DURATION '1000d'",
                Ok(()),
            ), None, None,
        ),
        ControlStep::new_boxed_restart_data_node("restart", 0),
        RequestStep::new_boxed("describe database after restart", Sql::build_request_with_str(
                url,
                "DESC DATABASE oceanic_station",
                Ok(vec![
                    "ttl,shard,vnode_duration,replica,precision,max_memcache_size,memcache_partitions,wal_max_file_size,wal_sync,strict_write,max_cache_readers",
                    "INF,1,2years 8months 25days 23h 31m 12s,1,NS,128 MiB,4,128 MiB,false,false,32"
                ]), false, false,
            ), None, None,
        ),
        RequestStep::new_boxed("drop database", SqlNoResult::build_request_with_str(
                url,
                "DROP DATABASE oceanic_station",
                Ok(()),
            ), None, None,
        ),
        ControlStep::new_boxed_restart_data_node("restart", 0),
        RequestStep::new_boxed("show databases after restart", Sql::build_request_with_str(
                url,
                "SHOW DATABASES",
                Ok(vec![
                    "cluster_schema",
                    "database_name",
                    "public",
                    "usage_schema",
                ]), true, false,
            ), None, None,
        ),
    ];
    executor.execute_steps(&steps);
}

#[test]
fn case4() {
    let mut ctx = E2eContext::new("restart_tests", "case_4");
    let mut executor = ctx.build_executor(cluster_def::one_data(1));

    let http_service_addr = executor.cluster_definition().data_cluster_def[0].http_host_port;
    let url_cnosdb_public =
        &format!("http://{http_service_addr}/api/v1/sql?tenant=cnosdb&db=public");
    let url_test = &format!("http://{http_service_addr}/api/v1/sql?tenant=test");

    let steps: Vec<StepPtr> = vec![
        RequestStep::new_boxed(
            "create user",
            SqlNoResult::build_request_with_str(
                url_cnosdb_public,
                "CREATE USER IF NOT EXISTS tester",
                Ok(()),
            ),
            None,
            None,
        ),
        ControlStep::new_boxed_restart_data_node("restart", 0),
        RequestStep::new_boxed(
            "select users",
            Sql::build_request_with_str(
                url_cnosdb_public,
                "SELECT * FROM cluster_schema.users WHERE user_name = 'tester'",
                Ok(vec![
                    "user_name,is_admin,user_options",
                    r#"tester,false,"{""hash_password"":""*****""}""#,
                ]),
                false,
                false,
            ),
            None,
            None,
        ),
        RequestStep::new_boxed(
            "alter user set admin",
            SqlNoResult::build_request_with_str(
                url_cnosdb_public,
                "alter user tester set granted_admin = true",
                Ok(()),
            ),
            None,
            None,
        ),
        ControlStep::new_boxed_restart_data_node("restart", 0),
        RequestStep::new_boxed(
            "select user after restart",
            Sql::build_request_with_str(
                url_cnosdb_public,
                "SELECT * FROM cluster_schema.users WHERE user_name = 'tester'",
                Ok(vec![
                    "user_name,is_admin,user_options",
                    r#"tester,true,"{""hash_password"":""*****"",""granted_admin"":true}""#,
                ]),
                false,
                false,
            ),
            None,
            None,
        ),
        RequestStep::new_boxed(
            "alter user set not admin",
            SqlNoResult::build_request_with_str(
                url_cnosdb_public,
                "alter user tester set granted_admin = false",
                Ok(()),
            ),
            None,
            None,
        ),
        ControlStep::new_boxed_restart_data_node("restart", 0),
        RequestStep::new_boxed(
            "select user after restart",
            Sql::build_request_with_str(
                url_cnosdb_public,
                "SELECT * FROM cluster_schema.users WHERE user_name = 'tester'",
                Ok(vec![
                    "user_name,is_admin,user_options",
                    r#"tester,false,"{""hash_password"":""*****"",""granted_admin"":false}""#,
                ]),
                false,
                false,
            ),
            None,
            None,
        ),
        RequestStep::new_boxed(
            "alter user set comment",
            SqlNoResult::build_request_with_str(
                url_cnosdb_public,
                "ALTER USER tester SET COMMENT = 'bbb'",
                Ok(()),
            ),
            None,
            None,
        ),
        ControlStep::new_boxed_restart_data_node("restart", 0),
        RequestStep::new_boxed(
            "select user after restart",
            Sql::build_request_with_str(
                url_cnosdb_public,
                "SELECT * FROM cluster_schema.users WHERE user_name = 'tester'",
                Ok(vec![
                    "user_name,is_admin,user_options",
                    r#"tester,false,"{""hash_password"":""*****"",""comment"":""bbb"",""granted_admin"":false}""#,
                ]),
                false,
                false,
            ),
            None,
            None,
        ),
        RequestStep::new_boxed(
            "drop user",
            SqlNoResult::build_request_with_str(url_cnosdb_public, "DROP USER tester", Ok(())),
            None,
            None,
        ),
        ControlStep::new_boxed_restart_data_node("restart", 0),
        RequestStep::new_boxed(
            "select user after restart",
            Sql::build_request_with_str(
                url_cnosdb_public,
                "SELECT * FROM cluster_schema.users WHERE user_name = 'tester'",
                Ok(vec!["user_name,is_admin,user_options"]),
                false,
                false,
            ),
            None,
            None,
        ),
        RequestStep::new_boxed(
            "create tenant",
            SqlNoResult::build_request_with_str(url_cnosdb_public, "CREATE TENANT test", Ok(())),
            None,
            None,
        ),
        ControlStep::new_boxed_restart_data_node("restart", 0),
        RequestStep::new_boxed(
            "select tenant after restart",
            Sql::build_request_with_str(
                url_cnosdb_public,
                "SELECT * FROM cluster_schema.tenants WHERE tenant_name = 'test'",
                Ok(vec![
                    "tenant_name,tenant_options",
                    r#"test,"{""comment"":null,""limiter_config"":null,""drop_after"":null,""tenant_is_hidden"":false}""#,
                ]),
                false,
                false,
            ),
            None,
            None,
        ),
        RequestStep::new_boxed(
            "alter tenant set comment",
            SqlNoResult::build_request_with_str(
                url_cnosdb_public,
                "ALTER TENANT test SET COMMENT = 'abc'",
                Ok(()),
            ),
            None,
            None,
        ),
        ControlStep::new_boxed_restart_data_node("restart", 0),
        RequestStep::new_boxed(
            "select tenant after restart",
            Sql::build_request_with_str(
                url_cnosdb_public,
                "SELECT * FROM cluster_schema.tenants WHERE tenant_name = 'test'",
                Ok(vec![
                    "tenant_name,tenant_options",
                    r#"test,"{""comment"":""abc"",""limiter_config"":null,""drop_after"":null,""tenant_is_hidden"":false}""#,
                ]),
                false,
                false,
            ),
            None,
            None,
        ),
        RequestStep::new_boxed(
            "create database",
            SqlNoResult::build_request_with_str(url_test, "CREATE DATABASE db1", Ok(())),
            None,
            None,
        ),
        RequestStep::new_boxed(
            "drop tenant",
            SqlNoResult::build_request_with_str(url_cnosdb_public, "DROP TENANT test", Ok(())),
            None,
            None,
        ),
        ControlStep::new_boxed_restart_data_node("restart", 0),
        RequestStep::new_boxed(
            "create tenant",
            SqlNoResult::build_request_with_str(url_cnosdb_public, "CREATE TENANT test", Ok(())),
            None,
            None,
        ),
        RequestStep::new_boxed(
            "show databases",
            Sql::build_request_with_str(
                url_test,
                "SHOW DATABASES",
                Ok(vec!["database_name"]),
                false,
                false,
            ),
            None,
            None,
        ),
    ];
    executor.execute_steps(&steps);
}

#[test]
fn case5() {
    let mut ctx = E2eContext::new("restart_tests", "case_5");
    let mut executor = ctx.build_executor(cluster_def::one_data(1));

    let http_service_addr = executor.cluster_definition().data_cluster_def[0].http_host_port;
    let url_cnosdb_public =
        &format!("http://{http_service_addr}/api/v1/sql?tenant=cnosdb&db=public");
    let url_test = &format!("http://{http_service_addr}/api/v1/sql?tenant=test");

    let steps: Vec<StepPtr> = vec![
        RequestStep::new_boxed("create user", SqlNoResult::build_request_with_str(
                url_cnosdb_public,
                "CREATE USER tester",
                Ok(()),
            ), None, None
        ),
        RequestStep::new_boxed("create tenant", SqlNoResult::build_request_with_str(
                url_cnosdb_public,
                "CREATE TENANT test",
                Ok(()),
            ), None, None
        ),
        RequestStep::new_boxed("create role", SqlNoResult::build_request_with_str(
                url_test,
                "CREATE ROLE r1 INHERIT member",
                Ok(()),
            ), None, None
        ),
        ControlStep::new_boxed_restart_data_node("restart", 0),
        RequestStep::new_boxed("select role after restart", Sql::build_request_with_str(
                url_test,
                "SELECT * FROM information_schema.roles WHERE role_name = 'r1'",
                Ok(vec!["role_name,role_type,inherit_role", "r1,custom,member"]),
                false, false
            ), None, None
        ),
        RequestStep::new_boxed("alter tenant add user", SqlNoResult::build_request_with_str(
                url_test,
                "ALTER TENANT test ADD USER tester AS r1",
                Ok(()),
            ), None, None
        ),
        ControlStep::new_boxed_restart_data_node("restart", 0),
        RequestStep::new_boxed("select 1", Sql::build_request_with_str(
                url_test,
                "SELECT 1",
                Ok(vec!["Int64(1)", "1"]),
                false, false,
            ),
            Some(CnosdbAuth {
                username: "tester".to_string(),
                password: Some("".to_string()),
            }),
            None
        ),
        RequestStep::new_boxed("alter tenant remove user", SqlNoResult::build_request_with_str(
                url_test,
                "ALTER TENANT test REMOVE USER tester",
                Ok(()),
            ), None, None
        ),
        ControlStep::new_boxed_restart_data_node("restart", 0),
        RequestStep::new_boxed("select 1", Sql::build_request_with_str(
                url_test,
                "SELECT 1",
                StepResult::Err(E2eError::Api {
                    status: StatusCode::UNPROCESSABLE_ENTITY,
                    url: None,
                    req: None,
                    resp: Some(r#"{"error_code":"010016","error_message":"Auth error: The member tester of tenant test not found"}"#.to_string()),
                }),
                false, false,
            ),
            Some(CnosdbAuth {
                username: "tester".to_string(),
                password: Some("".to_string()),
            }),
            None,
        ),
        RequestStep::new_boxed("create database", SqlNoResult::build_request_with_str(
                url_test,
                "CREATE DATABASE db1",
                Ok(()),
            ), None, None
        ),
        RequestStep::new_boxed("grant role", SqlNoResult::build_request_with_str(
                url_test,
                "GRANT WRITE ON DATABASE db1 TO ROLE r1",
                Ok(()),
            ), None, None
        ),
        ControlStep::new_boxed_restart_data_node("restart", 0),
        RequestStep::new_boxed("select database_privilege", Sql::build_request_with_str(
                url_test,
                "SELECT * FROM information_schema.database_privileges WHERE role_name = 'r1'",
                Ok(vec![
                    "tenant_name,database_name,privilege_type,role_name",
                    "test,db1,Write,r1",
                ]),
                false, false,
            ), None, None,
        ),
        RequestStep::new_boxed("revoke write role", SqlNoResult::build_request_with_str(
                url_test,
                "REVOKE WRITE ON DATABASE db1 FROM r1",
                Ok(()),
            ), None, None
        ),
        ControlStep::new_boxed_restart_data_node("restart", 0),
        RequestStep::new_boxed("select database_privilege", Sql::build_request_with_str(
                url_test,
                "SELECT * FROM information_schema.database_privileges WHERE role_name = 'r1'",
                Ok(vec!["tenant_name,database_name,privilege_type,role_name"]),
                false, false,
            ), None, None
        ),
        RequestStep::new_boxed("grant role", SqlNoResult::build_request_with_str(
                url_test,
                "GRANT ALL ON DATABASE db1 TO ROLE r1",
                Ok(()),
            ), None, None
        ),
        RequestStep::new_boxed("drop role", SqlNoResult::build_request_with_str(
                url_test,
                "DROP ROLE r1",
                Ok(()),
            ), None, None
        ),
        ControlStep::new_boxed_restart_data_node("restart", 0),
        RequestStep::new_boxed("select role", Sql::build_request_with_str(
                url_test,
                "SELECT * FROM information_schema.roles WHERE role_name = 'r1'",
                Ok(vec!["role_name,role_type,inherit_role"]),
                false, false,
            ), None, None
        ),
        RequestStep::new_boxed("create role", SqlNoResult::build_request_with_str(
                url_test,
                "CREATE ROLE r1 INHERIT member",
                Ok(()),
            ), None, None
        ),
        RequestStep::new_boxed("select database_privilege", Sql::build_request_with_str(
                url_test,
                "SELECT * FROM information_schema.database_privileges WHERE role_name = 'r1'",
                Ok(vec!["tenant_name,database_name,privilege_type,role_name"]),
                false, false,
            ), None, None
        ),
    ];
    executor.execute_steps(&steps);
}

#[test]
fn case6() {
    let mut ctx = E2eContext::new("restart_tests", "case_6");
    let mut executor = ctx.build_executor(cluster_def::one_meta_three_data());

    let http_service_addr = executor.cluster_definition().data_cluster_def[0].http_host_port;
    let url_cnosdb_public = &format!("http://{http_service_addr}/api/v1/sql?db=public");
    let url_cnosdb_db1 = &format!("http://{http_service_addr}/api/v1/sql?db=db1");

    let steps: Vec<StepPtr> = vec![
        ControlStep::new_boxed_sleep("sleep 5s", 5),
        RequestStep::new_boxed("create database", SqlNoResult::build_request_with_str(
                url_cnosdb_public,
                "create database db1 with replica 3",
                Ok(()),
            ), None, None
        ),
        ControlStep::new_boxed_sleep("sleep 1s", 1),
        RequestStep::new_boxed("create table", SqlNoResult::build_request_with_str(
                url_cnosdb_db1,
                "CREATE TABLE air (visibility DOUBLE,temperature DOUBLE,pressure DOUBLE,TAGS(station))",
                Ok(()),
            ), None, None
        ),
        ControlStep::new_boxed_sleep("sleep 1s", 1),
        RequestStep::new_boxed("insert data", SqlNoResult::build_request_with_str(
                url_cnosdb_db1,
                "INSERT INTO air (TIME, station, visibility, temperature, pressure) VALUES(1666165200290401000, 'XiaoMaiDao', 56, 69, 77)",
                Ok(()),
            ), None, None
        ),
        ControlStep::new_boxed_sleep("sleep 1s", 1),
        ControlStep::new_boxed_stop_data_node("stop data node 1", 1),
        ControlStep::new_boxed_sleep("sleep 60s", 60),
        RequestStep::new_boxed("drop database db1, ignore error responses", SqlNoResult::build_request_with_str(
                url_cnosdb_public,
                "drop database db1",
                Err(E2eError::Ignored), // Error is expected, but the error message is not checked.
            ), None, None
        ),
        ControlStep::new_boxed_sleep("sleep 1s", 1),
        RequestStep::new_boxed("select resource_status after stop data node 1", Sql::build_request_with_str(
                url_cnosdb_public,
                "select name,action,try_count,status from information_schema.resource_status where name = 'cnosdb-db1'",
                Ok(vec!["name,action,try_count,status", r"cnosdb-db1,DropDatabase,\d+,Failed"]),
                false, true,
            ), None, None
        ),
        ControlStep::new_boxed_start_data_node("start data node 1", 1),
        ControlStep::new_boxed_sleep("sleep 30s", 30),
        RequestStep::new_boxed("select resource_status after start data node 1", Sql::build_request_with_str(
                url_cnosdb_public,
                "select name,action,try_count,status from information_schema.resource_status where name = 'cnosdb-db1'",
                Ok(vec!["name,action,try_count,status", r"cnosdb-db1,DropDatabase,\d+,Successed"]),
                false, true,
            ), None, None
        ),
    ];
    executor.execute_steps(&steps);
}

#[test]
#[serial_test::serial]
fn case8_count_after_restart_cluster() {
    let mut ctx = E2eContext::new("restart_tests", "case8_count_after_restart_cluster");
    let mut executor = ctx.build_executor(CnosdbClusterDefinition::with_ids(&[1], &[1, 2]));
    let host_port = executor.cluster_definition().data_cluster_def[0].http_host_port;

    const CACHE_SIZE: usize = 1_048_576;

    executor.set_update_data_config_fn_vec(vec![
        Some(Box::new(|c| {
            c.wal.max_file_size = CACHE_SIZE as u64;
            c.cluster.raft_logs_to_keep = 5;
            c.cluster.trigger_snapshot_interval = Duration::new(1, 0);
            c.global.store_metrics = false;
            c.cache.max_buffer_size = 2_097_152;
        })),
        Some(Box::new(|c| {
            c.wal.max_file_size = CACHE_SIZE as u64;
            c.cluster.raft_logs_to_keep = 5;
            c.cluster.trigger_snapshot_interval = Duration::new(1, 0);
            c.global.store_metrics = false;
            c.cache.max_buffer_size = 2_097_152
        })),
    ]);

    let steps: Vec<StepPtr> = vec![
        RequestStep::new_boxed(
            "create database db1",
            SqlNoResult::build_request_with_str(
                format!("http://{host_port}/api/v1/sql?db=public"),
                "create database db1 with shard 4 replica 2",
                Ok(()),
            ),
            None,
            None,
        ),
        FunctionStep::new_boxed(
            "write data to db1",
            Box::new(move |context| {
                let client = context.data_client(0).clone();
                let mut buffer = String::with_capacity(CACHE_SIZE);
                let url = &format!("http://{host_port}/api/v1/write?db=db1");
                for i in 0..30000 {
                    let random_number = rand::thread_rng().gen_range(1000..10000);
                    let four_digit_float: f64 = rand::thread_rng().gen_range(0.0..1.0) * 10000.0;
                    let random_string: String = rand::thread_rng()
                        .sample_iter(&Alphanumeric)
                        .take(300)
                        .map(char::from)
                        .collect();
                    if let Err(e) = writeln!(
                        &mut buffer,
                        "tb1,t1=t1a,t2=t2a,t3=t3a f1={}i,f2={},f3=\"{}\" {}",
                        random_number, four_digit_float, random_string, i
                    ) {
                        return CaseFlowControl::Error(e.to_string());
                    }
                    if buffer.len() < CACHE_SIZE {
                        continue;
                    }
                    check_response!(client.post(url, &buffer));
                    buffer.clear();
                }
                if !buffer.is_empty() {
                    check_response!(client.post(url, &buffer));
                }
                CaseFlowControl::Continue
            }),
        ),
        RequestStep::new_boxed(
            "create database db1",
            Sql::build_request_with_str(
                format!("http://{host_port}/api/v1/sql?db=db1"),
                "select count(*) from tb1",
                Ok(vec!["COUNT(UInt8(1))", "30000"]),
                false,
                false,
            ),
            None,
            None,
        ),
        ControlStep::new_boxed_restart_cluster("restart cluster"),
        RequestStep::new_boxed(
            "create database db1",
            Sql::build_request_with_str(
                format!("http://{host_port}/api/v1/sql?db=db1"),
                "select count(*) from tb1",
                Ok(vec!["COUNT(UInt8(1))", "30000"]),
                false,
                false,
            ),
            None,
            None,
        ),
    ];
    executor.execute_steps(&steps);
}
