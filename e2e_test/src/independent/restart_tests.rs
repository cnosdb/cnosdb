#![cfg(test)]

use reqwest::StatusCode;
use serial_test::serial;

use crate::case::{CnosdbAuth, CnosdbRequest, E2eExecutor, Step};
use crate::{cluster_def, E2eError};

#[test]
#[serial]
fn case1() {
    let url = "http://127.0.0.1:8902/api/v1/sql?tenant=cnosdb&db=public";

    let executor = E2eExecutor::new_singleton("restart_tests", "case_1", cluster_def::one_data(1));
    executor.execute_steps(&[
        Step::CnosdbRequest {
            req: CnosdbRequest::Query {
                url,
                sql: "CREATE TABLE air (visibility DOUBLE, temperature DOUBLE, pressure DOUBLE, TAGS(station))",
                resp: Ok(vec![]),
                sorted: false,
                regex: false,
            },
            auth: None,
        },
        Step::CnosdbRequest {
            req: CnosdbRequest::Query { url, sql: "SHOW TABLES", resp: Ok(vec!["table_name", "air"]), sorted: false, regex: false, },
            auth: None,
        },
        Step::RestartDataNode(0),
        Step::CnosdbRequest {
            req: CnosdbRequest::Query { url, sql: "SHOW TABLES", resp: Ok(vec!["table_name", "air"]), sorted: false, regex: false, },
            auth: None,
        },
        Step::CnosdbRequest {
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
        Step::CnosdbRequest {
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
                regex: false,
            },
            auth: None,
        },
        Step::RestartDataNode(0),
        Step::CnosdbRequest {
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
                regex: false,
            },
            auth: None,
        },
        Step::CnosdbRequest {
            req: CnosdbRequest::Ddl { url, sql: "DROP TABLE air", resp: Ok(()) },
            auth: None,
        },
        Step::RestartDataNode(0),

        Step::CnosdbRequest {
            req: CnosdbRequest::Ddl { url, sql: "SELECT * FROM air", 
            resp:Err(E2eError::Api {
                status: StatusCode::UNPROCESSABLE_ENTITY,
                url: None,
                req: None,
                resp: Some(r#"{"error_code":"010001","error_message":"Datafusion: Error during planning: Table not found, tenant: cnosdb db: public, table: air"}"#.to_string()),
            }), },
            auth: None,
        },

        Step::CnosdbRequest {
            req: CnosdbRequest::Query { url, sql: "SHOW TABLES", resp: Ok(vec!["table_name"]), sorted: false, regex: false, },
            auth: None,
        },
    ]);
}

#[test]
#[serial]
fn case2() {
    let url = "http://127.0.0.1:8902/api/v1/sql?tenant=cnosdb&db=public";

    let executor = E2eExecutor::new_singleton("restart_tests", "case_2", cluster_def::one_data(1));
    executor.execute_steps(&[
        Step::CnosdbRequest {
            req: CnosdbRequest::Ddl {
                url,
                sql: "CREATE TABLE air (visibility DOUBLE, temperature DOUBLE, pressure DOUBLE, TAGS(station))",
                resp: Ok(()),
            },
            auth: None,
        },
        Step::CnosdbRequest {
            req: CnosdbRequest::Ddl {
                url,
                sql: "ALTER TABLE air ADD FIELD humidity DOUBLE",
                resp: Ok(()),
            },
            auth: None,
        },
        Step::RestartDataNode(0),
        Step::CnosdbRequest {
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
                sorted: true,
                regex: false,
            },
            auth: None,
        },
        Step::CnosdbRequest {
            req: CnosdbRequest::Ddl {
                url,
                sql: "ALTER TABLE air ALTER humidity SET CODEC(QUANTILE)",
                resp: Ok(()),
            },
            auth: None,
        },
        Step::RestartDataNode(0),
        Step::CnosdbRequest {
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
                regex: false,
            },
            auth: None,
        },
        Step::CnosdbRequest {
            req: CnosdbRequest::Ddl {
                url,
                sql: "ALTER TABLE air DROP humidity",
                resp: Ok(()),
            },
            auth: None,
        },
        Step::RestartDataNode(0),
        Step::CnosdbRequest {
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
                regex: false,
            },
            auth: None,
        },
        Step::CnosdbRequest {
            req: CnosdbRequest::Ddl {
                url,
                sql: "ALTER TABLE air ADD TAG height",
                resp: Ok(()),
            },
            auth: None,
        },
        Step::RestartDataNode(0),
        Step::CnosdbRequest {
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
                regex: false,
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
    executor.execute_steps(&[
        Step::CnosdbRequest {
            req: CnosdbRequest::Ddl {
                url,
                sql: "CREATE DATABASE oceanic_station",
                resp: Ok(()),
            },
            auth: None,
        },
        Step::RestartDataNode(0),
        Step::CnosdbRequest {
            req: CnosdbRequest::Query {
                url,
                sql: "SHOW DATABASES",
                resp: Ok(vec![
                    "cluster_schema",
                    "database_name",
                    "oceanic_station",
                    "public",
                    "usage_schema",
                ]),
                sorted: true,
                regex: false,
            },
            auth: None,
        },
        Step::CnosdbRequest {
            req: CnosdbRequest::Ddl {
                url,
                sql: "ALTER DATABASE oceanic_station SET VNODE_DURATION '1000d'",
                resp: Ok(()),
            },
            auth: None,
        },
        Step::RestartDataNode(0),
        Step::CnosdbRequest {
            req: CnosdbRequest::Query {
                url,
                sql: "DESC DATABASE oceanic_station",
                resp: Ok(vec![
                    "ttl,shard,vnode_duration,replica,precision,max_memcache_size,memcache_partitions,wal_max_file_size,wal_sync,strict_write,max_cache_readers",
                    "INF,1,2years 8months 25days 23h 31m 12s,1,NS,128 MiB,4,1 GiB,false,false,32"
                ]),
                sorted: false,
                regex: false,
            },
            auth: None,
        },
        Step::CnosdbRequest {
            req: CnosdbRequest::Ddl {
                url,
                sql: "DROP DATABASE oceanic_station",
                resp: Ok(()),
            },
            auth: None,
        },
        Step::RestartDataNode(0),
        Step::CnosdbRequest {
            req: CnosdbRequest::Query {
                url,
                sql: "SHOW DATABASES",
                resp: Ok(vec![
                    "cluster_schema",
                    "database_name",
                    "public",
                    "usage_schema",
                ]),
                sorted: true,
                regex: false,
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
    executor.execute_steps(&[
        Step::CnosdbRequest {
            req: CnosdbRequest::Ddl {
                url: url_cnosdb_public,
                sql: "CREATE USER IF NOT EXISTS tester",
                resp: Ok(()),
            },
            auth: None,
        },
        Step::RestartDataNode(0),
        Step::CnosdbRequest {
            req: CnosdbRequest::Query {
                url: url_cnosdb_public,
                sql: "SELECT * FROM cluster_schema.users WHERE user_name = 'tester'",
                resp: Ok(vec![
                    "user_name,is_admin,user_options",
                    r#"tester,false,"{""hash_password"":""*****""}""#,
                ]),
                sorted: false,
                regex: false,
            },
            auth: None,
        },
        Step::CnosdbRequest {
            req: CnosdbRequest::Ddl {
                url: url_cnosdb_public,
                sql: "alter user tester set granted_admin = true",
                resp: Ok(()),
            },
            auth: None,
        },
        Step::RestartDataNode(0),
        Step::CnosdbRequest {
            req: CnosdbRequest::Query {
                url: url_cnosdb_public,
                sql: "SELECT * FROM cluster_schema.users WHERE user_name = 'tester'",
                resp: Ok(vec![
                    "user_name,is_admin,user_options",
                    r#"tester,true,"{""hash_password"":""*****"",""granted_admin"":true}""#,
                ]),
                sorted: false,
                regex: false,
            },
            auth: None,
        },
        Step::CnosdbRequest {
            req: CnosdbRequest::Ddl {
                url: url_cnosdb_public,
                sql: "alter user tester set granted_admin = false",
                resp: Ok(()),
            },
            auth: None,
        },
        Step::RestartDataNode(0),
        Step::CnosdbRequest {
            req: CnosdbRequest::Query {
                url: url_cnosdb_public,
                sql: "SELECT * FROM cluster_schema.users WHERE user_name = 'tester'",
                resp: Ok(vec![
                    "user_name,is_admin,user_options",
                    r#"tester,false,"{""hash_password"":""*****"",""granted_admin"":false}""#,
                ]),
                sorted: false,
                regex: false,
            },
            auth: None,
        },
        Step::CnosdbRequest {
            req: CnosdbRequest::Ddl {
                url: url_cnosdb_public,
                sql: "ALTER USER tester SET COMMENT = 'bbb'",
                resp: Ok(()),
            },
            auth: None,
        },
        Step::RestartDataNode(0),
        Step::CnosdbRequest {
            req: CnosdbRequest::Query {
                url: url_cnosdb_public,
                sql: "SELECT * FROM cluster_schema.users WHERE user_name = 'tester'",
                resp: Ok(vec![
                    "user_name,is_admin,user_options",
                    r#"tester,false,"{""hash_password"":""*****"",""comment"":""bbb"",""granted_admin"":false}""#,
                ]),
                sorted: false,
                regex: false,
            },
            auth: None,
        },
        Step::CnosdbRequest {
            req: CnosdbRequest::Ddl {
                url: url_cnosdb_public,
                sql: "DROP USER tester",
                resp: Ok(()),
            },
            auth: None,
        },
        Step::RestartDataNode(0),
        Step::CnosdbRequest {
            req: CnosdbRequest::Query {
                url: url_cnosdb_public,
                sql: "SELECT * FROM cluster_schema.users WHERE user_name = 'tester'",
                resp: Ok(vec!["user_name,is_admin,user_options"]),
                sorted: false,
                regex: false,
            },
            auth: None,
        },
        Step::CnosdbRequest {
            req: CnosdbRequest::Ddl {
                url: url_cnosdb_public,
                sql: "CREATE TENANT test",
                resp: Ok(()),
            },
            auth: None,
        },
        Step::RestartDataNode(0),
        Step::CnosdbRequest {
            req: CnosdbRequest::Query {
                url: url_cnosdb_public,
                sql: "SELECT * FROM cluster_schema.tenants WHERE tenant_name = 'test'",
                resp: Ok(vec![
                    "tenant_name,tenant_options",
                    "test,\"{\"\"comment\"\":null,\"\"limiter_config\"\":null,\"\"drop_after\"\":null,\"\"tenant_is_hidden\"\":false}\""
                ]),
                sorted: false,
                regex: false,
            },
            auth: None,
        },
        Step::CnosdbRequest {
            req: CnosdbRequest::Ddl {
                url: url_cnosdb_public,
                sql: "ALTER TENANT test SET COMMENT = 'abc'",
                resp: Ok(()),
            },
            auth: None,
        },
        Step::RestartDataNode(0),
        Step::CnosdbRequest {
            req: CnosdbRequest::Query {
                url: url_cnosdb_public,
                sql: "SELECT * FROM cluster_schema.tenants WHERE tenant_name = 'test'",
                resp: Ok(vec![
                    "tenant_name,tenant_options",
                    "test,\"{\"\"comment\"\":\"\"abc\"\",\"\"limiter_config\"\":null,\"\"drop_after\"\":null,\"\"tenant_is_hidden\"\":false}\""
                ]),
                sorted: false,
                regex: false,
            },
            auth: None,
        },
        Step::CnosdbRequest {
            req: CnosdbRequest::Ddl {
                url: url_test_,
                sql: "CREATE DATABASE db1",
                resp: Ok(()),
            },
            auth: None,
        },
        Step::CnosdbRequest {
            req: CnosdbRequest::Ddl {
                url: url_cnosdb_public,
                sql: "DROP TENANT test",
                resp: Ok(()),
            },
            auth: None,
        },
        Step::RestartDataNode(0),
        Step::CnosdbRequest {
            req: CnosdbRequest::Ddl {
                url: url_cnosdb_public,
                sql: "CREATE TENANT test",
                resp: Ok(()),
            },
            auth: None,
        },
        Step::CnosdbRequest {
            req: CnosdbRequest::Query {
                url: url_test_,
                sql: "SHOW DATABASES",
                resp: Ok(vec!["database_name"]),
                sorted: false,
                regex: false,
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
    executor.execute_steps(&[
        Step::CnosdbRequest {
            req: CnosdbRequest::Ddl {
                url: url_cnosdb_public,
                sql: "CREATE USER tester",
                resp: Ok(()),
            },
            auth: None,
        },
        Step::CnosdbRequest {
            req: CnosdbRequest::Ddl {
                url: url_cnosdb_public,
                sql: "CREATE TENANT test",
                resp: Ok(()),
            },
            auth: None,
        },
        Step::CnosdbRequest {
            req: CnosdbRequest::Ddl {
                url: url_test_,
                sql: "CREATE ROLE r1 INHERIT member",
                resp: Ok(()),
            },
            auth: None,
        },
        Step::RestartDataNode(0),
        Step::CnosdbRequest {
            req: CnosdbRequest::Query {
                url: url_test_,
                sql: "SELECT * FROM information_schema.roles WHERE role_name = 'r1'",
                resp: Ok(vec!["role_name,role_type,inherit_role", "r1,custom,member"]),
                sorted: false,
                regex: false,
            },
            auth: None,
        },
        Step::CnosdbRequest {
            req: CnosdbRequest::Ddl {
                url: url_test_,
                sql: "ALTER TENANT test ADD USER tester AS r1",
                resp: Ok(()),
            },
            auth: None,
        },
        Step::RestartDataNode(0),
        Step::CnosdbRequest {
            req: CnosdbRequest::Query {
                url: url_test_,
                sql: "SELECT 1",
                resp: Ok(vec!["Int64(1)", "1"]),
                sorted: false,
                regex: false,
            },
            auth: Some(CnosdbAuth {
                username: "tester".to_string(),
                password: Some("".to_string()),
            }),
        },
        Step::CnosdbRequest {
            req: CnosdbRequest::Ddl {
                url: url_test_,
                sql: "ALTER TENANT test REMOVE USER tester",
                resp: Ok(()),
            },
            auth: None,
        },
        Step::RestartDataNode(0),
        Step::CnosdbRequest {
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
                regex: false,
            },
            auth: Some(CnosdbAuth {
                username: "tester".to_string(),
                password: Some("".to_string()),
            }),
        },
        Step::CnosdbRequest {
            req: CnosdbRequest::Ddl {
                url: url_test_,
                sql: "CREATE DATABASE db1",
                resp: Ok(()),
            },
            auth: None,
        },
        Step::CnosdbRequest {
            req: CnosdbRequest::Ddl {
                url: url_test_,
                sql: "GRANT WRITE ON DATABASE db1 TO ROLE r1",
                resp: Ok(()),
            },
            auth: None,
        },
        Step::RestartDataNode(0),
        Step::CnosdbRequest {
            req: CnosdbRequest::Query {
                url: url_test_,
                sql: "SELECT * FROM information_schema.database_privileges WHERE role_name = 'r1'",
                resp: Ok(vec![
                    "tenant_name,database_name,privilege_type,role_name",
                    "test,db1,Write,r1",
                ]),
                sorted: false,
                regex: false,
            },
            auth: None,
        },
        Step::CnosdbRequest {
            req: CnosdbRequest::Ddl {
                url: url_test_,
                sql: "REVOKE WRITE ON DATABASE db1 FROM r1",
                resp: Ok(()),
            },
            auth: None,
        },
        Step::RestartDataNode(0),
        Step::CnosdbRequest {
            req: CnosdbRequest::Query {
                url: url_test_,
                sql: "SELECT * FROM information_schema.database_privileges WHERE role_name = 'r1'",
                resp: Ok(vec!["tenant_name,database_name,privilege_type,role_name"]),
                sorted: false,
                regex: false,
            },
            auth: None,
        },
        Step::CnosdbRequest {
            req: CnosdbRequest::Ddl {
                url: url_test_,
                sql: "GRANT ALL ON DATABASE db1 TO ROLE r1",
                resp: Ok(()),
            },
            auth: None,
        },
        Step::CnosdbRequest {
            req: CnosdbRequest::Ddl {
                url: url_test_,
                sql: "DROP ROLE r1",
                resp: Ok(()),
            },
            auth: None,
        },
        Step::RestartDataNode(0),
        Step::CnosdbRequest {
            req: CnosdbRequest::Query {
                url: url_test_,
                sql: "SELECT * FROM information_schema.roles WHERE role_name = 'r1'",
                resp: Ok(vec!["role_name,role_type,inherit_role"]),
                sorted: false,
                regex: false,
            },
            auth: None,
        },
        Step::CnosdbRequest {
            req: CnosdbRequest::Ddl {
                url: url_test_,
                sql: "CREATE ROLE r1 INHERIT member",
                resp: Ok(()),
            },
            auth: None,
        },
        Step::CnosdbRequest {
            req: CnosdbRequest::Query {
                url: url_test_,
                sql: "SELECT * FROM information_schema.database_privileges WHERE role_name = 'r1'",
                resp: Ok(vec!["tenant_name,database_name,privilege_type,role_name"]),
                sorted: false,
                regex: false,
            },
            auth: None,
        },
    ]);
}

#[test]
fn case6() {
    let url_cnosdb_public = "http://127.0.0.1:8902/api/v1/sql?db=public";
    let url_cnosdb_db1 = "http://127.0.0.1:8902/api/v1/sql?db=db1";

    let executor = E2eExecutor::new_cluster(
        "restart_tests",
        "case_6",
        cluster_def::one_meta_three_data(),
    );
    executor.execute_steps(&[
        Step::Sleep(5),
        Step::CnosdbRequest {
            req: CnosdbRequest::Ddl {
                url: url_cnosdb_public,
                sql: "create database db1 with replica 3",
                resp: Ok(()),
            },
            auth: None,
        },
        Step::Sleep(1),
        Step::CnosdbRequest {
            req: CnosdbRequest::Ddl {
                url: url_cnosdb_db1,
                sql: "CREATE TABLE air (visibility DOUBLE,temperature DOUBLE,pressure DOUBLE,TAGS(station))",
                resp: Ok(()),
            },
            auth: None,
        },
        Step::Sleep(1),
        Step::CnosdbRequest {
            req: CnosdbRequest::Insert {
                url: url_cnosdb_db1,
                sql: "INSERT INTO air (TIME, station, visibility, temperature, pressure) VALUES(1666165200290401000, 'XiaoMaiDao', 56, 69, 77)",
                resp: Ok(()),
            },
            auth: None,
        },
        Step::Sleep(1),
        Step::StopDataNode(1),
        Step::Sleep(60),
        Step::CnosdbRequest {
            req: CnosdbRequest::Ddl {
                url: url_cnosdb_public,
                sql: "drop database db1",
                resp: Err(E2eError::Ignored), // Error is expected, but the error message is not checked.
            },
            auth: None,
        },
        Step::Sleep(1),
        Step::CnosdbRequest {
            req: CnosdbRequest::Query {
                url: url_cnosdb_public,
                sql: "select name,action,try_count,status from information_schema.resource_status where name = 'cnosdb-db1'",
                resp: Ok(vec!["name,action,try_count,status", r"cnosdb-db1,DropDatabase,\d+,Successed"]),
                sorted: false,
                regex: true,
            },
            auth: None,
        },
        Step::StartDataNode(1),
        Step::Sleep(30),
        Step::CnosdbRequest {
            req: CnosdbRequest::Query {
                url: url_cnosdb_public,
                sql: "select name,action,try_count,status from information_schema.resource_status where name = 'cnosdb-db1'",
                resp: Ok(vec!["name,action,try_count,status", r"cnosdb-db1,DropDatabase,\d+,Successed"]),
                sorted: false,
                regex: true,
            },
            auth: None,
        },
    ]);
}
