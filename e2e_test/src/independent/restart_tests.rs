#[cfg(test)]
pub mod test {
    use std::sync::Arc;
    use std::thread;
    use std::time::Duration;

    use http_protocol::status_code;
    use regex::Regex;

    use crate::utils::{clean_env, start_cluster, start_singleton, Client};

    #[test]
    fn case1() {
        println!("Test begin restart_test_case_1");
        clean_env();
        let mut data = start_singleton();

        let client = Client::new("root".to_string(), Some(String::new()));

        let resp = client
            .post(
                "http://127.0.0.1:8902/api/v1/sql?db=public",
                "CREATE TABLE air (
                    visibility DOUBLE,
                    temperature DOUBLE,
                    pressure DOUBLE,
                    TAGS(station))",
            )
            .unwrap();

        assert_eq!(resp.status(), status_code::OK);

        data.restart_singleton("config_8902");

        let resp = client
            .post("http://127.0.0.1:8902/api/v1/sql?db=public", "SHOW TABLES")
            .unwrap();

        assert_eq!(resp.status(), status_code::OK);
        assert_eq!(
            resp.text()
                .unwrap()
                .trim()
                .split_terminator('\n')
                .collect::<Vec<_>>(),
            vec!["table_name", "air"]
        );

        let resp = client
            .post(
                "http://127.0.0.1:8902/api/v1/sql?db=public",
                "INSERT INTO air (time, station, visibility, temperature, pressure) VALUES
                    ('2023-01-01 01:10:00', 'XiaoMaiDao', 79, 80, 63),
                    ('2023-01-01 01:20:00', 'XiaoMaiDao', 80, 60, 63),
                    ('2023-01-01 01:30:00', 'XiaoMaiDao', 81, 70, 61)",
            )
            .unwrap();

        assert_eq!(resp.status(), status_code::OK);

        data.restart_singleton("config_8902");

        let resp = client
            .post(
                "http://127.0.0.1:8902/api/v1/sql?db=public",
                "SELECT * FROM air order by time",
            )
            .unwrap();

        assert_eq!(resp.status(), status_code::OK);
        assert_eq!(
            resp.text()
                .unwrap()
                .split_terminator('\n')
                .collect::<Vec<_>>(),
            vec![
                "time,station,visibility,temperature,pressure",
                "2023-01-01T01:10:00.000000000,XiaoMaiDao,79.0,80.0,63.0",
                "2023-01-01T01:20:00.000000000,XiaoMaiDao,80.0,60.0,63.0",
                "2023-01-01T01:30:00.000000000,XiaoMaiDao,81.0,70.0,61.0"
            ]
        );

        let resp = client
            .post(
                "http://127.0.0.1:8902/api/v1/sql?db=public",
                "DROP TABLE air",
            )
            .unwrap();

        assert_eq!(resp.status(), status_code::OK);

        data.restart_singleton("config_8902");

        let resp = client
            .post("http://127.0.0.1:8902/api/v1/sql?db=public", "SHOW TABLES")
            .unwrap();

        assert_eq!(resp.status(), status_code::OK);
        assert!(resp.text().unwrap().trim().is_empty());

        println!("Test complete restart_test_case_1");
    }

    #[test]
    fn case2() {
        println!("Test begin restart_test_case_2");
        clean_env();
        let mut data = start_singleton();

        let client = Client::new("root".to_string(), Some(String::new()));

        let resp = client
            .post(
                "http://127.0.0.1:8902/api/v1/sql?db=public",
                "CREATE TABLE air (
                    visibility DOUBLE,
                    temperature DOUBLE,
                    pressure DOUBLE,
                    TAGS(station))",
            )
            .unwrap();

        assert_eq!(resp.status(), status_code::OK);

        let resp = client
            .post(
                "http://127.0.0.1:8902/api/v1/sql?db=public",
                "ALTER TABLE air ADD FIELD humidity DOUBLE",
            )
            .unwrap();

        assert_eq!(resp.status(), status_code::OK);

        data.restart_singleton("config_8902");

        let resp = client
            .post(
                "http://127.0.0.1:8902/api/v1/sql?db=public",
                "DESC TABLE air",
            )
            .unwrap();

        assert_eq!(resp.status(), status_code::OK);
        assert_eq!(
            {
                let mut res = resp
                    .text()
                    .unwrap()
                    .trim()
                    .split_terminator('\n')
                    .map(|s| s.to_owned())
                    .collect::<Vec<_>>();
                res.sort_unstable();
                res
            },
            vec![
                "column_name,data_type,column_type,compression_codec",
                "humidity,DOUBLE,FIELD,DEFAULT",
                "pressure,DOUBLE,FIELD,DEFAULT",
                "station,STRING,TAG,DEFAULT",
                "temperature,DOUBLE,FIELD,DEFAULT",
                "time,TIMESTAMP(NANOSECOND),TIME,DEFAULT",
                "visibility,DOUBLE,FIELD,DEFAULT"
            ]
        );

        let resp = client
            .post(
                "http://127.0.0.1:8902/api/v1/sql?db=public",
                "ALTER TABLE air ALTER humidity SET CODEC(QUANTILE)",
            )
            .unwrap();

        assert_eq!(resp.status(), status_code::OK);

        data.restart_singleton("config_8902");

        let resp = client
            .post(
                "http://127.0.0.1:8902/api/v1/sql?db=public",
                "DESC TABLE air",
            )
            .unwrap();

        assert_eq!(resp.status(), status_code::OK);
        assert_eq!(
            {
                let mut res = resp
                    .text()
                    .unwrap()
                    .trim()
                    .split_terminator('\n')
                    .map(|s| s.to_owned())
                    .collect::<Vec<_>>();
                res.sort_unstable();
                res
            },
            vec![
                "column_name,data_type,column_type,compression_codec",
                "humidity,DOUBLE,FIELD,QUANTILE",
                "pressure,DOUBLE,FIELD,DEFAULT",
                "station,STRING,TAG,DEFAULT",
                "temperature,DOUBLE,FIELD,DEFAULT",
                "time,TIMESTAMP(NANOSECOND),TIME,DEFAULT",
                "visibility,DOUBLE,FIELD,DEFAULT"
            ]
        );

        let resp = client
            .post(
                "http://127.0.0.1:8902/api/v1/sql?db=public",
                "ALTER TABLE air DROP humidity",
            )
            .unwrap();

        assert_eq!(resp.status(), status_code::OK);

        data.restart_singleton("config_8902");

        let resp = client
            .post(
                "http://127.0.0.1:8902/api/v1/sql?db=public",
                "DESC TABLE air",
            )
            .unwrap();

        assert_eq!(resp.status(), status_code::OK);
        assert_eq!(
            {
                let mut res = resp
                    .text()
                    .unwrap()
                    .trim()
                    .split_terminator('\n')
                    .map(|s| s.to_owned())
                    .collect::<Vec<_>>();
                res.sort_unstable();
                res
            },
            vec![
                "column_name,data_type,column_type,compression_codec",
                "pressure,DOUBLE,FIELD,DEFAULT",
                "station,STRING,TAG,DEFAULT",
                "temperature,DOUBLE,FIELD,DEFAULT",
                "time,TIMESTAMP(NANOSECOND),TIME,DEFAULT",
                "visibility,DOUBLE,FIELD,DEFAULT"
            ]
        );

        let resp = client
            .post(
                "http://127.0.0.1:8902/api/v1/sql?db=public",
                "ALTER TABLE air ADD TAG height",
            )
            .unwrap();

        assert_eq!(resp.status(), status_code::OK);

        data.restart_singleton("config_8902");

        let resp = client
            .post(
                "http://127.0.0.1:8902/api/v1/sql?db=public",
                "DESC TABLE air",
            )
            .unwrap();

        assert_eq!(resp.status(), status_code::OK);
        assert_eq!(
            {
                let mut res = resp
                    .text()
                    .unwrap()
                    .trim()
                    .split_terminator('\n')
                    .map(|s| s.to_owned())
                    .collect::<Vec<_>>();
                res.sort_unstable();
                res
            },
            vec![
                "column_name,data_type,column_type,compression_codec",
                "height,STRING,TAG,DEFAULT",
                "pressure,DOUBLE,FIELD,DEFAULT",
                "station,STRING,TAG,DEFAULT",
                "temperature,DOUBLE,FIELD,DEFAULT",
                "time,TIMESTAMP(NANOSECOND),TIME,DEFAULT",
                "visibility,DOUBLE,FIELD,DEFAULT"
            ]
        );

        println!("Test complete restart_test_case_2");
    }

    #[test]
    fn case3() {
        println!("Test begin restart_test_case_3");
        clean_env();
        let mut data = start_singleton();

        let client = Client::new("root".to_string(), Some(String::new()));

        let resp = client
            .post(
                "http://127.0.0.1:8902/api/v1/sql?db=public",
                "CREATE DATABASE oceanic_station",
            )
            .unwrap();

        assert_eq!(resp.status(), status_code::OK);

        data.restart_singleton("config_8902");

        let resp = client
            .post(
                "http://127.0.0.1:8902/api/v1/sql?db=public",
                "SHOW DATABASES",
            )
            .unwrap();

        assert_eq!(resp.status(), status_code::OK);
        assert_eq!(
            {
                let mut res = resp
                    .text()
                    .unwrap()
                    .trim()
                    .split_terminator('\n')
                    .map(|s| s.to_owned())
                    .collect::<Vec<_>>();
                res.sort_unstable();
                res
            },
            vec!["database_name", "oceanic_station", "public", "usage_schema"]
        );

        let resp = client
            .post(
                "http://127.0.0.1:8902/api/v1/sql?db=public",
                "ALTER DATABASE oceanic_station SET VNODE_DURATION '1000d'",
            )
            .unwrap();

        assert_eq!(resp.status(), status_code::OK);

        data.restart_singleton("config_8902");

        let resp = client
            .post(
                "http://127.0.0.1:8902/api/v1/sql?db=public",
                "DESC DATABASE oceanic_station",
            )
            .unwrap();

        assert_eq!(resp.status(), status_code::OK);
        assert_eq!(
            resp.text()
                .unwrap()
                .trim()
                .split_terminator('\n')
                .collect::<Vec<_>>(),
            vec![
                "ttl,shard,vnode_duration,replica,precision",
                "INF,1,1000 Days,1,NS"
            ]
        );

        let resp = client
            .post(
                "http://127.0.0.1:8902/api/v1/sql?db=public",
                "DROP DATABASE oceanic_station",
            )
            .unwrap();

        assert_eq!(resp.status(), status_code::OK);

        data.restart_singleton("config_8902");

        let resp = client
            .post(
                "http://127.0.0.1:8902/api/v1/sql?db=public",
                "SHOW DATABASES",
            )
            .unwrap();

        assert_eq!(resp.status(), status_code::OK);
        assert_eq!(
            {
                let mut res = resp
                    .text()
                    .unwrap()
                    .trim()
                    .split_terminator('\n')
                    .map(|s| s.to_owned())
                    .collect::<Vec<_>>();
                res.sort_unstable();
                res
            },
            vec!["database_name", "public", "usage_schema"]
        );

        println!("Test complete restart_test_case_3");
    }

    #[test]
    fn case4() {
        println!("Test begin restart_test_case_4");
        clean_env();
        let mut data = start_singleton();

        let client = Client::new("root".to_string(), Some(String::new()));

        let resp = client
            .post(
                "http://127.0.0.1:8902/api/v1/sql?db=public",
                "CREATE USER IF NOT EXISTS tester",
            )
            .unwrap();

        assert_eq!(resp.status(), status_code::OK);

        data.restart_singleton("config_8902");

        let resp = client
            .post(
                "http://127.0.0.1:8902/api/v1/sql?db=public",
                "SELECT * FROM cluster_schema.users WHERE user_name = 'tester'",
            )
            .unwrap();

        assert_eq!(resp.status(), status_code::OK);
        assert_eq!(
            resp.text()
                .unwrap()
                .split_terminator('\n')
                .collect::<Vec<_>>(),
            vec![
                "user_name,is_admin,user_options",
                r#"tester,false,"{""hash_password"":""*****"",""must_change_password"":false,""granted_admin"":false}""#
            ]
        );

        let resp = client
            .post(
                "http://127.0.0.1:8902/api/v1/sql?db=public",
                "alter user tester set granted_admin = true",
            )
            .unwrap();

        assert_eq!(resp.status(), status_code::OK);

        data.restart_singleton("config_8902");

        let resp = client
            .post(
                "http://127.0.0.1:8902/api/v1/sql?db=public",
                "SELECT * FROM cluster_schema.users WHERE user_name = 'tester'",
            )
            .unwrap();

        assert_eq!(resp.status(), status_code::OK);
        assert_eq!(
            resp.text()
                .unwrap()
                .split_terminator('\n')
                .collect::<Vec<_>>(),
            vec![
                "user_name,is_admin,user_options",
                r#"tester,true,"{""hash_password"":""*****"",""must_change_password"":false,""granted_admin"":true}""#
            ]
        );

        let resp = client
            .post(
                "http://127.0.0.1:8902/api/v1/sql?db=public",
                "alter user tester set granted_admin = false",
            )
            .unwrap();

        assert_eq!(resp.status(), status_code::OK);

        data.restart_singleton("config_8902");

        let resp = client
            .post(
                "http://127.0.0.1:8902/api/v1/sql?db=public",
                "SELECT * FROM cluster_schema.users WHERE user_name = 'tester'",
            )
            .unwrap();

        assert_eq!(resp.status(), status_code::OK);
        assert_eq!(
            resp.text()
                .unwrap()
                .split_terminator('\n')
                .collect::<Vec<_>>(),
            vec![
                "user_name,is_admin,user_options",
                r#"tester,false,"{""hash_password"":""*****"",""must_change_password"":false,""granted_admin"":false}""#
            ]
        );

        let resp = client
            .post(
                "http://127.0.0.1:8902/api/v1/sql?db=public",
                "ALTER USER tester SET COMMENT = 'bbb'",
            )
            .unwrap();

        assert_eq!(resp.status(), status_code::OK);

        data.restart_singleton("config_8902");

        let resp = client
            .post(
                "http://127.0.0.1:8902/api/v1/sql?db=public",
                "SELECT * FROM cluster_schema.users WHERE user_name = 'tester'",
            )
            .unwrap();

        assert_eq!(resp.status(), status_code::OK);
        assert_eq!(
            resp.text()
                .unwrap()
                .split_terminator('\n')
                .collect::<Vec<_>>(),
            vec![
                "user_name,is_admin,user_options",
                r#"tester,false,"{""hash_password"":""*****"",""must_change_password"":false,""comment"":""bbb"",""granted_admin"":false}""#
            ]
        );

        let resp = client
            .post(
                "http://127.0.0.1:8902/api/v1/sql?db=public",
                "DROP USER tester",
            )
            .unwrap();

        assert_eq!(resp.status(), status_code::OK);

        data.restart_singleton("config_8902");

        let resp = client
            .post(
                "http://127.0.0.1:8902/api/v1/sql?db=public",
                "SELECT * FROM cluster_schema.users WHERE user_name = 'tester'",
            )
            .unwrap();

        assert_eq!(resp.status(), status_code::OK);
        assert!(resp.text().unwrap().trim().is_empty());

        let resp = client
            .post(
                "http://127.0.0.1:8902/api/v1/sql?db=public",
                "CREATE TENANT test",
            )
            .unwrap();

        assert_eq!(resp.status(), status_code::OK);

        data.restart_singleton("config_8902");

        let resp = client
            .post(
                "http://127.0.0.1:8902/api/v1/sql?db=public",
                "SELECT * FROM cluster_schema.tenants WHERE tenant_name = 'test'",
            )
            .unwrap();

        assert_eq!(resp.status(), status_code::OK);
        assert_eq!(
            resp.text()
                .unwrap()
                .split_terminator('\n')
                .collect::<Vec<_>>(),
            vec![
                "tenant_name,tenant_options",
                "test,\"{\"\"comment\"\":null,\"\"limiter_config\"\":null,\"\"after\"\":null,\"\"tenant_is_hidden\"\":false}\""
            ]
        );

        let resp = client
            .post(
                "http://127.0.0.1:8902/api/v1/sql?db=public",
                "ALTER TENANT test SET COMMENT = 'abc'",
            )
            .unwrap();

        assert_eq!(resp.status(), status_code::OK);

        data.restart_singleton("config_8902");

        let resp = client
            .post(
                "http://127.0.0.1:8902/api/v1/sql?db=public",
                "SELECT * FROM cluster_schema.tenants WHERE tenant_name = 'test'",
            )
            .unwrap();

        assert_eq!(resp.status(), status_code::OK);
        assert_eq!(
            resp.text()
                .unwrap()
                .split_terminator('\n')
                .collect::<Vec<_>>(),
            vec![
                "tenant_name,tenant_options",
                "test,\"{\"\"comment\"\":\"\"abc\"\",\"\"limiter_config\"\":null,\"\"after\"\":null,\"\"tenant_is_hidden\"\":false}\""
            ]
        );

        let resp = client
            .post(
                "http://127.0.0.1:8902/api/v1/sql?tenant=test",
                "CREATE DATABASE db1",
            )
            .unwrap();

        assert_eq!(resp.status(), status_code::OK);

        let resp = client
            .post(
                "http://127.0.0.1:8902/api/v1/sql?db=public",
                "DROP TENANT test",
            )
            .unwrap();

        assert_eq!(resp.status(), status_code::OK);

        data.restart_singleton("config_8902");

        let resp = client
            .post(
                "http://127.0.0.1:8902/api/v1/sql?db=public",
                "CREATE TENANT test",
            )
            .unwrap();

        assert_eq!(resp.status(), status_code::OK);

        let resp = client
            .post(
                "http://127.0.0.1:8902/api/v1/sql?tenant=test",
                "SHOW DATABASES",
            )
            .unwrap();

        assert_eq!(resp.status(), status_code::OK);
        assert!(resp.text().unwrap().trim().is_empty());

        println!("Test complete restart_test_case_4");
    }

    #[test]
    fn case5() {
        println!("Test begin restart_test_case_5");
        clean_env();
        let mut data = start_singleton();

        let client = Client::new("root".to_string(), Some(String::new()));
        let tester_client = Client::new("tester".to_string(), Some(String::new()));

        let resp = client
            .post(
                "http://127.0.0.1:8902/api/v1/sql?db=public",
                "CREATE USER tester",
            )
            .unwrap();

        assert_eq!(resp.status(), status_code::OK);

        let resp = client
            .post(
                "http://127.0.0.1:8902/api/v1/sql?db=public",
                "CREATE TENANT test",
            )
            .unwrap();

        assert_eq!(resp.status(), status_code::OK);

        let resp = client
            .post(
                "http://127.0.0.1:8902/api/v1/sql?tenant=test",
                "CREATE ROLE r1 INHERIT member",
            )
            .unwrap();

        assert_eq!(resp.status(), status_code::OK);

        data.restart_singleton("config_8902");

        let resp = client
            .post(
                "http://127.0.0.1:8902/api/v1/sql?tenant=test",
                "SELECT * FROM information_schema.roles WHERE role_name = 'r1'",
            )
            .unwrap();

        assert_eq!(resp.status(), status_code::OK);
        assert_eq!(
            resp.text()
                .unwrap()
                .split_terminator('\n')
                .collect::<Vec<_>>(),
            vec!["role_name,role_type,inherit_role", "r1,custom,member"]
        );

        let resp = client
            .post(
                "http://127.0.0.1:8902/api/v1/sql?tenant=test",
                "ALTER TENANT test ADD USER tester AS r1",
            )
            .unwrap();

        assert_eq!(resp.status(), status_code::OK);

        data.restart_singleton("config_8902");

        let resp = tester_client
            .post("http://127.0.0.1:8902/api/v1/sql?tenant=test", "SELECT 1")
            .unwrap();

        assert_eq!(resp.status(), status_code::OK);
        assert_eq!(
            resp.text()
                .unwrap()
                .split_terminator('\n')
                .collect::<Vec<_>>(),
            vec!["Int64(1)", "1"]
        );

        let resp = client
            .post(
                "http://127.0.0.1:8902/api/v1/sql?tenant=test",
                "ALTER TENANT test REMOVE USER tester",
            )
            .unwrap();

        assert_eq!(resp.status(), status_code::OK);

        data.restart_singleton("config_8902");

        let resp = tester_client
            .post("http://127.0.0.1:8902/api/v1/sql?tenant=test", "SELECT 1")
            .unwrap();

        assert_eq!(
            resp.text().unwrap(),
            r#"{"error_code":"010016","error_message":"Auth error: The member tester of tenant test not found"}"#
        );

        let resp = client
            .post(
                "http://127.0.0.1:8902/api/v1/sql?tenant=test",
                "CREATE DATABASE db1",
            )
            .unwrap();

        assert_eq!(resp.status(), status_code::OK);

        let resp = client
            .post(
                "http://127.0.0.1:8902/api/v1/sql?tenant=test",
                "GRANT WRITE ON DATABASE db1 TO ROLE r1",
            )
            .unwrap();

        assert_eq!(resp.status(), status_code::OK);

        data.restart_singleton("config_8902");

        let resp = client
            .post(
                "http://127.0.0.1:8902/api/v1/sql?tenant=test",
                "SELECT * FROM information_schema.database_privileges WHERE role_name = 'r1'",
            )
            .unwrap();

        assert_eq!(resp.status(), status_code::OK);
        assert_eq!(
            resp.text()
                .unwrap()
                .split_terminator('\n')
                .collect::<Vec<_>>(),
            vec![
                "tenant_name,database_name,privilege_type,role_name",
                "test,db1,Write,r1"
            ]
        );

        let resp = client
            .post(
                "http://127.0.0.1:8902/api/v1/sql?tenant=test",
                "REVOKE WRITE ON DATABASE db1 FROM r1",
            )
            .unwrap();

        assert_eq!(resp.status(), status_code::OK);

        data.restart_singleton("config_8902");

        let resp = client
            .post(
                "http://127.0.0.1:8902/api/v1/sql?tenant=test",
                "SELECT * FROM information_schema.database_privileges WHERE role_name = 'r1'",
            )
            .unwrap();

        assert_eq!(resp.status(), status_code::OK);
        assert!(resp.text().unwrap().trim().is_empty());

        let resp = client
            .post(
                "http://127.0.0.1:8902/api/v1/sql?tenant=test",
                "GRANT ALL ON DATABASE db1 TO ROLE r1",
            )
            .unwrap();

        assert_eq!(resp.status(), status_code::OK);

        let resp = client
            .post(
                "http://127.0.0.1:8902/api/v1/sql?tenant=test",
                "DROP ROLE r1",
            )
            .unwrap();

        assert_eq!(resp.status(), status_code::OK);

        data.restart_singleton("config_8902");

        let resp = client
            .post(
                "http://127.0.0.1:8902/api/v1/sql?tenant=test",
                "SELECT * FROM information_schema.roles WHERE role_name = 'r1'",
            )
            .unwrap();

        assert_eq!(resp.status(), status_code::OK);
        assert!(resp.text().unwrap().trim().is_empty());

        let resp = client
            .post(
                "http://127.0.0.1:8902/api/v1/sql?tenant=test",
                "CREATE ROLE r1 INHERIT member",
            )
            .unwrap();

        assert_eq!(resp.status(), status_code::OK);

        let resp = client
            .post(
                "http://127.0.0.1:8902/api/v1/sql?tenant=test",
                "SELECT * FROM information_schema.database_privileges WHERE role_name = 'r1'",
            )
            .unwrap();

        assert_eq!(resp.status(), status_code::OK);
        assert!(resp.text().unwrap().trim().is_empty());

        println!("Test complete restart_test_case_5");
    }

    #[test]
    fn case6() {
        println!("Test begin restart_test_case_6");
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .worker_threads(4)
            .build()
            .unwrap();
        let runtime = Arc::new(runtime);

        clean_env();
        let (_meta, mut data) = start_cluster(runtime, 3, 2);

        thread::sleep(Duration::from_secs(30));

        let client = Client::new("root".to_string(), Some(String::new()));
        let resp = client
            .post(
                "http://127.0.0.1:8902/api/v1/sql?db=public",
                "create database db1 with replica 2",
            )
            .unwrap();
        assert_eq!(resp.status(), status_code::OK);

        let resp = client
            .post(
                "http://127.0.0.1:8902/api/v1/sql?db=db1",
                "CREATE TABLE air (visibility DOUBLE,temperature DOUBLE,pressure DOUBLE,TAGS(station))",
            )
            .unwrap();
        assert_eq!(resp.status(), status_code::OK);

        let resp = client
            .post(
                "http://127.0.0.1:8902/api/v1/sql?db=db1",
                "INSERT INTO air (TIME, station, visibility, temperature, pressure) VALUES(1666165200290401000, 'XiaoMaiDao', 56, 69, 77)",
            )
            .unwrap();
        assert_eq!(resp.status(), status_code::OK);

        data.kill_process("config_8912");

        let resp = client
            .post(
                "http://127.0.0.1:8902/api/v1/sql?db=public",
                "drop database db1",
            )
            .unwrap();
        assert_eq!(resp.status(), status_code::UNPROCESSABLE_ENTITY);

        thread::sleep(Duration::from_secs(71));

        let resp = client
            .post(
                "http://127.0.0.1:8902/api/v1/sql?db=public",
                "select name,action,try_count,status from information_schema.resource_status where name = 'cnosdb-db1'",
            ).unwrap();
        assert_eq!(resp.status(), status_code::OK);
        let expect_res =
            Regex::new(r"name,action,try_count,status\ncnosdb-db1,DropDatabase,\d+,Failed\n")
                .unwrap();
        let actual_res = resp.text().unwrap();
        assert!(expect_res.find(&actual_res).is_some());

        data.start_process("config_8912.toml", false);

        let resp = client
            .post(
                "http://127.0.0.1:8902/api/v1/sql?db=public",
                "select name,action,try_count,status from information_schema.resource_status where name = 'cnosdb-db1'",
            ).unwrap();
        assert_eq!(resp.status(), status_code::OK);
        let expect_res =
            Regex::new(r"name,action,try_count,status\ncnosdb-db1,DropDatabase,\d+,Successed\n")
                .unwrap();
        let actual_res = resp.text().unwrap();
        assert!(expect_res.find(&actual_res).is_some());

        println!("Test complete restart_test_case_6");
    }
}
