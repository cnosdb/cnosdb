#![cfg(test)]

use std::path::Path;

use http_protocol::status_code;
use serial_test::serial;

use crate::utils::{build_data_node_config, kill_all, run_singleton, Client};
use crate::{check_response, cluster_def};

#[test]
#[serial]
fn test1() {
    println!("Test begin auth_test");

    let test_dir = "/tmp/e2e_test/auth_tests/test1";
    let _ = std::fs::remove_dir_all(test_dir);
    std::fs::create_dir_all(test_dir).unwrap();

    kill_all();

    let data_node_def = &cluster_def::one_data(1);

    {
        // Start cnosdb singleton with `auth_enabled = false`, alter password for root.
        let data = run_singleton(test_dir, data_node_def, false, true);
        check_response!(data.client.post(
            "http://127.0.0.1:8902/api/v1/sql?db=public",
            "alter user root set password='abc'",
        ));
        check_response!(data.client.post(
            "http://127.0.0.1:8902/api/v1/sql?db=public",
            "alter user root set must_change_password = false",
        ));
    }

    // Start cnosdb singleton with `auth_enabled = true`
    let mut config = build_data_node_config(test_dir, &data_node_def.config_file_name);
    data_node_def.update_config(&mut config);
    config.query.auth_enabled = true;
    let config_dir = Path::new(test_dir).join("data").join("config");
    std::fs::create_dir_all(&config_dir).unwrap();
    let config_file_path = config_dir.join(&data_node_def.config_file_name);
    std::fs::write(config_file_path, config.to_string_pretty()).unwrap();

    let _data = run_singleton(test_dir, data_node_def, false, false);

    {
        let client = Client::with_auth("root".to_string(), Some("ab".to_string()));

        let resp = client
            .post("http://127.0.0.1:8902/api/v1/sql?db=public", "select 1")
            .unwrap();
        assert_eq!(resp.status(), status_code::UNPROCESSABLE_ENTITY);
        assert_eq!(
            resp.text().unwrap(),
            "{\"error_code\":\"010016\",\"error_message\":\"Auth error: Access denied for user 'root' (using xxx) username or password invalid\"}" 
        );
    }
    {
        let client = Client::with_auth("root".to_string(), None);

        let resp = client
            .post("http://127.0.0.1:8902/api/v1/sql?db=public", "select 1")
            .unwrap();
        assert_eq!(resp.status(), status_code::UNPROCESSABLE_ENTITY);
        assert_eq!(
            resp.text().unwrap(),
            "{\"error_code\":\"010016\",\"error_message\":\"Auth error: Access denied for user 'root' (using xxx) username or password invalid\"}"
         );
    }
    {
        let client = Client::with_auth("root".to_string(), Some("abc".to_owned()));

        let resp =
            check_response!(client.post("http://127.0.0.1:8902/api/v1/sql?db=public", "select 1"));
        assert_eq!(resp.text().unwrap(), "Int64(1)\n1\n");

        check_response!(client.post(
            "http://127.0.0.1:8902/api/v1/sql?db=public",
            "create user u1",
        ));

        check_response!(client.post(
            "http://127.0.0.1:8902/api/v1/sql?db=public",
            "alter tenant cnosdb add user u1 as member",
        ));
    }
    {
        let client = Client::with_auth("u1".to_string(), None);

        let resp = client
            .post("http://127.0.0.1:8902/api/v1/sql?db=public", "select 1")
            .unwrap();
        assert_eq!(resp.status(), status_code::UNPROCESSABLE_ENTITY);
        assert_eq!(
            resp.text().unwrap(),
            "{\"error_code\":\"010016\",\"error_message\":\"Auth error: Access denied for user 'u1' (using xxx) username or password invalid\"}"
        );
    }
    {
        let client = Client::with_auth("root".to_string(), Some("abc".to_owned()));

        let resp = client
            .post(
                "http://127.0.0.1:8902/api/v1/sql?db=public",
                "alter user u1 set password='abc'",
            )
            .unwrap();
        assert_eq!(resp.status(), status_code::OK);
    }
    {
        let client = Client::with_auth("u1".to_string(), Some("abc".to_owned()));

        let resp = client
            .post("http://127.0.0.1:8902/api/v1/sql?db=public", "select 1")
            .unwrap();
        assert_eq!(resp.status(), status_code::OK);
        assert_eq!(resp.text().unwrap(), "Int64(1)\n1\n");
    }
    {
        let client = Client::with_auth("root".to_string(), Some("abc".to_owned()));

        check_response!(client
            .post("http://127.0.0.1:8902/api/v1/sql?db=public", "CREATE USER IF NOT EXISTS test WITH PASSWORD='123456', MUST_CHANGE_PASSWORD=false, COMMENT = 'test';"));

        check_response!(client.post(
            "http://127.0.0.1:8902/api/v1/sql?db=public",
            "alter user test set granted_admin = true;",
        ));
    }
    {
        let client = Client::with_auth("test".to_string(), Some("123456".to_owned()));
        let resp = client
            .post(
                "http://127.0.0.1:8902/api/v1/sql?db=public",
                "show databases;",
            )
            .unwrap();

        assert_eq!(resp.status(), status_code::OK);
        assert_eq!(
            resp.text().unwrap(),
            "database_name\ncluster_schema\npublic\nusage_schema\n"
        )
    }
}
#[test]
#[serial]
fn test2() {
    println!("Test begin auth_test");

    let test_dir = "/tmp/e2e_test/auth_tests/test2";
    let _ = std::fs::remove_dir_all(test_dir);
    std::fs::create_dir_all(test_dir).unwrap();

    kill_all();

    let data_node_def = &cluster_def::one_data(1);

    {
        // Start cnosdb singleton with `auth_enabled = false`, alter password for root.
        let data = run_singleton(test_dir, data_node_def, false, true);
        check_response!(data.client.post(
            "http://127.0.0.1:8902/api/v1/sql?db=public",
            "create user test_must with must_change_password = true, password = '123';",
        ));

        check_response!(data.client.post(
            "http://127.0.0.1:8902/api/v1/sql?db=public",
            "alter tenant cnosdb add user test_must as owner;",
        ));

        let client = Client::with_auth("test_must".to_string(), None);
        check_response!(client.post("http://127.0.0.1:8902/api/v1/sql?db=public", "show tables;"));
    }
    // Start cnosdb singleton with `auth_enabled = true`
    let mut config = build_data_node_config(test_dir, &data_node_def.config_file_name);
    data_node_def.update_config(&mut config);
    config.query.auth_enabled = true;
    let config_dir = Path::new(test_dir).join("data").join("config");
    std::fs::create_dir_all(&config_dir).unwrap();
    let config_file_path = config_dir.join(&data_node_def.config_file_name);
    std::fs::write(config_file_path, config.to_string_pretty()).unwrap();

    let _data = run_singleton(test_dir, data_node_def, false, false);
    {
        let client = Client::with_auth("root".to_string(), None);

        let resp = client
            .post("http://127.0.0.1:8902/api/v1/sql?db=public", "select 1")
            .unwrap();
        assert_eq!(resp.status(), status_code::UNPROCESSABLE_ENTITY);
        assert_eq!(
            resp.text().unwrap(),
            "{\"error_code\":\"010016\",\"error_message\":\"Auth error: Access denied for user 'root' (using xxx) username or password invalid\"}" 
        );
    }
    {
        let client = Client::with_auth("root".to_string(), Some("root".to_string()));

        let resp = client
            .post("http://127.0.0.1:8902/api/v1/sql?db=public", "select 1")
            .unwrap();
        assert_eq!(
            resp.text().unwrap(),
            "{\"error_code\":\"010004\",\"error_message\":\"Insufficient privileges, expected [change password]\"}" 
        );
    }

    {
        let client = Client::with_auth("test_must".to_string(), Some("123".to_string()));

        let resp = client
        .post("http://127.0.0.1:8902/api/v1/write?db=public", "air,station=XiaoMaiDao visibility=53,temperature=53,pressure=69 1644125400000000000")
        .unwrap();
        assert_eq!(resp.status(), status_code::UNPROCESSABLE_ENTITY);
        assert_eq!(
            resp.text().unwrap(),
            "{\"error_code\":\"010004\",\"error_message\":\"Insufficient privileges, expected [change password]\"}" 
        );

        let resp = client
            .post("http://127.0.0.1:8902/api/v1/sql?db=public", "select 1;")
            .unwrap();
        assert_eq!(resp.status(), status_code::UNPROCESSABLE_ENTITY);
        assert_eq!(
            resp.text().unwrap(),
            "{\"error_code\":\"010004\",\"error_message\":\"Insufficient privileges, expected [change password]\"}" 
        );
    }
    {
        let client = Client::with_auth("test_must".to_string(), Some("123".to_string()));

        let resp = client
            .post(
                "http://127.0.0.1:8902/api/v1/sql?db=public",
                "alter user test_must set must_change_password = false;",
            )
            .unwrap();
        assert_eq!(resp.status(), status_code::UNPROCESSABLE_ENTITY);
        assert_eq!(
            resp.text().unwrap(),
            "{\"error_code\":\"010004\",\"error_message\":\"Insufficient privileges, expected [change password]\"}" 
        );

        let resp = client
            .post(
                "http://127.0.0.1:8902/api/v1/sql?db=public",
                "alter user test_must set password = '123';",
            )
            .unwrap();

        assert_eq!(resp.status(), status_code::UNPROCESSABLE_ENTITY);
        assert_eq!(
            resp.text().unwrap(),
            "{\"error_code\":\"010004\",\"error_message\":\"Insufficient privileges, expected [input different password]\"}" 
        );

        check_response!(client.post(
            "http://127.0.0.1:8902/api/v1/sql?db=public",
            "alter user test_must set password = '1234';",
        ));
    }

    {
        let client = Client::with_auth("test_must".to_string(), Some("1234".to_string()));

        check_response!(client.post("http://127.0.0.1:8902/api/v1/sql?db=public", "show tables;"));

        let resp = client
            .post(
                "http://127.0.0.1:8902/api/v1/sql?db=public",
                "alter user test_must set must_change_password = false;",
            )
            .unwrap();
        assert_eq!(resp.status(), status_code::UNPROCESSABLE_ENTITY);
        assert_eq!(
            resp.text().unwrap(),
            "{\"error_code\":\"010004\",\"error_message\":\"Insufficient privileges, expected [maintainer for system]\"}" 
        );
    }
}
