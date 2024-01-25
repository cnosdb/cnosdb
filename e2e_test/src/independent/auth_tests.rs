#![cfg(test)]

use std::path::Path;

use http_protocol::status_code;
use serial_test::serial;

use crate::utils::{build_data_node_config, kill_all, run_singleton, Client};
use crate::{assert_response_is_ok, cluster_def};

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
        let resp = data
            .client
            .post(
                "http://127.0.0.1:8902/api/v1/sql?db=public",
                "alter user root set password='abc'",
            )
            .unwrap();
        assert_response_is_ok!(resp);
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

        let resp = client
            .post("http://127.0.0.1:8902/api/v1/sql?db=public", "select 1")
            .unwrap();
        assert_response_is_ok!(resp);
        assert_eq!(resp.text().unwrap(), "Int64(1)\n1\n");

        let resp = client
            .post(
                "http://127.0.0.1:8902/api/v1/sql?db=public",
                "create user u1",
            )
            .unwrap();
        assert_response_is_ok!(resp);

        let resp = client
            .post(
                "http://127.0.0.1:8902/api/v1/sql?db=public",
                "alter tenant cnosdb add user u1 as member",
            )
            .unwrap();
        assert_response_is_ok!(resp);
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

        let resp = client
            .post("http://127.0.0.1:8902/api/v1/sql?db=public", "CREATE USER IF NOT EXISTS test WITH PASSWORD='123456', MUST_CHANGE_PASSWORD=false, COMMENT = 'test';")
            .unwrap();
        assert_response_is_ok!(resp);

        let resp = client
            .post(
                "http://127.0.0.1:8902/api/v1/sql?db=public",
                "alter user test set granted_admin = true;",
            )
            .unwrap();
        assert_response_is_ok!(resp);
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
            "database_name\npublic\nusage_schema\n"
        )
    }
}
