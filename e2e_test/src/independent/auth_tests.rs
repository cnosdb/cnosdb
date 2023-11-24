#[cfg(test)]
mod test {
    use std::fs;
    use std::path::PathBuf;

    use http_protocol::status_code;
    use serial_test::serial;

    use crate::utils::{clean_env, modify_config_file, start_singleton, Client};

    #[test]
    #[serial]
    fn test1() {
        println!("Test begin auth_test");
        clean_env();

        let data = start_singleton(None::<PathBuf>, "config_8902.toml", "127.0.0.1:8902");

        let resp = data
            .client
            .post(
                "http://127.0.0.1:8902/api/v1/sql?db=public",
                "alter user root set password='abc'",
            )
            .unwrap();
        assert_eq!(resp.status(), status_code::OK);

        drop(data);

        let crate_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let workspace_dir = crate_dir.parent().unwrap();
        let config_dir = crate_dir.join("tmp_config");
        let config_path = config_dir.join("config_8902.toml");

        let _ = fs::remove_dir_all(&config_dir);

        fs::create_dir(&config_dir).unwrap();

        let default_config_content =
            fs::read_to_string(workspace_dir.join("config").join("config_8902.toml")).unwrap();

        let config_content = modify_config_file(
            &default_config_content,
            "auth_enabled = false",
            "auth_enabled = true",
        );

        fs::write(config_path, config_content).unwrap();

        let _data = start_singleton(Some(&config_dir), "config_8902.toml", "127.0.0.1:8902");

        let client = Client::new("root".to_string(), Some("ab".to_owned()));

        let resp = client
            .post("http://127.0.0.1:8902/api/v1/sql?db=public", "select 1")
            .unwrap();
        assert_eq!(resp.status(), status_code::UNPROCESSABLE_ENTITY);
        assert_eq!(
            resp.text().unwrap(),
            "{\"error_code\":\"010016\",\"error_message\":\"Auth error: Access denied for user 'root' (using password) incorrect password attempt.\"}"
        );

        let client = Client::new("root".to_string(), None);

        let resp = client
            .post("http://127.0.0.1:8902/api/v1/sql?db=public", "select 1")
            .unwrap();
        assert_eq!(resp.status(), status_code::UNPROCESSABLE_ENTITY);
        assert_eq!(
            resp.text().unwrap(),
            "{\"error_code\":\"010016\",\"error_message\":\"Auth error: Access denied for user 'root' (using password) incorrect password attempt.\"}"
        );

        let client = Client::new("root".to_string(), Some("abc".to_owned()));

        let resp = client
            .post("http://127.0.0.1:8902/api/v1/sql?db=public", "select 1")
            .unwrap();
        assert_eq!(resp.status(), status_code::OK);
        assert_eq!(resp.text().unwrap(), "Int64(1)\n1\n");

        let resp = client
            .post(
                "http://127.0.0.1:8902/api/v1/sql?db=public",
                "create user u1",
            )
            .unwrap();
        assert_eq!(resp.status(), status_code::OK);

        let resp = client
            .post(
                "http://127.0.0.1:8902/api/v1/sql?db=public",
                "alter tenant cnosdb add user u1 as member",
            )
            .unwrap();
        assert_eq!(resp.status(), status_code::OK);

        let client = Client::new("u1".to_string(), None);

        let resp = client
            .post("http://127.0.0.1:8902/api/v1/sql?db=public", "select 1")
            .unwrap();
        assert_eq!(resp.status(), status_code::UNPROCESSABLE_ENTITY);
        assert_eq!(
            resp.text().unwrap(),
            "{\"error_code\":\"010016\",\"error_message\":\"Auth error: Password not set\"}"
        );

        let client = Client::new("root".to_string(), Some("abc".to_owned()));

        let resp = client
            .post(
                "http://127.0.0.1:8902/api/v1/sql?db=public",
                "alter user u1 set password='abc'",
            )
            .unwrap();
        assert_eq!(resp.status(), status_code::OK);

        let client = Client::new("u1".to_string(), Some("abc".to_owned()));

        let resp = client
            .post("http://127.0.0.1:8902/api/v1/sql?db=public", "select 1")
            .unwrap();
        assert_eq!(resp.status(), status_code::OK);
        assert_eq!(resp.text().unwrap(), "Int64(1)\n1\n");

        let _ = fs::remove_dir_all(&config_dir);
        clean_env();
    }
}
