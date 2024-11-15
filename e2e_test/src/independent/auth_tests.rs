use http_protocol::status_code;

use crate::utils::global::E2eContext;
use crate::utils::Client;
use crate::{check_response, cluster_def};

#[test]
fn test1() {
    let mut ctx = E2eContext::new("auth_tests", "test1");
    let mut executor = ctx.build_executor(cluster_def::one_data(1));
    let host_port = executor.cluster_definition().data_cluster_def[0].http_host_port;
    let api_v1_sql_url = &format!("http://{host_port}/api/v1/sql?db=public");

    executor.startup();

    {
        // Start cnosdb singleton with `auth_enabled = false`, alter password for root.
        let client = executor.case_context().data_client(0);
        check_response!(client.post(api_v1_sql_url, "alter user root set password='abc'",));
        check_response!(client.post(
            api_v1_sql_url,
            "alter user root set must_change_password = false",
        ));
        executor.shutdown();
    }

    // Start cnosdb singleton with `auth_enabled = true`
    executor.set_update_data_config_fn_vec(vec![Some(Box::new(|config| {
        config.query.auth_enabled = true;
    }))]);
    executor.restart(true);

    {
        let client = Client::with_auth("root".to_string(), Some("ab".to_string()));

        let resp = client.post(api_v1_sql_url, "select 1").unwrap();
        assert_eq!(resp.status(), status_code::UNPROCESSABLE_ENTITY);
        assert_eq!(
            resp.text().unwrap(),
            "{\"error_code\":\"010016\",\"error_message\":\"Auth error: Access denied for user 'root' (using xxx) username or password invalid\"}" 
        );
    }
    {
        let client = Client::with_auth("root".to_string(), None);

        let resp = client.post(api_v1_sql_url, "select 1").unwrap();
        assert_eq!(resp.status(), status_code::UNPROCESSABLE_ENTITY);
        assert_eq!(
            resp.text().unwrap(),
            "{\"error_code\":\"010016\",\"error_message\":\"Auth error: Access denied for user 'root' (using xxx) username or password invalid\"}"
         );
    }
    {
        let client = Client::with_auth("root".to_string(), Some("abc".to_owned()));

        let resp = check_response!(client.post(api_v1_sql_url, "select 1"));
        assert_eq!(resp.text().unwrap(), "Int64(1)\n1\n");

        check_response!(client.post(api_v1_sql_url, "create user u1",));
        check_response!(client.post(api_v1_sql_url, "alter tenant cnosdb add user u1 as member",));
    }
    {
        let client = Client::with_auth("u1".to_string(), None);

        let resp = client.post(api_v1_sql_url, "select 1").unwrap();
        assert_eq!(resp.status(), status_code::UNPROCESSABLE_ENTITY);
        assert_eq!(
            resp.text().unwrap(),
            "{\"error_code\":\"010016\",\"error_message\":\"Auth error: Access denied for user 'u1' (using xxx) username or password invalid\"}"
        );
    }
    {
        let client = Client::with_auth("root".to_string(), Some("abc".to_owned()));
        check_response!(client.post(api_v1_sql_url, "alter user u1 set password='abc'"));
    }
    {
        let client = Client::with_auth("u1".to_string(), Some("abc".to_owned()));

        let resp = check_response!(client.post(api_v1_sql_url, "select 1"));
        assert_eq!(resp.text().unwrap(), "Int64(1)\n1\n");
    }
    {
        let client = Client::with_auth("root".to_string(), Some("abc".to_owned()));

        check_response!(client
            .post(api_v1_sql_url, "CREATE USER IF NOT EXISTS test WITH PASSWORD='123456', MUST_CHANGE_PASSWORD=false, COMMENT = 'test';"));

        check_response!(client.post(api_v1_sql_url, "alter user test set granted_admin = true;",));
    }
    {
        let client = Client::with_auth("test".to_string(), Some("123456".to_owned()));
        let resp = check_response!(client.post(api_v1_sql_url, "show databases;"));
        assert_eq!(
            resp.text().unwrap(),
            "database_name\ncluster_schema\npublic\nusage_schema\n"
        )
    }
}

#[test]
fn test2() {
    let mut ctx = E2eContext::new("auth_tests", "test2");
    let mut executor = ctx.build_executor(cluster_def::one_data(1));
    let host_port = executor.cluster_definition().data_cluster_def[0].http_host_port;
    let api_v1_sql_url = &format!("http://{host_port}/api/v1/sql?db=public");
    let api_v1_write_url = &format!("http://{host_port}/api/v1/write?db=public");

    executor.startup();

    {
        // Start cnosdb singleton with `auth_enabled = false`, alter password for root.
        let client = executor.case_context().data_client(0);
        check_response!(client.post(
            api_v1_sql_url,
            "create user test_must with must_change_password = true, password = '123';",
        ));

        check_response!(client.post(
            api_v1_sql_url,
            "alter tenant cnosdb add user test_must as owner;",
        ));

        let client = Client::with_auth("test_must".to_string(), None);
        check_response!(client.post(api_v1_sql_url, "show tables;"));
    }

    // Start cnosdb singleton with `auth_enabled = true`
    executor.set_update_data_config_fn_vec(vec![Some(Box::new(|config| {
        config.query.auth_enabled = true;
    }))]);
    executor.restart(true);

    {
        let client = Client::with_auth("root".to_string(), None);

        let resp = client.post(api_v1_sql_url, "select 1").unwrap();
        assert_eq!(resp.status(), status_code::UNPROCESSABLE_ENTITY);
        assert_eq!(
            resp.text().unwrap(),
            "{\"error_code\":\"010016\",\"error_message\":\"Auth error: Access denied for user 'root' (using xxx) username or password invalid\"}",
        );
    }
    {
        let client = Client::with_auth("root".to_string(), Some("root".to_string()));

        let resp = client.post(api_v1_sql_url, "select 1").unwrap();
        assert_eq!(
            resp.text().unwrap(),
            "{\"error_code\":\"010004\",\"error_message\":\"Insufficient privileges, expected [change password]\"}",
        );
    }

    {
        let client = Client::with_auth("test_must".to_string(), Some("123".to_string()));

        let resp = client.post(
            api_v1_write_url,
            "air,station=XiaoMaiDao visibility=53,temperature=53,pressure=69 1644125400000000000"
        ).unwrap();
        assert_eq!(resp.status(), status_code::UNPROCESSABLE_ENTITY);
        assert_eq!(
            resp.text().unwrap(),
            "{\"error_code\":\"010004\",\"error_message\":\"Insufficient privileges, expected [change password]\"}",
        );

        let resp = client.post(api_v1_sql_url, "select 1;").unwrap();
        assert_eq!(resp.status(), status_code::UNPROCESSABLE_ENTITY);
        assert_eq!(
            resp.text().unwrap(),
            "{\"error_code\":\"010004\",\"error_message\":\"Insufficient privileges, expected [change password]\"}",
        );
    }
    {
        let client = Client::with_auth("test_must".to_string(), Some("123".to_string()));

        let resp = client
            .post(
                api_v1_sql_url,
                "alter user test_must set must_change_password = false;",
            )
            .unwrap();
        assert_eq!(resp.status(), status_code::UNPROCESSABLE_ENTITY);
        assert_eq!(
            resp.text().unwrap(),
            "{\"error_code\":\"010004\",\"error_message\":\"Insufficient privileges, expected [change password]\"}",
        );

        let resp = client
            .post(api_v1_sql_url, "alter user test_must set password = '123';")
            .unwrap();

        assert_eq!(resp.status(), status_code::UNPROCESSABLE_ENTITY);
        assert_eq!(
            resp.text().unwrap(),
            "{\"error_code\":\"010004\",\"error_message\":\"Insufficient privileges, expected [input different password]\"}",
        );

        check_response!(client.post(
            api_v1_sql_url,
            "alter user test_must set password = '1234';",
        ));
    }

    {
        let client = Client::with_auth("test_must".to_string(), Some("1234".to_string()));

        check_response!(client.post(api_v1_sql_url, "show tables;"));

        let resp = client
            .post(
                api_v1_sql_url,
                "alter user test_must set must_change_password = false;",
            )
            .unwrap();
        assert_eq!(resp.status(), status_code::UNPROCESSABLE_ENTITY);
        assert_eq!(
            resp.text().unwrap(),
            "{\"error_code\":\"010004\",\"error_message\":\"Insufficient privileges, expected [maintainer for system]\"}",
        );
    }
}
