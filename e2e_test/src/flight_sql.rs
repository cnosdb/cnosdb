#[cfg(test)]
mod test {
    use std::time::Duration;

    use arrow_flight::sql::client::FlightSqlServiceClient;
    use arrow_flight::sql::{CommandGetDbSchemas, CommandGetTables};
    use arrow_flight::utils::flight_data_to_batches;
    use arrow_flight::FlightInfo;
    use datafusion::arrow::error::ArrowError;
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::arrow::util::pretty;
    use datafusion::{arrow, assert_batches_eq};
    use futures::TryStreamExt;
    use tonic::transport::{Channel, Endpoint};

    async fn flight_channel(host: &str, port: u16) -> Result<Channel, ArrowError> {
        let endpoint = Endpoint::new(format!("http://{}:{}", host, port))
            .map_err(|_| ArrowError::IoError("Cannot create endpoint".to_string()))?
            .connect_timeout(Duration::from_secs(20))
            .timeout(Duration::from_secs(20))
            .tcp_nodelay(true) // Disable Nagle's Algorithm since we don't want packets to wait
            .tcp_keepalive(Option::Some(Duration::from_secs(3600)))
            .http2_keep_alive_interval(Duration::from_secs(300))
            .keep_alive_timeout(Duration::from_secs(20))
            .keep_alive_while_idle(true);

        let channel = endpoint
            .connect()
            .await
            .map_err(|e| ArrowError::IoError(format!("Cannot connect to endpoint: {e}")))?;

        Ok(channel)
    }

    async fn fetch_result_and_print(
        flight_info: FlightInfo,
        client: &mut FlightSqlServiceClient,
    ) -> Vec<RecordBatch> {
        let mut batches = vec![];
        for ep in &flight_info.endpoint {
            if let Some(tkt) = &ep.ticket {
                let stream = client.do_get(tkt.clone()).await.unwrap();
                let flight_data = stream.try_collect::<Vec<_>>().await.unwrap();
                batches.extend(flight_data_to_batches(&flight_data).unwrap());
            };
        }

        batches
    }

    async fn authed_client() -> FlightSqlServiceClient {
        let channel = flight_channel("localhost", 8904).await.unwrap();
        let mut client = FlightSqlServiceClient::new(channel);

        // 1. handshake, basic authentication
        let _ = client.handshake("root", "").await.unwrap();

        client
    }

    #[tokio::test]
    async fn test_sql_client_get_catalogs() {
        let mut client = authed_client().await;

        let flight_info = client.get_catalogs().await.unwrap();

        let actual = fetch_result_and_print(flight_info, &mut client).await;

        let actual_str = pretty::pretty_format_batches(&actual).unwrap();

        assert!(format!("{actual_str}").contains("cnosdb"));
    }

    #[tokio::test]
    async fn test_sql_client_get_db_schemas() {
        let mut client = authed_client().await;

        let flight_info = client
            .get_db_schemas(CommandGetDbSchemas {
                catalog: None,
                db_schema_filter_pattern: Some("usage_%".to_string()),
            })
            .await
            .unwrap();

        let expected = vec![
            "+--------------+---------------+",
            "| table_schem  | table_catalog |",
            "+--------------+---------------+",
            "| usage_schema | cnosdb        |",
            "+--------------+---------------+",
        ];
        let actual = fetch_result_and_print(flight_info, &mut client).await;

        assert_batches_eq!(expected, &actual);
    }

    #[tokio::test]
    async fn test_sql_client_get_tables() {
        let mut client = authed_client().await;

        let flight_info = client
            .get_tables(CommandGetTables {
                catalog: None,
                db_schema_filter_pattern: Some("usage_schema".to_string()),
                table_name_filter_pattern: Some("coord_%_in".to_string()),
                table_types: vec!["TABLE".to_string(), "VIEW".to_string()],
                include_schema: false,
            })
            .await
            .unwrap();

        let expected = vec![
            "+-----------+--------------+---------------+------------+",
            "| table_cat | table_schem  | table_name    | table_type |",
            "+-----------+--------------+---------------+------------+",
            "| cnosdb    | usage_schema | coord_data_in | TABLE      |",
            "+-----------+--------------+---------------+------------+",
        ];
        let actual = fetch_result_and_print(flight_info, &mut client).await;

        assert_batches_eq!(expected, &actual);
    }

    #[tokio::test]
    async fn test_sql_client_get_table_types() {
        let mut client = authed_client().await;

        let flight_info = client.get_table_types().await.unwrap();

        let expected = vec![
            "+-----------------+",
            "| table_type      |",
            "+-----------------+",
            "| TABLE           |",
            "| VIEW            |",
            "| LOCAL TEMPORARY |",
            "+-----------------+",
        ];
        let actual = fetch_result_and_print(flight_info, &mut client).await;

        assert_batches_eq!(expected, &actual);
    }

    #[tokio::test]
    async fn test_sql_client_excute_data_type_between() {
        let mut client = authed_client().await;

        // clean env
        let flight_info = client
            .execute("DROP DATABASE IF EXISTS tc_between;".to_string())
            .await
            .unwrap();
        let _actual = fetch_result_and_print(flight_info, &mut client).await;
        // create database
        let flight_info = client
            .execute("CREATE DATABASE tc_between WITH TTL '100000d';".to_string())
            .await
            .unwrap();
        let _actual = fetch_result_and_print(flight_info, &mut client).await;
        // use database tc_between
        client.set_header("db", "tc_between");

        let flight_info = client
            .execute(
                "CREATE TABLE IF NOT EXISTS m2(f0 BIGINT UNSIGNED, f1 BIGINT, TAGS(t0, t1));"
                    .to_string(),
            )
            .await
            .unwrap();
        let _actual = fetch_result_and_print(flight_info, &mut client).await;

        let flight_info = client.execute("INSERT m2(TIME, t0, f0, t1, f1) VALUES(CAST (1672301798050000000 AS TIMESTAMP), 'Ig.UZ', 531136669299148225, 'n꓃DH~B ', 9223372036854775807),(CAST (1672301798060000000 AS TIMESTAMP), '263356943', 1040920791041719924, '', -9223372036854775807),(CAST (1672301798070000000 AS TIMESTAMP), '1040920791041719924', 442061994865016078, 'gc.', 0);".to_string()).await.unwrap();
        let _actual = fetch_result_and_print(flight_info, &mut client).await;
        let flight_info = client.execute(r"INSERT m2(TIME, t0, f0, t1, f1) VALUES(CAST (3031647407609562138 AS TIMESTAMP), 'ᵵh', 4166390262642105876, '7ua', 0.0),(CAST (1079616064603730664 AS TIMESTAMP), '}\', 7806435932478031652, 'qy', 23.456), (CAST (263356943 AS TIMESTAMP), '0.6287658423307444', 5466573340614276155, ',J씟\h', -23.456), (CAST (1742494251700243812 AS TIMESTAMP), '#f^Kr잿z', 196790207, 'aF', 0.123);".to_string()).await.unwrap();
        let _actual = fetch_result_and_print(flight_info, &mut client).await;
        let flight_info = client.execute("INSERT m2(TIME, t0, f0, t1, f1) VALUES(CAST (3584132160280509277 AS TIMESTAMP), '', 4132058214182166915, 'V*1lE/', -0.123);".to_string()).await.unwrap();
        let _actual = fetch_result_and_print(flight_info, &mut client).await;

        let flight_info = client.execute("SELECT m2.f0 FROM m2 WHERE CAST(0 AS STRING) BETWEEN (CAST( starts_with(m2.t0, m2.t1) AS STRING)) AND (m2.t1) order by time desc;".to_string()).await.unwrap();
        let actual = fetch_result_and_print(flight_info, &mut client).await;
        let expected = vec![
            "+---------------------+",
            "| f0                  |",
            "+---------------------+",
            "| 4132058214182166915 |",
            "| 4166390262642105876 |",
            "| 196790207           |",
            "| 442061994865016078  |",
            "| 531136669299148225  |",
            "| 7806435932478031652 |",
            "+---------------------+",
        ];
        assert_batches_eq!(expected, &actual);

        let flight_info = client
            .execute("SELECT * FROM m2 ORDER BY t1;".to_string())
            .await
            .unwrap();
        let actual = fetch_result_and_print(flight_info, &mut client).await;
        let expected = vec![
            "+-------------------------------+---------------------+----------+---------------------+----------------------+",
            "| time                          | t0                  | t1       | f0                  | f1                   |",
            "+-------------------------------+---------------------+----------+---------------------+----------------------+",
            "| 2022-12-29T08:16:38.060       | 263356943           |          | 1040920791041719924 | -9223372036854775807 |",
            r"| 1970-01-01T00:00:00.263356943 | 0.6287658423307444  | ,J씟\h   | 5466573340614276155 | -23                  |",
            "| 2066-01-25T12:16:47.609562138 | ᵵh                  | 7ua      | 4166390262642105876 | 0                    |",
            "| 2083-07-30T00:16:00.280509277 |                     | V*1lE/   | 4132058214182166915 | 0                    |",
            "| 2025-03-20T18:10:51.700243812 | #f^Kr잿z           | aF       | 196790207           | 0                    |",
            "| 2022-12-29T08:16:38.070       | 1040920791041719924 | gc.     | 442061994865016078  | 0                    |",
            "| 2022-12-29T08:16:38.050       | Ig.UZ               | n꓃DH~B  | 531136669299148225  | 9223372036854775807  |",
            r"| 2004-03-18T13:21:04.603730664 | }\                  | qy      | 7806435932478031652 | 23                   |",
            "+-------------------------------+---------------------+----------+---------------------+----------------------+",
        ];
        assert_batches_eq!(expected, &actual);

        let flight_info = client
            .execute("SELECT * FROM m2 WHERE t1 <= '0' order by time desc;".to_string())
            .await
            .unwrap();
        let actual = fetch_result_and_print(flight_info, &mut client).await;
        let expected = vec![
            "+-------------------------------+--------------------+--------+---------------------+----------------------+",
            "| time                          | t0                 | t1     | f0                  | f1                   |",
            "+-------------------------------+--------------------+--------+---------------------+----------------------+",
            "| 2022-12-29T08:16:38.060       | 263356943          |        | 1040920791041719924 | -9223372036854775807 |",
            r"| 1970-01-01T00:00:00.263356943 | 0.6287658423307444 | ,J씟\h | 5466573340614276155 | -23                  |",
            "+-------------------------------+--------------------+--------+---------------------+----------------------+",
        ];
        assert_batches_eq!(expected, &actual);

        let flight_info = client
            .execute("SELECT * FROM m2 WHERE t1 >= '0'  order by time desc;".to_string())
            .await
            .unwrap();
        let actual = fetch_result_and_print(flight_info, &mut client).await;
        let expected = vec![
            "+-------------------------------+---------------------+----------+---------------------+---------------------+",
            "| time                          | t0                  | t1       | f0                  | f1                  |",
            "+-------------------------------+---------------------+----------+---------------------+---------------------+",
            "| 2083-07-30T00:16:00.280509277 |                     | V*1lE/   | 4132058214182166915 | 0                   |",
            "| 2066-01-25T12:16:47.609562138 | ᵵh                  | 7ua      | 4166390262642105876 | 0                   |",
            "| 2025-03-20T18:10:51.700243812 | #f^Kr잿z           | aF       | 196790207           | 0                   |",
            "| 2022-12-29T08:16:38.070       | 1040920791041719924 | gc.     | 442061994865016078  | 0                   |",
            "| 2022-12-29T08:16:38.050       | Ig.UZ               | n꓃DH~B  | 531136669299148225  | 9223372036854775807 |",
            r"| 2004-03-18T13:21:04.603730664 | }\                  | qy      | 7806435932478031652 | 23                  |",
            "+-------------------------------+---------------------+----------+---------------------+---------------------+",
        ];
        assert_batches_eq!(expected, &actual);

        let flight_info = client
            .execute("SELECT * FROM m2 WHERE t1 <= '8' order by time desc;".to_string())
            .await
            .unwrap();
        let actual = fetch_result_and_print(flight_info, &mut client).await;
        let expected = vec![
            "+-------------------------------+--------------------+--------+---------------------+----------------------+",
            "| time                          | t0                 | t1     | f0                  | f1                   |",
            "+-------------------------------+--------------------+--------+---------------------+----------------------+",
            "| 2066-01-25T12:16:47.609562138 | ᵵh                 | 7ua    | 4166390262642105876 | 0                    |",
            "| 2022-12-29T08:16:38.060       | 263356943          |        | 1040920791041719924 | -9223372036854775807 |",
            r"| 1970-01-01T00:00:00.263356943 | 0.6287658423307444 | ,J씟\h | 5466573340614276155 | -23                  |",
            "+-------------------------------+--------------------+--------+---------------------+----------------------+",
        ];
        assert_batches_eq!(expected, &actual);

        let flight_info = client
            .execute(
                "SELECT * FROM m2 WHERE t1 BETWEEN '7ua' AND 'aF'  order by time desc;".to_string(),
            )
            .await
            .unwrap();
        let actual = fetch_result_and_print(flight_info, &mut client).await;
        let expected = vec![
            "+-------------------------------+-----------+--------+---------------------+----+",
            "| time                          | t0        | t1     | f0                  | f1 |",
            "+-------------------------------+-----------+--------+---------------------+----+",
            "| 2083-07-30T00:16:00.280509277 |           | V*1lE/ | 4132058214182166915 | 0  |",
            "| 2066-01-25T12:16:47.609562138 | ᵵh        | 7ua    | 4166390262642105876 | 0  |",
            "| 2025-03-20T18:10:51.700243812 | #f^Kr잿z | aF     | 196790207           | 0  |",
            "+-------------------------------+-----------+--------+---------------------+----+",
        ];
        assert_batches_eq!(expected, &actual);

        let flight_info = client
            .execute("SELECT * FROM m2 WHERE t1 <= 'V*1lE/' order by time desc;".to_string())
            .await
            .unwrap();
        let actual = fetch_result_and_print(flight_info, &mut client).await;
        let expected = vec![
            "+-------------------------------+--------------------+--------+---------------------+----------------------+",
            "| time                          | t0                 | t1     | f0                  | f1                   |",
            "+-------------------------------+--------------------+--------+---------------------+----------------------+",
            "| 2083-07-30T00:16:00.280509277 |                    | V*1lE/ | 4132058214182166915 | 0                    |",
            "| 2066-01-25T12:16:47.609562138 | ᵵh                 | 7ua    | 4166390262642105876 | 0                    |",
            "| 2022-12-29T08:16:38.060       | 263356943          |        | 1040920791041719924 | -9223372036854775807 |",
            r"| 1970-01-01T00:00:00.263356943 | 0.6287658423307444 | ,J씟\h | 5466573340614276155 | -23                  |",
            "+-------------------------------+--------------------+--------+---------------------+----------------------+",
        ];
        assert_batches_eq!(expected, &actual);

        let flight_info = client
            .execute("SELECT * FROM m2 WHERE t1 >= 'gc.' order by time desc;".to_string())
            .await
            .unwrap();
        let actual = fetch_result_and_print(flight_info, &mut client).await;
        let expected = vec![
            "+-------------------------------+---------------------+----------+---------------------+---------------------+",
            "| time                          | t0                  | t1       | f0                  | f1                  |",
            "+-------------------------------+---------------------+----------+---------------------+---------------------+",
            "| 2022-12-29T08:16:38.070       | 1040920791041719924 | gc.     | 442061994865016078  | 0                   |",
            "| 2022-12-29T08:16:38.050       | Ig.UZ               | n꓃DH~B  | 531136669299148225  | 9223372036854775807 |",
            r"| 2004-03-18T13:21:04.603730664 | }\                  | qy      | 7806435932478031652 | 23                  |",
            "+-------------------------------+---------------------+----------+---------------------+---------------------+",
        ];
        assert_batches_eq!(expected, &actual);

        let flight_info = client
            .execute("SELECT * FROM m2 WHERE f1 > -0.123 order by time desc;".to_string())
            .await
            .unwrap();
        let actual = fetch_result_and_print(flight_info, &mut client).await;
        let expected = vec![
            "+-------------------------------+---------------------+----------+---------------------+---------------------+",
            "| time                          | t0                  | t1       | f0                  | f1                  |",
            "+-------------------------------+---------------------+----------+---------------------+---------------------+",
            "| 2083-07-30T00:16:00.280509277 |                     | V*1lE/   | 4132058214182166915 | 0                   |",
            "| 2066-01-25T12:16:47.609562138 | ᵵh                  | 7ua      | 4166390262642105876 | 0                   |",
            "| 2025-03-20T18:10:51.700243812 | #f^Kr잿z           | aF       | 196790207           | 0                   |",
            "| 2022-12-29T08:16:38.070       | 1040920791041719924 | gc.     | 442061994865016078  | 0                   |",
            "| 2022-12-29T08:16:38.050       | Ig.UZ               | n꓃DH~B  | 531136669299148225  | 9223372036854775807 |",
            r"| 2004-03-18T13:21:04.603730664 | }\                  | qy      | 7806435932478031652 | 23                  |",
            "+-------------------------------+---------------------+----------+---------------------+---------------------+",
        ];
        assert_batches_eq!(expected, &actual);

        let flight_info = client.execute("drop table m2".to_string()).await.unwrap();
        let _actual = fetch_result_and_print(flight_info, &mut client).await;
        let flight_info = client
            .execute("drop database tc_between".to_string())
            .await
            .unwrap();
        let _actual = fetch_result_and_print(flight_info, &mut client).await;
    }

    #[tokio::test]
    async fn test_sql_client_alter_user() {
        let mut client = authed_client().await;

        // clean env
        let flight_info = client
            .execute("drop user if exists test_au_u1;".to_string())
            .await
            .unwrap();
        let _actual = fetch_result_and_print(flight_info, &mut client).await;
        let flight_info = client
            .execute("drop user if exists test_au_u2;".to_string())
            .await
            .unwrap();
        let _actual = fetch_result_and_print(flight_info, &mut client).await;

        // create user
        let flight_info = client
            .execute("create user if not exists test_au_u1;".to_string())
            .await
            .unwrap();
        let _actual = fetch_result_and_print(flight_info, &mut client).await;

        let flight_info = client
            .execute("create user if not exists test_au_u2;".to_string())
            .await
            .unwrap();
        let _actual = fetch_result_and_print(flight_info, &mut client).await;

        // add user to cnosdb tenant
        let flight_info = client
            .execute("alter tenant cnosdb add user test_au_u1 as member;".to_string())
            .await
            .unwrap();
        let _actual = fetch_result_and_print(flight_info, &mut client).await;

        let flight_info = client
            .execute("alter tenant cnosdb add user test_au_u2 as member;".to_string())
            .await
            .unwrap();
        let _actual = fetch_result_and_print(flight_info, &mut client).await;

        // check user info
        let flight_info = client.execute("select * from cluster_schema.users where user_name in ('root', 'test_au_u1', 'test_au_u2') order by user_name;".to_string()).await.unwrap();
        let actual = fetch_result_and_print(flight_info, &mut client).await;
        let expected = vec![
            "+------------+----------+-------------------------------------------------------------------------------------------------+",
            "| user_name  | is_admin | user_options                                                                                    |",
            "+------------+----------+-------------------------------------------------------------------------------------------------+",
            "| root       | true     | {\"password\":\"*****\",\"must_change_password\":true,\"comment\":\"system admin\",\"granted_admin\":false} |",
            "| test_au_u1 | false    | {\"password\":\"*****\",\"must_change_password\":false,\"granted_admin\":false}                         |",
            "| test_au_u2 | false    | {\"password\":\"*****\",\"must_change_password\":false,\"granted_admin\":false}                         |",
            "+------------+----------+-------------------------------------------------------------------------------------------------+",
        ];
        assert_batches_eq!(expected, &actual);

        let flight_info = client
            .execute("alter user test_au_u1 set granted_admin = true;".to_string())
            .await
            .unwrap();
        let _actual = fetch_result_and_print(flight_info, &mut client).await;

        let flight_info = client.execute("select * from cluster_schema.users where user_name in ('root', 'test_au_u1', 'test_au_u2') order by user_name;".to_string()).await.unwrap();
        let actual = fetch_result_and_print(flight_info, &mut client).await;
        let expected = vec![
            "+------------+----------+-------------------------------------------------------------------------------------------------+",
            "| user_name  | is_admin | user_options                                                                                    |",
            "+------------+----------+-------------------------------------------------------------------------------------------------+",
            "| root       | true     | {\"password\":\"*****\",\"must_change_password\":true,\"comment\":\"system admin\",\"granted_admin\":false} |",
            "| test_au_u1 | true     | {\"password\":\"*****\",\"must_change_password\":false,\"granted_admin\":true}                          |",
            "| test_au_u2 | false    | {\"password\":\"*****\",\"must_change_password\":false,\"granted_admin\":false}                         |",
            "+------------+----------+-------------------------------------------------------------------------------------------------+",
        ];
        assert_batches_eq!(expected, &actual);

        let flight_info = client
            .execute("alter user test_au_u2 set granted_admin = true;".to_string())
            .await
            .unwrap();
        let _actual = fetch_result_and_print(flight_info, &mut client).await;

        let flight_info = client.execute("select * from cluster_schema.users where user_name in ('root', 'test_au_u1', 'test_au_u2') order by user_name;".to_string()).await.unwrap();
        let actual = fetch_result_and_print(flight_info, &mut client).await;
        let expected = vec![
            "+------------+----------+-------------------------------------------------------------------------------------------------+",
            "| user_name  | is_admin | user_options                                                                                    |",
            "+------------+----------+-------------------------------------------------------------------------------------------------+",
            "| root       | true     | {\"password\":\"*****\",\"must_change_password\":true,\"comment\":\"system admin\",\"granted_admin\":false} |",
            "| test_au_u1 | true     | {\"password\":\"*****\",\"must_change_password\":false,\"granted_admin\":true}                          |",
            "| test_au_u2 | true     | {\"password\":\"*****\",\"must_change_password\":false,\"granted_admin\":true}                          |",
            "+------------+----------+-------------------------------------------------------------------------------------------------+",
        ];
        assert_batches_eq!(expected, &actual);

        let flight_info = client
            .execute("alter user test_au_u1 set granted_admin = false;".to_string())
            .await
            .unwrap();
        let _actual = fetch_result_and_print(flight_info, &mut client).await;
        let flight_info = client
            .execute("alter user test_au_u2 set granted_admin = false;".to_string())
            .await
            .unwrap();
        let _actual = fetch_result_and_print(flight_info, &mut client).await;

        let flight_info = client.execute("select * from cluster_schema.users where user_name in ('root', 'test_au_u1', 'test_au_u2') order by user_name;".to_string()).await.unwrap();
        let actual = fetch_result_and_print(flight_info, &mut client).await;
        let expected = vec![
            "+------------+----------+-------------------------------------------------------------------------------------------------+",
            "| user_name  | is_admin | user_options                                                                                    |",
            "+------------+----------+-------------------------------------------------------------------------------------------------+",
            "| root       | true     | {\"password\":\"*****\",\"must_change_password\":true,\"comment\":\"system admin\",\"granted_admin\":false} |",
            "| test_au_u1 | false    | {\"password\":\"*****\",\"must_change_password\":false,\"granted_admin\":false}                         |",
            "| test_au_u2 | false    | {\"password\":\"*****\",\"must_change_password\":false,\"granted_admin\":false}                         |",
            "+------------+----------+-------------------------------------------------------------------------------------------------+",
        ];
        assert_batches_eq!(expected, &actual);

        // clean env
        let flight_info = client
            .execute("drop user if exists test_au_u1;".to_string())
            .await
            .unwrap();
        let _actual = fetch_result_and_print(flight_info, &mut client).await;
        let flight_info = client
            .execute("drop user if exists test_au_u2;".to_string())
            .await
            .unwrap();
        let _actual = fetch_result_and_print(flight_info, &mut client).await;
    }

    #[tokio::test]
    async fn test_sql_client_alter_database() {
        // create flight client
        let mut client = authed_client().await;

        // clean env
        let flight_info = client
            .execute("DROP DATABASE IF EXISTS alter_database;".to_string())
            .await
            .unwrap();
        let _actual = fetch_result_and_print(flight_info, &mut client).await;

        let flight_info = client.execute("CREATE DATABASE alter_database WITH TTl '10d' SHARD 5 VNOdE_DURATiON '3d' REPLICA 1 pRECISIOn 'us';".to_string()).await.unwrap();
        let _actual = fetch_result_and_print(flight_info, &mut client).await;

        let flight_info = client
            .execute("DESCRIBE DATABASE alter_database;".to_string())
            .await
            .unwrap();
        let actual = fetch_result_and_print(flight_info, &mut client).await;
        let exspected = vec![
            "+---------+-------+----------------+---------+-----------+",
            "| TTL     | SHARD | VNODE_DURATION | REPLICA | PRECISION |",
            "+---------+-------+----------------+---------+-----------+",
            "| 10 Days | 5     | 3 Days         | 1       | US        |",
            "+---------+-------+----------------+---------+-----------+",
        ];
        assert_batches_eq!(exspected, &actual);

        let flight_info = client
            .execute("ALTER DATABASE alter_database Set TTL '30d';".to_string())
            .await
            .unwrap();
        let _actual = fetch_result_and_print(flight_info, &mut client).await;

        let flight_info = client
            .execute("DESCRIBE DATABASE alter_database;".to_string())
            .await
            .unwrap();
        let actual = fetch_result_and_print(flight_info, &mut client).await;
        let exspected = vec![
            "+---------+-------+----------------+---------+-----------+",
            "| TTL     | SHARD | VNODE_DURATION | REPLICA | PRECISION |",
            "+---------+-------+----------------+---------+-----------+",
            "| 30 Days | 5     | 3 Days         | 1       | US        |",
            "+---------+-------+----------------+---------+-----------+",
        ];
        assert_batches_eq!(exspected, &actual);

        // set shard 6
        let flight_info = client
            .execute("ALTER DATABASE alter_database Set SHARD 6;".to_string())
            .await
            .unwrap();
        let _actual = fetch_result_and_print(flight_info, &mut client).await;

        let flight_info = client
            .execute("DESCRIBE DATABASE alter_database;".to_string())
            .await
            .unwrap();
        let actual = fetch_result_and_print(flight_info, &mut client).await;
        let exspected = vec![
            "+---------+-------+----------------+---------+-----------+",
            "| TTL     | SHARD | VNODE_DURATION | REPLICA | PRECISION |",
            "+---------+-------+----------------+---------+-----------+",
            "| 30 Days | 6     | 3 Days         | 1       | US        |",
            "+---------+-------+----------------+---------+-----------+",
        ];
        assert_batches_eq!(exspected, &actual);

        // set vnode_duration 100d
        let flight_info = client
            .execute("ALTER DATABASE alter_database Set VNODE_DURATION '100d';".to_string())
            .await
            .unwrap();
        let _actual = fetch_result_and_print(flight_info, &mut client).await;

        let flight_info = client
            .execute("DESCRIBE DATABASE alter_database;".to_string())
            .await
            .unwrap();
        let actual = fetch_result_and_print(flight_info, &mut client).await;
        let exspected = vec![
            "+---------+-------+----------------+---------+-----------+",
            "| TTL     | SHARD | VNODE_DURATION | REPLICA | PRECISION |",
            "+---------+-------+----------------+---------+-----------+",
            "| 30 Days | 6     | 100 Days       | 1       | US        |",
            "+---------+-------+----------------+---------+-----------+",
        ];
        assert_batches_eq!(exspected, &actual);

        let flight_info = client
            .execute("ALTER DATABASE alter_database Set REPLICA 1;".to_string())
            .await
            .unwrap();
        let _actual = fetch_result_and_print(flight_info, &mut client).await;

        let flight_info = client
            .execute("DESCRIBE DATABASE alter_database;".to_string())
            .await
            .unwrap();
        let actual = fetch_result_and_print(flight_info, &mut client).await;
        let exspected = vec![
            "+---------+-------+----------------+---------+-----------+",
            "| TTL     | SHARD | VNODE_DURATION | REPLICA | PRECISION |",
            "+---------+-------+----------------+---------+-----------+",
            "| 30 Days | 6     | 100 Days       | 1       | US        |",
            "+---------+-------+----------------+---------+-----------+",
        ];
        assert_batches_eq!(exspected, &actual);

        // clean env
        let flight_info = client
            .execute("DROP DATABASE IF EXISTS alter_database;".to_string())
            .await
            .unwrap();
        let _actual = fetch_result_and_print(flight_info, &mut client).await;
    }

    #[tokio::test]
    async fn test_sql_client_empty_table() {
        // create flight client
        let mut client = authed_client().await;

        // clean env
        let flight_info = client
            .execute("DROP DATABASE IF EXISTS empty_table;".to_string())
            .await
            .unwrap();
        let _actual = fetch_result_and_print(flight_info, &mut client).await;

        let flight_info = client
            .execute("CREATE DATABASE empty_table;".to_string())
            .await
            .unwrap();
        let _actual = fetch_result_and_print(flight_info, &mut client).await;

        let flight_info = client
            .execute("CREATE TABLE empty_table.empty (f DOUBLE, TAGS(t));".to_string())
            .await
            .unwrap();
        let _actual = fetch_result_and_print(flight_info, &mut client).await;

        let flight_info = client
            .execute("SELECT * FROM empty_table.empty;".to_string())
            .await
            .unwrap();
        let actual = fetch_result_and_print(flight_info, &mut client).await;
        let exspected = vec!["++", "++"];
        assert_batches_eq!(exspected, &actual);

        // clean env
        let flight_info = client
            .execute("DROP DATABASE IF EXISTS empty_table;".to_string())
            .await
            .unwrap();
        let _actual = fetch_result_and_print(flight_info, &mut client).await;
    }

    #[tokio::test]
    async fn test_sql_client_filter_push_down() {
        // create flight client
        let mut client = authed_client().await;

        // clean env
        let flight_info = client
            .execute("DROP DATABASE IF EXISTS filter_push_down;".to_string())
            .await
            .unwrap();
        let _actual = fetch_result_and_print(flight_info, &mut client).await;

        let flight_info = client
            .execute("CREATE DATABASE filter_push_down WITH TTL '100000d';".to_string())
            .await
            .unwrap();
        let _actual = fetch_result_and_print(flight_info, &mut client).await;

        client.set_header("db", "filter_push_down");

        let flight_info = client
            .execute(
                "CREATE TABLE m0(
            f0 BIGINT CODEC(DELTA),
            f1 STRING CODEC(GZIP),
            f2 BIGINT UNSIGNED CODEC(NULL),
            f3 BOOLEAN,
            f4 DOUBLE CODEC(GORILLA),
            TAGS(t0, t1));"
                    .to_string(),
            )
            .await
            .unwrap();
        let _actual = fetch_result_and_print(flight_info, &mut client).await;

        let flight_info = client
            .execute(
                "INSERT m0(TIME, f2) VALUES(5867172425191822176, 888), (3986678807649375642, 999);"
                    .to_string(),
            )
            .await
            .unwrap();
        let _actual = fetch_result_and_print(flight_info, &mut client).await;
        let flight_info = client
            .execute("INSERT m0(TIME, f3) VALUES(7488251815539246350, FALSE);".to_string())
            .await
            .unwrap();
        let _actual = fetch_result_and_print(flight_info, &mut client).await;
        let flight_info = client
            .execute("INSERT m0(TIME, f4) VALUES(5414775681413349294, 1.111);".to_string())
            .await
            .unwrap();
        let _actual = fetch_result_and_print(flight_info, &mut client).await;
        let flight_info = client
            .execute(
                "INSERT m0(TIME, t0, t1, f0, f1, f2, f3, f4)
        VALUES
            (1, 'a', 'b', 11, '11', 11, true, 11.11),
            (2, 'a', 'c', 12, '11', 11, false, 11.11),
            (3, 'b', 'b', 13, '11', 11, false, 11.11),
            (4, 'b', 'a', 14, '11', 11, true, 11.11),
            (5, 'a', 'a', 11, '11', 11, true, 11.11),
            (6, 'b', 'c', 15, '11', 11, false, 11.11);"
                    .to_string(),
            )
            .await
            .unwrap();
        let _actual = fetch_result_and_print(flight_info, &mut client).await;

        let flight_info = client
            .execute("select * from m0 order by time, t0, t1, f0;".to_string())
            .await
            .unwrap();
        let actual = fetch_result_and_print(flight_info, &mut client).await;
        let exspected = vec![
            "+-------------------------------+----+----+----+----+-----+-------+-------+",
            "| time                          | t0 | t1 | f0 | f1 | f2  | f3    | f4    |",
            "+-------------------------------+----+----+----+----+-----+-------+-------+",
            "| 1970-01-01T00:00:00.000000001 | a  | b  | 11 | 11 | 11  | true  | 11.11 |",
            "| 1970-01-01T00:00:00.000000002 | a  | c  | 12 | 11 | 11  | false | 11.11 |",
            "| 1970-01-01T00:00:00.000000003 | b  | b  | 13 | 11 | 11  | false | 11.11 |",
            "| 1970-01-01T00:00:00.000000004 | b  | a  | 14 | 11 | 11  | true  | 11.11 |",
            "| 1970-01-01T00:00:00.000000005 | a  | a  | 11 | 11 | 11  | true  | 11.11 |",
            "| 1970-01-01T00:00:00.000000006 | b  | c  | 15 | 11 | 11  | false | 11.11 |",
            "| 2096-05-01T02:46:47.649375642 |    |    |    |    | 999 |       |       |",
            "| 2141-08-03T00:21:21.413349294 |    |    |    |    |     |       | 1.111 |",
            "| 2155-12-04T02:07:05.191822176 |    |    |    |    | 888 |       |       |",
            "| 2207-04-18T13:56:55.539246350 |    |    |    |    |     | false |       |",
            "+-------------------------------+----+----+----+----+-----+-------+-------+",
        ];
        assert_batches_eq!(exspected, &actual);

        let flight_info = client
            .execute(
                "SELECT ALL * FROM m0 AS M0 WHERE NOT ((('TOk')=(m0.t0)))
        UNION ALL
        SELECT ALL * FROM m0 AS M0  WHERE NOT (NOT ((('TOk')=(m0.t0))))
        UNION ALL
        SELECT ALL * FROM m0 AS M0  WHERE (NOT ((('TOk')=(m0.t0)))) IS NULL order by time desc;"
                    .to_string(),
            )
            .await
            .unwrap();
        let actual = fetch_result_and_print(flight_info, &mut client).await;
        let exspected = vec![
            "+-------------------------------+----+----+----+----+-----+-------+-------+",
            "| time                          | t0 | t1 | f0 | f1 | f2  | f3    | f4    |",
            "+-------------------------------+----+----+----+----+-----+-------+-------+",
            "| 2207-04-18T13:56:55.539246350 |    |    |    |    |     | false |       |",
            "| 2155-12-04T02:07:05.191822176 |    |    |    |    | 888 |       |       |",
            "| 2141-08-03T00:21:21.413349294 |    |    |    |    |     |       | 1.111 |",
            "| 2096-05-01T02:46:47.649375642 |    |    |    |    | 999 |       |       |",
            "| 1970-01-01T00:00:00.000000006 | b  | c  | 15 | 11 | 11  | false | 11.11 |",
            "| 1970-01-01T00:00:00.000000005 | a  | a  | 11 | 11 | 11  | true  | 11.11 |",
            "| 1970-01-01T00:00:00.000000004 | b  | a  | 14 | 11 | 11  | true  | 11.11 |",
            "| 1970-01-01T00:00:00.000000003 | b  | b  | 13 | 11 | 11  | false | 11.11 |",
            "| 1970-01-01T00:00:00.000000002 | a  | c  | 12 | 11 | 11  | false | 11.11 |",
            "| 1970-01-01T00:00:00.000000001 | a  | b  | 11 | 11 | 11  | true  | 11.11 |",
            "+-------------------------------+----+----+----+----+-----+-------+-------+",
        ];
        assert_batches_eq!(exspected, &actual);

        let flight_info = client
            .execute("select * from m0 where time = 0;".to_string())
            .await
            .unwrap();
        let actual = fetch_result_and_print(flight_info, &mut client).await;
        let exspected = vec![
            "+------+----+----+----+----+----+----+----+",
            "| time | t0 | t1 | f0 | f1 | f2 | f3 | f4 |",
            "+------+----+----+----+----+----+----+----+",
            "+------+----+----+----+----+----+----+----+",
        ];
        assert_batches_eq!(exspected, &actual);

        let flight_info = client
            .execute("select * from m0 where time > 3 order by time desc;".to_string())
            .await
            .unwrap();
        let actual = fetch_result_and_print(flight_info, &mut client).await;
        let exspected = vec![
            "+-------------------------------+----+----+----+----+-----+-------+-------+",
            "| time                          | t0 | t1 | f0 | f1 | f2  | f3    | f4    |",
            "+-------------------------------+----+----+----+----+-----+-------+-------+",
            "| 2207-04-18T13:56:55.539246350 |    |    |    |    |     | false |       |",
            "| 2155-12-04T02:07:05.191822176 |    |    |    |    | 888 |       |       |",
            "| 2141-08-03T00:21:21.413349294 |    |    |    |    |     |       | 1.111 |",
            "| 2096-05-01T02:46:47.649375642 |    |    |    |    | 999 |       |       |",
            "| 1970-01-01T00:00:00.000000006 | b  | c  | 15 | 11 | 11  | false | 11.11 |",
            "| 1970-01-01T00:00:00.000000005 | a  | a  | 11 | 11 | 11  | true  | 11.11 |",
            "| 1970-01-01T00:00:00.000000004 | b  | a  | 14 | 11 | 11  | true  | 11.11 |",
            "+-------------------------------+----+----+----+----+-----+-------+-------+",
        ];
        assert_batches_eq!(exspected, &actual);

        let flight_info = client
            .execute("select * from m0 where t0 = 'xx' order by time desc;".to_string())
            .await
            .unwrap();
        let actual = fetch_result_and_print(flight_info, &mut client).await;
        let exspected = vec!["++", "++"];
        assert_batches_eq!(exspected, &actual);

        let flight_info = client
            .execute("select * from m0 where t0 = 'a' order by time desc;".to_string())
            .await
            .unwrap();
        let actual = fetch_result_and_print(flight_info, &mut client).await;
        let exspected = vec![
            "+-------------------------------+----+----+----+----+----+-------+-------+",
            "| time                          | t0 | t1 | f0 | f1 | f2 | f3    | f4    |",
            "+-------------------------------+----+----+----+----+----+-------+-------+",
            "| 1970-01-01T00:00:00.000000005 | a  | a  | 11 | 11 | 11 | true  | 11.11 |",
            "| 1970-01-01T00:00:00.000000002 | a  | c  | 12 | 11 | 11 | false | 11.11 |",
            "| 1970-01-01T00:00:00.000000001 | a  | b  | 11 | 11 | 11 | true  | 11.11 |",
            "+-------------------------------+----+----+----+----+----+-------+-------+",
        ];
        assert_batches_eq!(exspected, &actual);

        let flight_info = client
            .execute(
                "select * from m0 
        where t0 = 'a' and t1 = 'b';"
                    .to_string(),
            )
            .await
            .unwrap();
        let actual = fetch_result_and_print(flight_info, &mut client).await;
        let exspected = vec![
            "+-------------------------------+----+----+----+----+----+------+-------+",
            "| time                          | t0 | t1 | f0 | f1 | f2 | f3   | f4    |",
            "+-------------------------------+----+----+----+----+----+------+-------+",
            "| 1970-01-01T00:00:00.000000001 | a  | b  | 11 | 11 | 11 | true | 11.11 |",
            "+-------------------------------+----+----+----+----+----+------+-------+",
        ];
        assert_batches_eq!(exspected, &actual);

        let flight_info = client
            .execute("select * from m0 where t0 = 'a' or t1 = 'b' order by time desc;".to_string())
            .await
            .unwrap();
        let actual = fetch_result_and_print(flight_info, &mut client).await;
        let exspected = vec![
            "+-------------------------------+----+----+----+----+----+-------+-------+",
            "| time                          | t0 | t1 | f0 | f1 | f2 | f3    | f4    |",
            "+-------------------------------+----+----+----+----+----+-------+-------+",
            "| 1970-01-01T00:00:00.000000005 | a  | a  | 11 | 11 | 11 | true  | 11.11 |",
            "| 1970-01-01T00:00:00.000000003 | b  | b  | 13 | 11 | 11 | false | 11.11 |",
            "| 1970-01-01T00:00:00.000000002 | a  | c  | 12 | 11 | 11 | false | 11.11 |",
            "| 1970-01-01T00:00:00.000000001 | a  | b  | 11 | 11 | 11 | true  | 11.11 |",
            "+-------------------------------+----+----+----+----+----+-------+-------+",
        ];
        assert_batches_eq!(exspected, &actual);

        let flight_info = client
            .execute("select * from m0 where t0 = 'a' and f0 = 11 order by time desc;".to_string())
            .await
            .unwrap();
        let actual = fetch_result_and_print(flight_info, &mut client).await;
        let exspected = vec![
            "+-------------------------------+----+----+----+----+----+------+-------+",
            "| time                          | t0 | t1 | f0 | f1 | f2 | f3   | f4    |",
            "+-------------------------------+----+----+----+----+----+------+-------+",
            "| 1970-01-01T00:00:00.000000005 | a  | a  | 11 | 11 | 11 | true | 11.11 |",
            "| 1970-01-01T00:00:00.000000001 | a  | b  | 11 | 11 | 11 | true | 11.11 |",
            "+-------------------------------+----+----+----+----+----+------+-------+",
        ];
        assert_batches_eq!(exspected, &actual);

        let flight_info = client
            .execute("select * from m0 where t0 = 'a' and f0 > 12;".to_string())
            .await
            .unwrap();
        let actual = fetch_result_and_print(flight_info, &mut client).await;
        let exspected = vec!["++", "++"];
        assert_batches_eq!(exspected, &actual);

        let flight_info = client
            .execute("select * from m0  where t0 = 'a' or f0 = 11 order by time desc;".to_string())
            .await
            .unwrap();
        let actual = fetch_result_and_print(flight_info, &mut client).await;
        let exspected = vec![
            "+-------------------------------+----+----+----+----+----+-------+-------+",
            "| time                          | t0 | t1 | f0 | f1 | f2 | f3    | f4    |",
            "+-------------------------------+----+----+----+----+----+-------+-------+",
            "| 1970-01-01T00:00:00.000000005 | a  | a  | 11 | 11 | 11 | true  | 11.11 |",
            "| 1970-01-01T00:00:00.000000002 | a  | c  | 12 | 11 | 11 | false | 11.11 |",
            "| 1970-01-01T00:00:00.000000001 | a  | b  | 11 | 11 | 11 | true  | 11.11 |",
            "+-------------------------------+----+----+----+----+----+-------+-------+",
        ];
        assert_batches_eq!(exspected, &actual);

        let flight_info = client
            .execute("select * from m0 where t0 = 'a' or f0 > 12 order by time desc;".to_string())
            .await
            .unwrap();
        let actual = fetch_result_and_print(flight_info, &mut client).await;
        let exspected = vec![
            "+-------------------------------+----+----+----+----+----+-------+-------+",
            "| time                          | t0 | t1 | f0 | f1 | f2 | f3    | f4    |",
            "+-------------------------------+----+----+----+----+----+-------+-------+",
            "| 1970-01-01T00:00:00.000000006 | b  | c  | 15 | 11 | 11 | false | 11.11 |",
            "| 1970-01-01T00:00:00.000000005 | a  | a  | 11 | 11 | 11 | true  | 11.11 |",
            "| 1970-01-01T00:00:00.000000004 | b  | a  | 14 | 11 | 11 | true  | 11.11 |",
            "| 1970-01-01T00:00:00.000000003 | b  | b  | 13 | 11 | 11 | false | 11.11 |",
            "| 1970-01-01T00:00:00.000000002 | a  | c  | 12 | 11 | 11 | false | 11.11 |",
            "| 1970-01-01T00:00:00.000000001 | a  | b  | 11 | 11 | 11 | true  | 11.11 |",
            "+-------------------------------+----+----+----+----+----+-------+-------+",
        ];
        assert_batches_eq!(exspected, &actual);

        let flight_info = client
            .execute("select * from m0 where t0 = 'a' and f0 = 11 and time > 3;".to_string())
            .await
            .unwrap();
        let actual = fetch_result_and_print(flight_info, &mut client).await;
        let exspected = vec![
            "+-------------------------------+----+----+----+----+----+------+-------+",
            "| time                          | t0 | t1 | f0 | f1 | f2 | f3   | f4    |",
            "+-------------------------------+----+----+----+----+----+------+-------+",
            "| 1970-01-01T00:00:00.000000005 | a  | a  | 11 | 11 | 11 | true | 11.11 |",
            "+-------------------------------+----+----+----+----+----+------+-------+",
        ];
        assert_batches_eq!(exspected, &actual);

        let flight_info = client
            .execute(
                "select * from m0 where t0 = 'a' and f0 = 11 or time > 3 order by time desc;"
                    .to_string(),
            )
            .await
            .unwrap();
        let actual = fetch_result_and_print(flight_info, &mut client).await;
        let exspected = vec![
            "+-------------------------------+----+----+----+----+-----+-------+-------+",
            "| time                          | t0 | t1 | f0 | f1 | f2  | f3    | f4    |",
            "+-------------------------------+----+----+----+----+-----+-------+-------+",
            "| 2207-04-18T13:56:55.539246350 |    |    |    |    |     | false |       |",
            "| 2155-12-04T02:07:05.191822176 |    |    |    |    | 888 |       |       |",
            "| 2141-08-03T00:21:21.413349294 |    |    |    |    |     |       | 1.111 |",
            "| 2096-05-01T02:46:47.649375642 |    |    |    |    | 999 |       |       |",
            "| 1970-01-01T00:00:00.000000006 | b  | c  | 15 | 11 | 11  | false | 11.11 |",
            "| 1970-01-01T00:00:00.000000005 | a  | a  | 11 | 11 | 11  | true  | 11.11 |",
            "| 1970-01-01T00:00:00.000000004 | b  | a  | 14 | 11 | 11  | true  | 11.11 |",
            "| 1970-01-01T00:00:00.000000001 | a  | b  | 11 | 11 | 11  | true  | 11.11 |",
            "+-------------------------------+----+----+----+----+-----+-------+-------+",
        ];
        assert_batches_eq!(exspected, &actual);

        let flight_info = client
            .execute("explain select * from m0 where t0 = null;".to_string())
            .await
            .unwrap();
        let actual = fetch_result_and_print(flight_info, &mut client).await;
        let exspected = vec![
            "+---------------+---------------------------------------------------------------------------------------------------------------------------------------+",
            "| plan_type     | plan                                                                                                                                  |",
            "+---------------+---------------------------------------------------------------------------------------------------------------------------------------+",
            "| logical_plan  | Filter: m0.t0 = Utf8(NULL)                                                                                                            |",
            "|               |   TableScan: m0 projection=[time, t0, t1, f0, f1, f2, f3, f4], partial_filters=[m0.t0 = Utf8(NULL)]                                   |",
            "| physical_plan | CoalesceBatchesExec: target_batch_size=8192                                                                                           |",
            "|               |   FilterExec: t0@1 = NULL                                                                                                             |",
            "|               |     TskvExec: limit=None, predicate=ColumnDomains { column_to_domain: Some({}) }, split_num=5, projection=[time,t0,t1,f0,f1,f2,f3,f4] |",
            "|               |                                                                                                                                       |",
            "+---------------+---------------------------------------------------------------------------------------------------------------------------------------+",
        ];
        assert_batches_eq!(exspected, &actual);

        let flight_info = client
            .execute("explain select * from m0 where t0 > null;".to_string())
            .await
            .unwrap();
        let actual = fetch_result_and_print(flight_info, &mut client).await;
        let exspected = vec![
            "+---------------+---------------------------------------------------------------------------------------------------------------------------------------+",
            "| plan_type     | plan                                                                                                                                  |",
            "+---------------+---------------------------------------------------------------------------------------------------------------------------------------+",
            "| logical_plan  | Filter: m0.t0 > Utf8(NULL)                                                                                                            |",
            "|               |   TableScan: m0 projection=[time, t0, t1, f0, f1, f2, f3, f4], partial_filters=[m0.t0 > Utf8(NULL)]                                   |",
            "| physical_plan | CoalesceBatchesExec: target_batch_size=8192                                                                                           |",
            "|               |   FilterExec: t0@1 > NULL                                                                                                             |",
            "|               |     TskvExec: limit=None, predicate=ColumnDomains { column_to_domain: Some({}) }, split_num=5, projection=[time,t0,t1,f0,f1,f2,f3,f4] |",
            "|               |                                                                                                                                       |",
            "+---------------+---------------------------------------------------------------------------------------------------------------------------------------+",
        ];
        assert_batches_eq!(exspected, &actual);

        // clean env
        let flight_info = client
            .execute("DROP DATABASE IF EXISTS filter_push_down;".to_string())
            .await
            .unwrap();
        let _actual = fetch_result_and_print(flight_info, &mut client).await;
    }
}
