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
}
