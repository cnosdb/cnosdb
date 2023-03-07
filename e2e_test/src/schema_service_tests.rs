#[cfg(test)]
mod test {
    use protos::schema_service::*;
    use protos::{self};
    use tonic::Request;

    #[tokio::test]
    #[ignore]
    async fn test_get_database() {
        let mut client =
            schema_service_client::SchemaServiceClient::connect("http://127.0.0.1:31006")
                .await
                .unwrap();

        let req = GetDatabaseRequest {
            database: "dba".to_string(),
        };

        let resp = client.get_database(Request::new(req)).await;
        assert!(resp.is_ok());

        let resp = resp.unwrap();
        dbg!(&resp);
    }
}
