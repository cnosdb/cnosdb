#[actix_rt::test]
#[ignore]
async fn test_call_add_series() {
    use protos::kv_service::{tskv_service_client, AddSeriesRpcRequest, FieldInfo, FieldType, Tag};

    let mut conn =
        tskv_service_client::TskvServiceClient::connect("http://127.0.0.1:31006").await.unwrap();

    let tags = vec![Tag { key: Vec::<u8>::from("host"), value: Vec::<u8>::from("node1") }];
    let fields = vec![FieldInfo { field_type: FieldType::Integer as i32, // enum
                                  name: Vec::<u8>::from("cpu"),
                                  id: 0 }];
    let req = AddSeriesRpcRequest { version: 1, series_id: 10010, tags, fields };
    let resp = conn.add_series(req).await;
    match resp {
        Ok(t) => {
            assert_eq!(t.get_ref().series_id, 10010);
        },
        Err(e) => {
            println!("{}", e);
        },
    };
}
