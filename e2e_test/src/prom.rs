#[cfg(test)]
mod test {
    use http_protocol::response::Response;
    use http_protocol::status_code;
    use models::snappy::SnappyCodec;
    use protobuf::Message;
    use protos::models_helper::{parse_proto_bytes, to_proto_bytes};
    use protos::prompb::remote::{Query, QueryResult, ReadRequest, ReadResponse, WriteRequest};
    use protos::prompb::types::{label_matcher, Label, LabelMatcher, Sample, TimeSeries};

    use crate::http_api_tests::test::client;

    const SQL_PATH: &str = "/api/v1/sql";
    const PROM_WRITE_PATH: &str = "/api/v1/prom/write";
    const PROM_READ_PATH: &str = "/api/v1/prom/read";

    fn serialize<T: Message>(msg: T) -> Vec<u8> {
        let mut compressed = Vec::new();
        let input_buf = to_proto_bytes(msg).unwrap();
        SnappyCodec::default()
            .compress(&input_buf, &mut compressed)
            .unwrap();
        compressed
    }

    fn deserialize<T: Message>(compressed: &[u8]) -> T {
        let mut decompressed = Vec::new();
        SnappyCodec::default()
            .decompress(compressed, &mut decompressed, None)
            .unwrap();
        parse_proto_bytes::<T>(&decompressed).unwrap()
    }

    fn labels() -> Vec<Label> {
        let table_name_label = Label {
            name: "__name__".to_string(),
            value: "test_prom".to_string(),
            ..Default::default()
        };

        let tag_label = Label {
            name: "tag1".to_string(),
            value: "todo!()".to_string(),
            ..Default::default()
        };
        vec![table_name_label, tag_label]
    }

    fn test_write_req() -> Vec<u8> {
        let sample = Sample {
            value: 1.1,
            timestamp: 1686819776617,
            ..Default::default()
        };

        let timeseries = TimeSeries {
            labels: labels(),
            samples: vec![sample],
            ..Default::default()
        };

        let write_request = WriteRequest {
            timeseries: vec![timeseries],
            ..Default::default()
        };

        serialize(write_request)
    }

    fn test_read_req() -> Vec<u8> {
        let table_name_label_matcher = LabelMatcher {
            type_: label_matcher::Type::EQ.into(),
            name: "__name__".to_string(),
            value: "test_prom".to_string(),
            ..Default::default()
        };
        let label_matcher = LabelMatcher {
            type_: label_matcher::Type::EQ.into(),
            name: "tag1".to_string(),
            value: "todo!()".to_string(),
            ..Default::default()
        };

        let query = Query {
            start_timestamp_ms: 1686819776615,
            end_timestamp_ms: 1686819777615,
            matchers: vec![table_name_label_matcher, label_matcher],
            ..Default::default()
        };

        let read_req = ReadRequest {
            queries: vec![query],
            ..Default::default()
        };

        serialize(read_req)
    }

    fn test_read_resp() -> ReadResponse {
        let sample = Sample {
            value: 1.1,
            timestamp: 1686819776617,
            ..Default::default()
        };

        let timeseries = TimeSeries {
            labels: labels(),
            samples: vec![sample],
            ..Default::default()
        };

        let result = QueryResult {
            timeseries: vec![timeseries],
            ..Default::default()
        };

        ReadResponse {
            results: vec![result],
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn test_prom() {
        let param = &[("db", "public")];
        let username = "root";

        let client = client();

        // clean data
        let body = "drop table if exists test_prom;";
        let resp: Response = client
            .post(SQL_PATH)
            .query(param)
            .basic_auth::<&str, &str>(username, None)
            .body(body)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), status_code::OK);

        // write data
        let body = test_write_req();
        let resp: Response = client
            .post(PROM_WRITE_PATH)
            .query(param)
            .basic_auth::<&str, &str>(username, None)
            .body(body)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), status_code::OK);

        // read data
        let body = test_read_req();
        let resp: Response = client
            .post(PROM_READ_PATH)
            .query(param)
            .basic_auth::<&str, &str>(username, None)
            .body(body)
            .send()
            .await
            .unwrap();

        assert_eq!(resp.status(), status_code::OK);

        let resp: ReadResponse = deserialize(&resp.bytes().await.unwrap());

        assert_eq!(resp, test_read_resp());
    }
}
