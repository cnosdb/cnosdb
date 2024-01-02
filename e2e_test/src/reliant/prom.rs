#![cfg(test)]

use http_protocol::status_code;
use models::snappy::SnappyCodec;
use protobuf::Message;
use protos::models_helper::{parse_proto_bytes, to_proto_bytes};
use protos::prompb::remote::{Query, QueryResult, ReadRequest, ReadResponse, WriteRequest};
use protos::prompb::types::{label_matcher, Label, LabelMatcher, Sample, TimeSeries};
use reqwest::Method;

use crate::assert_response_is_ok;
use crate::utils::Client;

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

#[test]
fn test_prom() {
    let client = Client::with_auth("root".to_string(), None);

    // clean data
    let body = "drop table if exists test_prom;";
    let resp = client
        .post("http://127.0.0.1:8902/api/v1/sql?db=public", body)
        .unwrap();
    assert_response_is_ok!(resp);

    // write data
    let body = test_write_req();
    let resp = client
        .request_with_auth(
            Method::POST,
            "http://127.0.0.1:8902/api/v1/prom/write?db=public",
        )
        .body(body)
        .send()
        .unwrap();
    assert_response_is_ok!(resp);

    // read data
    let body = test_read_req();
    let resp = client
        .request_with_auth(
            Method::POST,
            "http://127.0.0.1:8902/api/v1/prom/read?db=public",
        )
        .body(body)
        .send()
        .unwrap();

    assert_eq!(resp.status(), status_code::OK);

    let resp: ReadResponse = deserialize(&resp.bytes().unwrap());
    assert_eq!(resp, test_read_resp());
}
