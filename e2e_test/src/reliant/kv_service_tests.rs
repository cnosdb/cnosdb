#![cfg(test)]

use std::path::PathBuf;

use protos::kv_service::tskv_service_client::TskvServiceClient;
use protos::kv_service::*;
use protos::models::*;
use protos::{self};
use tonic::transport::{Certificate, Channel, ClientTlsConfig};
use tonic::Request;

use crate::utils::CRATE_DIR;

async fn get_tls_config() -> ClientTlsConfig {
    let create_dir = PathBuf::from(CRATE_DIR);
    let pem_path = create_dir
        .parent()
        .unwrap_or(&create_dir)
        .join("config/resource/tls/ca.crt");
    let pem = tokio::fs::read(pem_path).await.unwrap();
    let ca = Certificate::from_pem(pem);
    ClientTlsConfig::new()
        .ca_certificate(ca)
        .domain_name("cnosdb.com")
}
async fn get_client(tls: bool) -> TskvServiceClient<Channel> {
    if tls {
        let channel = Channel::from_static("http://127.0.0.1:8903")
            .tls_config(get_tls_config().await)
            .unwrap()
            .connect()
            .await
            .unwrap();
        TskvServiceClient::new(channel)
    } else {
        TskvServiceClient::connect("http://127.0.0.1:8903")
            .await
            .unwrap()
    }
}

#[tokio::test]
async fn test_tskv_ping() {
    let mut fbb = flatbuffers::FlatBufferBuilder::new();
    let payload = fbb.create_vector(b"hello world");

    let mut builder = PingBodyBuilder::new(&mut fbb);
    builder.add_payload(payload);
    let root = builder.finish();
    fbb.finish(root, None);

    let finished_data = fbb.finished_data();

    let decoded_payload = flatbuffers::root::<PingBody>(finished_data);
    assert!(decoded_payload.is_ok());

    let mut client = get_client(false).await;

    let resp = client
        .ping(Request::new(PingRequest {
            version: 10,
            body: finished_data.to_vec(),
        }))
        .await;

    let ping_response = resp.unwrap().into_inner();
    println!("PING: {:?}", ping_response);

    let ping_response_body = flatbuffers::root::<PingBody>(&ping_response.body);
    if let Err(e) = ping_response_body {
        eprintln!("{}", e);
    } else {
        println!("ping_resp:body(flatbuffer): {:?}", ping_response_body);
    }
}
