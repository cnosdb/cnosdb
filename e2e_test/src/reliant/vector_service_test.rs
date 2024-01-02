#![cfg(test)]

use protos::vector::vector_client::VectorClient;
use tonic::transport::{Certificate, Channel, ClientTlsConfig};

async fn get_tls_config() -> ClientTlsConfig {
    let pem = tokio::fs::read("../config/tls/ca.crt").await.unwrap();
    let ca = Certificate::from_pem(pem);
    ClientTlsConfig::new()
        .ca_certificate(ca)
        .domain_name("cnosdb.com")
}

async fn get_client(tls: bool) -> VectorClient<Channel> {
    if tls {
        let channel = Channel::from_static("http://127.0.0.1:8906")
            .tls_config(get_tls_config().await)
            .unwrap()
            .connect()
            .await
            .unwrap();
        VectorClient::new(channel)
    } else {
        VectorClient::connect("http://127.0.0.1:8906")
            .await
            .unwrap()
    }
}

#[tokio::test]
async fn test_health_check() {
    let mut client = get_client(false).await;
    let resp = client
        .health_check(tonic::Request::new(protos::vector::HealthCheckRequest {}))
        .await;
    assert!(resp.is_ok());
    let health_check_response = resp.unwrap().into_inner();
    assert_eq!(health_check_response.status, 0);
}

#[tokio::test]
async fn test_write_metric() {
    let mut client = get_client(false).await;
    let resp = client
        .push_events(tonic::Request::new(protos::vector::PushEventsRequest {
            events: vec![],
        }))
        .await;

    assert!(resp.is_ok());
}
