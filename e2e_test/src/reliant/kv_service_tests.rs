#[cfg(test)]
mod test {
    use protos::kv_service::tskv_service_client::TskvServiceClient;
    use protos::kv_service::*;
    use protos::models::*;
    use protos::{self, models_helper};
    use tokio::sync::mpsc;
    use tokio_stream::wrappers::ReceiverStream;
    use tonic::transport::{Certificate, Channel, ClientTlsConfig};
    use tonic::Request;

    async fn get_tls_config() -> ClientTlsConfig {
        let pem = tokio::fs::read("../config/tls/ca.crt").await.unwrap();
        let ca = Certificate::from_pem(pem);
        ClientTlsConfig::new()
            .ca_certificate(ca)
            .domain_name("cnosdb.com")
    }
    async fn get_client(tls: bool) -> TskvServiceClient<Channel> {
        if tls {
            let channel = Channel::from_static("http://127.0.0.1:31006")
                .tls_config(get_tls_config().await)
                .unwrap()
                .connect()
                .await
                .unwrap();
            TskvServiceClient::new(channel)
        } else {
            TskvServiceClient::connect("http://127.0.0.1:31006")
                .await
                .unwrap()
        }
    }

    #[tokio::test]
    #[ignore]
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

        let mut client = get_client(true).await;

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

    #[tokio::test]
    #[ignore]
    async fn test_tskv_write_points() {
        let (tx, rx) = mpsc::channel(1);
        tokio::spawn(async move {
            for _ in 0..10 {
                let mut fbb = flatbuffers::FlatBufferBuilder::new();
                let points = models_helper::create_random_points_with_delta(&mut fbb, 1);
                fbb.finish(points, None);
                let points = fbb.finished_data().to_vec();
                tx.send(WritePointsRequest {
                    version: 1,
                    meta: Some(Meta {
                        tenant: "cnosdb".to_string(),
                        user: None,
                        password: None,
                    }),
                    points,
                })
                .await
                .unwrap();
            }
        });
        let req_stream = ReceiverStream::from(rx);

        let mut client = get_client(true).await;

        let mut resp_stream = client.write_points(req_stream).await.unwrap().into_inner();

        loop {
            match resp_stream.message().await {
                Ok(Some(item)) => {
                    println!("\treceived: {:?}", item);
                }
                Ok(None) => {
                    println!("\t stream finished.");
                    break;
                }
                Err(err) => {
                    println!("{}", err);
                    break;
                }
            }
        }
    }
}
