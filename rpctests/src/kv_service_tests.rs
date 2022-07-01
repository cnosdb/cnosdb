#[cfg(test)]
mod test {
    use protos::{self, kv_service::*, models::*, models_helper};
    use tokio::sync::mpsc;
    use tokio_stream::wrappers::ReceiverStream;
    use tonic::Request;

    #[tokio::test]
    async fn test_tskv_ping() {
        use protos::kv_service::tskv_service_client::TskvServiceClient;
        let mut fbb = flatbuffers::FlatBufferBuilder::new();
        let payload = fbb.create_vector(b"hello world");

        let mut builder = PingBodyBuilder::new(&mut fbb);
        builder.add_payload(payload);
        let root = builder.finish();
        fbb.finish(root, None);

        let finished_data = fbb.finished_data();

        let decoded_payload = flatbuffers::root::<PingBody>(finished_data);
        assert!(decoded_payload.is_ok());

        let mut client = TskvServiceClient::connect("http://127.0.0.1:31006").await.unwrap();

        let resp = client.ping(Request::new(PingRequest { version: 10,
                                                          body: finished_data.to_vec() }))
                         .await;
        assert!(resp.is_ok());

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
    async fn test_tskv_write_points() {
        let (tx, rx) = mpsc::channel(1);
        tokio::spawn(async move {
            for _ in 0..10 {
                let mut fbb = flatbuffers::FlatBufferBuilder::new();
                let points = models_helper::create_random_points(&mut fbb, 1);
                fbb.finish(points, None);
                let points = fbb.finished_data().to_vec();
                tx.send(WritePointsRpcRequest { version: 1,
                                                database: "database".to_string(),
                                                points })
                  .await
                  .unwrap();
            }
        });
        let req_stream = ReceiverStream::from(rx);

        let mut client =
            tskv_service_client::TskvServiceClient::connect("http://127.0.0.1:31006").await
                                                                                     .unwrap();

        let mut resp_stream = client.write_points(req_stream).await.unwrap().into_inner();

        loop {
            match resp_stream.message().await {
                Ok(Some(item)) => {
                    println!("\trecived: {:?}", item);
                },
                Ok(None) => {
                    println!("\t stream finished.");
                    break;
                },
                Err(err) => {
                    println!("{}", err);
                    break;
                },
            }
        }
    }
}
