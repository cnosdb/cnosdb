#[cfg(test)]
mod test {
    use tonic::Request;

    use protos::kv_service::*;
    use protos::models::*;

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

        let decoded_payload = flatbuffers::root::<PingBody>(&finished_data);
        assert!(decoded_payload.is_ok());

        let mut client = TskvServiceClient::connect("http://[::1]:10000")
            .await
            .unwrap();

        let resp = client
            .ping(Request::new(PingRequest {
                version: 10,
                body: finished_data.to_vec(),
            }))
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
}
