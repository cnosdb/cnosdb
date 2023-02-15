use std::pin::Pin;

use futures::Stream;
use protos::kv_service::tskv_service_server::TskvService;
use protos::kv_service::{PingRequest, PingResponse, WritePointsRequest, WritePointsResponse};
use protos::models::{PingBody, PingBodyBuilder};
use tokio::sync::mpsc::{self};
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt;
use tonic::{Request, Response, Status, Streaming};
use trace::debug;
use tskv::engine::EngineRef;

pub struct TskvServiceImpl {
    pub kv_engine: EngineRef,
}

#[tonic::async_trait]
impl TskvService for TskvServiceImpl {
    async fn ping(&self, _request: Request<PingRequest>) -> Result<Response<PingResponse>, Status> {
        debug!("PING");

        let ping_req = _request.into_inner();
        let ping_body = flatbuffers::root::<PingBody>(&ping_req.body);
        if let Err(e) = ping_body {
            eprintln!("{}", e);
        } else {
            println!("ping_req:body(flatbuffer): {:?}", ping_body);
        }

        let mut fbb = flatbuffers::FlatBufferBuilder::new();
        let payload = fbb.create_vector(b"hello, caller");

        let mut builder = PingBodyBuilder::new(&mut fbb);
        builder.add_payload(payload);
        let root = builder.finish();
        fbb.finish(root, None);

        let finished_data = fbb.finished_data();

        Ok(Response::new(PingResponse {
            version: 1,
            body: finished_data.to_vec(),
        }))
    }

    type WritePointsStream =
        Pin<Box<dyn Stream<Item = Result<WritePointsResponse, Status>> + Send + Sync + 'static>>;

    async fn write_points(
        &self,
        request: Request<Streaming<WritePointsRequest>>,
    ) -> Result<Response<Self::WritePointsStream>, Status> {
        let mut stream = request.into_inner();
        let (resp_sender, resp_receiver) = mpsc::channel(128);
        // let req_sender = self.sender.clone();
        // let f =
        while let Some(result) = stream.next().await {
            match result {
                Ok(req) => {
                    let ret = self
                        .kv_engine
                        .write(0, req)
                        .await
                        .map_err(|err| Status::internal(err.to_string()));
                    resp_sender.send(ret).await.expect("successful");
                }
                Err(status) => {
                    match resp_sender.send(Err(status)).await {
                        Ok(_) => (),
                        Err(_err) => break, // response was dropped
                    }
                }
            }
        }
        println!("stream ended");

        // tokio::spawn(f);
        // echo just write the same data that was received
        let out_stream = ReceiverStream::new(resp_receiver);

        Ok(Response::new(Box::pin(out_stream)))
    }
}
