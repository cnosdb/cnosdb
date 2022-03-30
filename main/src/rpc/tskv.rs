use futures::Stream;
use protos::kv_service::tskv_service_server::TskvService;
use protos::kv_service::{
    AddSeriesRpcRequest, AddSeriesRpcResponse, GetSeriesInfoRpcRequest, GetSeriesInfoRpcResponse,
    PingRequest, PingResponse, WriteRowsRpcRequest, WriteRowsRpcResponse,
};
use protos::models::{PingBody, PingBodyBuilder};
use std::pin::Pin;
use tokio::sync::mpsc;
use tokio_stream::{wrappers::ReceiverStream, StreamExt};
use tonic::{Request, Response, Status, Streaming};
// use tskv::TsKv;

pub struct TskvServiceImpl {}

#[tonic::async_trait]
impl TskvService for TskvServiceImpl {
    async fn ping(&self, _request: Request<PingRequest>) -> Result<Response<PingResponse>, Status> {
        println!("PING: {:?}", _request);

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

    async fn add_series(
        &self,
        _request: Request<AddSeriesRpcRequest>,
    ) -> Result<Response<AddSeriesRpcResponse>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }

    async fn get_series_info(
        &self,
        _request: Request<GetSeriesInfoRpcRequest>,
    ) -> Result<Response<GetSeriesInfoRpcResponse>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }

    type WriteRowsStream =
        Pin<Box<dyn Stream<Item = Result<WriteRowsRpcResponse, Status>> + Send + Sync + 'static>>;

    async fn write_rows(
        &self,
        request: Request<Streaming<WriteRowsRpcRequest>>,
    ) -> Result<Response<Self::WriteRowsStream>, Status> {
        let mut stream = request.into_inner();
        let (tx, rx) = mpsc::channel(128);
        tokio::spawn(async move {
            while let Some(result) = stream.next().await {
                match result {
                    Ok(_req) => {
                        tx.send(Ok(WriteRowsRpcResponse {
                            version: 1,
                            ..Default::default()
                        }))
                        .await
                        .expect("successful");
                    }
                    Err(err) => {
                        match tx.send(Err(err)).await {
                            Ok(_) => (),
                            Err(_err) => break, // response was droped
                        }
                    }
                }
            }
            println!("stream ended");
        });
        // echo just write the same data that was received
        let out_stream = ReceiverStream::new(rx);

        Ok(Response::new(Box::pin(out_stream)))
    }
}
