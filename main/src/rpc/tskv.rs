use std::pin::Pin;

use futures::Stream;
use protos::{
    kv_service::{
        tskv_service_server::TskvService, AddSeriesRpcRequest, AddSeriesRpcResponse,
        GetSeriesInfoRpcRequest, GetSeriesInfoRpcResponse, PingRequest, PingResponse,
        WritePointsRpcRequest, WritePointsRpcResponse, WriteRowsRpcRequest, WriteRowsRpcResponse,
    },
    models::{PingBody, PingBodyBuilder},
};
use tokio::sync::mpsc::{self};
use tokio_stream::{wrappers::ReceiverStream, StreamExt};
use tonic::{Request, Response, Status, Streaming};
use trace::debug;

use tskv::engine::EngineRef;

pub struct TskvServiceImpl {
    // pub sender: channel::Sender<tskv::Task>,
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
        let (resp_sender, resp_receiver) = mpsc::channel(128);
        tokio::spawn(async move {
            while let Some(result) = stream.next().await {
                match result {
                    Ok(_req) => {
                        resp_sender
                            .send(Ok(WriteRowsRpcResponse {
                                version: 1,
                                rows: vec![],
                            }))
                            .await
                            .expect("successful");
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
        });
        // echo just write the same data that was received
        let out_stream = ReceiverStream::new(resp_receiver);

        Ok(Response::new(Box::pin(out_stream)))
    }

    type WritePointsStream =
        Pin<Box<dyn Stream<Item = Result<WritePointsRpcResponse, Status>> + Send + Sync + 'static>>;

    async fn write_points(
        &self,
        request: Request<Streaming<WritePointsRpcRequest>>,
    ) -> Result<Response<Self::WritePointsStream>, Status> {
        let mut stream = request.into_inner();
        let (resp_sender, resp_receiver) = mpsc::channel(128);
        // let req_sender = self.sender.clone();
        // let f =
        while let Some(result) = stream.next().await {
            match result {
                Ok(req) => {
                    // 1. send Request to handler
                    // let (tx, rx) = oneshot::channel();
                    // let ret = req_sender
                    //     .send(tskv::Task::WritePoints { req, tx })
                    //     .await
                    //     .map_err(|err| Status::internal(err.to_string()));

                    let ret = self
                        .kv_engine
                        .write(0, req)
                        .await
                        .map_err(|err| Status::internal(err.to_string()));
                    // 2. if something wrong when sending Request
                    // if let Err(err) = ret {
                    //     resp_sender.send(Err(err)).await.expect("successful");
                    //     continue;
                    // }
                    // // 3. receive Response from handler
                    // let ret = match rx.await {
                    //     Ok(Ok(resp)) => Ok(resp),
                    //     Ok(Err(err)) => Err(Status::internal(err.to_string())),
                    //     Err(err) => Err(Status::internal(err.to_string())),
                    // };

                    // 4. send Response out of this Stream
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
