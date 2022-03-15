use std::pin::Pin;

use futures::Stream;
use tonic::{Request, Response, Status, Streaming};

use protos::models::{PingBody, PingBodyBuilder};
use protos::tskv::ts_kv_server::TsKv;
use protos::tskv::{
    AddSeriesRpcRequest, AddSeriesRpcResponse, GetSeriesInfoRpcRequest, GetSeriesInfoRpcResponse,
    PingRequest, PingResponse, WriteRowsRpcRequest, WriteRowsRpcResponse,
};

#[derive(Clone)]
pub struct TsKvImpl {}

#[tonic::async_trait]
impl TsKv for TsKvImpl {
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
            protocol_version: 1,
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
        _request: Request<Streaming<WriteRowsRpcRequest>>,
    ) -> Result<Response<Self::WriteRowsStream>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }
}

#[tokio::test]
async fn test_tikv_ping() {
    use protos::tskv::ts_kv_client::TsKvClient;

    let mut fbb = flatbuffers::FlatBufferBuilder::new();
    let payload = fbb.create_vector(b"hello world");

    let mut builder = PingBodyBuilder::new(&mut fbb);
    builder.add_payload(payload);
    let root = builder.finish();
    fbb.finish(root, None);

    let finished_data = fbb.finished_data();

    let decoded_payload = flatbuffers::root::<PingBody>(&finished_data);
    assert!(decoded_payload.is_ok());

    let mut client = TsKvClient::connect("http://[::1]:10000").await.unwrap();

    let resp = client
        .ping(Request::new(PingRequest {
            protocol_version: 10,
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
