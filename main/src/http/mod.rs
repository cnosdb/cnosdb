use std::collections::HashMap;
use std::convert::Infallible;
use std::net::SocketAddr;

use chrono::Local;
use flatbuffers::FlatBufferBuilder;
use futures::StreamExt;
use hyper::server::conn::AddrStream;
use hyper::service::{make_service_fn, service_fn};
use hyper::{Body, Request, Response, Server as HyperServer};
use line_protocol::{line_protocol_to_lines, Line};
use protos::kv_service::WritePointsRpcRequest;
use protos::models::{self as fb_models, FieldBuilder, Point, PointArgs, TagBuilder};
use snafu::{ResultExt, Snafu};
use tokio::sync::mpsc::error::SendError;
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::oneshot;
use tokio::sync::oneshot::error::RecvError;
use trace::{debug, info};

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(display("Hyper error: {}", source))]
    Hyper { source: hyper::Error },

    #[snafu(display("Body oversize: {}", size))]
    BodyOversize { size: usize },

    #[snafu(display("Message is not valid UTF-8"))]
    NotUtf8,

    #[snafu(display("Error parsing message: {}", source))]
    ParseLineProtocol { source: line_protocol::Error },

    #[snafu(display("Error sending to channel receiver: {}", source))]
    ChannelSend { source: SendError<tskv::Task> },

    #[snafu(display("Error receiving from channel receiver: {}", source))]
    ChannelReceive { source: RecvError },

    #[snafu(display("Error from tskv: {}", source))]
    Tskv { source: tskv::Error },

    #[snafu(display("Invalid message: {}", reason))]
    Syntax { reason: String },
}

pub async fn serve(addr: SocketAddr, sender: UnboundedSender<tskv::Task>) -> Result<(), Error> {
    let make_service = make_service_fn(move |conn: &AddrStream| {
        let sender = sender.clone();
        let remote_addr = conn.remote_addr();
        info!("Remote IP: {}", remote_addr);

        let service = service_fn(move |req| match req.uri().path() {
            "/write/line_protocol" => write_line_protocol(sender.clone(), req),
            _ => write_line_protocol(sender.clone(), req),
        });

        async move { Ok::<_, Infallible>(service) }
    });

    let server = HyperServer::bind(&addr).serve(make_service);

    server.await.context(HyperSnafu)
}

async fn write_line_protocol(
    sender: UnboundedSender<tskv::Task>,
    req: Request<Body>,
) -> Result<Response<Body>, Error> {
    let database;
    if let Some(query) = req.uri().query() {
        let query_params = parse_query(query);
        database = query_params
            .get("database")
            .unwrap_or_else(|| &"")
            .to_string();
    } else {
        return Err(Error::Syntax {
            reason: format!("Need some request parameters."),
        });
    }

    if database.is_empty() {
        return Err(Error::Syntax {
            reason: format!("Request has no parameter 'database'."),
        });
    }

    let mut body = req.into_body();
    let mut buffer: Vec<u8> = Vec::with_capacity(512);
    let mut len = 0_usize;
    while let Some(chunk) = body.next().await {
        let chunk = chunk.context(HyperSnafu)?;
        len += chunk.len();
        if len > 102400 {
            return Err(Error::BodyOversize { size: 102400 });
        }
        buffer.extend_from_slice(chunk.as_ref());
    }
    let lines = String::from_utf8(buffer).map_err(|_| Error::NotUtf8)?;
    let line_protocol_lines = line_protocol_to_lines(&lines, Local::now().timestamp_millis())
        .context(ParseLineProtocolSnafu)?;
    debug!("Write request: {:?}", line_protocol_lines);
    let points = parse_lines_to_points(&line_protocol_lines);

    let req = WritePointsRpcRequest {
        version: 1,
        database,
        points,
    };

    // 1. send Request to handler
    let (tx, rx) = oneshot::channel();
    let _ = sender
        .send(tskv::Task::WritePoints { req, tx })
        .context(ChannelSendSnafu)?;

    // 3. receive Response from handler
    let _ = match rx.await {
        Ok(Ok(resp)) => resp,
        Ok(Err(err)) => return Err(Error::Tskv { source: err }),
        Err(err) => return Err(Error::ChannelReceive { source: err }),
    };

    let resp = http::Response::builder()
        .body(Body::from("Write succeed."))
        .unwrap();
    Ok(resp)
}

/// Parse query('param1=val1&param2=val2') to HashMap.
fn parse_query(query: &str) -> HashMap<&str, &str> {
    let mut map = HashMap::new();

    let mut key_begin = 0_usize;
    let mut val_begin = 0_usize;
    let mut key = "";
    for (i, c) in query.chars().enumerate() {
        if c == '=' {
            key = &query[key_begin..i];
            val_begin = i + 1;
        }
        if c == '&' {
            let value = &query[val_begin..i];
            map.insert(key, value);
            key_begin = i + 1;
        }
    }
    if val_begin < query.len() {
        let value = &query[val_begin..];
        map.insert(key, value);
    }

    map
}

fn parse_lines_to_points(lines: &[Line]) -> Vec<u8> {
    let mut fbb = FlatBufferBuilder::new();
    let mut point_offsets = Vec::with_capacity(lines.len());
    for line in lines.iter() {
        let mut tags = Vec::new();
        for (k, v) in line.tags.iter() {
            let fbk = fbb.create_vector(k.as_bytes());
            let fbv = fbb.create_vector(v.as_bytes());
            let mut tag_builder = TagBuilder::new(&mut fbb);
            tag_builder.add_key(fbk);
            tag_builder.add_value(fbv);
            tags.push(tag_builder.finish());
        }
        let mut fields = Vec::new();
        for (k, v) in line.fields.iter() {
            let fbk = fbb.create_vector(k.as_bytes());
            let fbv = fbb.create_vector(v.as_bytes());
            let mut field_builder = FieldBuilder::new(&mut fbb);
            field_builder.add_name(fbk);
            field_builder.add_type_(fb_models::FieldType::Float);
            field_builder.add_value(fbv);
            fields.push(field_builder.finish());
        }
        let point_args = PointArgs {
            tags: Some(fbb.create_vector(&tags)),
            fields: Some(fbb.create_vector(&fields)),
            timestamp: line.timestamp,
        };
        point_offsets.push(Point::create(&mut fbb, &point_args));
    }
    let points = fbb.create_vector(&point_offsets);
    fbb.finish(points, None);
    fbb.finished_data().to_vec()
}

#[cfg(test)]
mod test {
    use std::net::SocketAddr;

    use protos::kv_service::WritePointsRpcResponse;
    use tokio::{spawn, sync::mpsc};
    use tskv::Task;

    use super::serve;

    /// Start a server instance to test write-requests.
    /// This test case won't stop itself. Use ctrl+c to stop.
    #[tokio::test]
    #[ignore]
    async fn run_server() {
        // curl -v -X POST http://127.0.0.1:8003/write/line_protocol\?database\=dba --data "ma,ta=a1,tb=b1 fa=1,fb=2 1"

        let http_host = "127.0.0.1:8003"
            .parse::<SocketAddr>()
            .expect("Invalid host");
        let (sender, mut receiver) = mpsc::unbounded_channel();
        let server_join_handle = spawn(async move { serve(http_host, sender).await });

        spawn(async move {
            while let Some(task) = receiver.recv().await {
                match task {
                    Task::WritePoints { req, tx } => {
                        println!("{:?}", req);
                        let resp = WritePointsRpcResponse {
                            version: 1,
                            points: req.points.clone(),
                        };
                        tx.send(Ok(resp)).unwrap();
                    }
                    _ => {}
                }
            }
        });

        // let mut client_join_hadnles = Vec::new();
        // for i in 0..1 {
        //     client_join_hadnles.push(spawn(async move {
        //         let client = reqwest::Client::new();
        //         let resp = client
        //             .post("http://127.0.0.1:8003/write/line_protocol?database=dba")
        //             .body("ma,ta=a1,tb=b1 fa=1,fb=2")
        //             .send()
        //             .await
        //             .unwrap()
        //             .bytes()
        //             .await
        //             .unwrap()
        //             .to_vec();
        //         let resp = String::from_utf8(resp).unwrap();
        //         println!("{}-length:{} '{}'", i, resp.len(), resp);
        //     }));
        // }

        server_join_handle.await.unwrap().unwrap();
    }
}
