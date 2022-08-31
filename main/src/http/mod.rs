use std::collections::HashMap;
use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::Arc;

use hyper::server::conn::AddrStream;
use hyper::service::{make_service_fn, service_fn};
use hyper::Server as HyperServer;
use snafu::{ResultExt, Snafu};
use spi::server::dbms::DatabaseManagerSystem;
use spi::server::ServerError;
use tokio::sync::mpsc::error::SendError;
use tokio::sync::oneshot::error::RecvError;
use trace::info;

use async_channel as channel;

use crate::http::handler::route;

mod handler;

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

    #[snafu(display("Error sending to channel receiver: {}", source))]
    AsyncChanSend {
        source: channel::SendError<tskv::Task>,
    },

    #[snafu(display("Error executiong query: {}", source))]
    Query { source: ServerError },

    #[snafu(display("Error from tskv: {}", source))]
    Tskv { source: tskv::Error },

    #[snafu(display("Invalid message: {}", reason))]
    Syntax { reason: String },
}

pub async fn serve(
    addr: SocketAddr,
    db: Arc<dyn DatabaseManagerSystem + Send + Sync>,
    sender: channel::Sender<tskv::Task>,
) -> Result<(), Error> {
    let make_service = make_service_fn(move |conn: &AddrStream| {
        let db = db.clone();
        let sender = sender.clone();
        let remote_addr = conn.remote_addr();
        info!("Remote IP: {}", remote_addr);

        async move {
            Ok::<_, Infallible>(service_fn(move |req| {
                route(req, db.clone(), sender.clone())
            }))
        }
    });

    let server = HyperServer::bind(&addr).serve(make_service);

    server.await.context(HyperSnafu)
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

#[cfg(test)]
mod test {
    use std::{net::SocketAddr, sync::Arc};

    use async_channel as channel;
    use config::get_config;
    use protos::kv_service::WritePointsRpcResponse;
    use tokio::spawn;
    use trace::init_default_global_tracing;
    use tskv::{Options, Task, TsKv};

    use super::serve;

    /// Start a server instance to test write-requests.
    /// This test case won't stop itself. Use ctrl+c to stop.
    #[tokio::test]
    #[ignore]
    async fn run_server() {
        //! ```bash
        //! curl -v -X POST http://127.0.0.1:8003/write/line_protocol\?database\=dba --data "ma,ta=a1,tb=b1 fa=1,fb=2 1"
        //! ```

        init_default_global_tracing("tskv_log", "tskv.log", "debug");

        let global_config = get_config("../config/config.toml");
        let http_host = "127.0.0.1:8003"
            .parse::<SocketAddr>()
            .expect("Invalid host");
        let opt = Options::from(&global_config);

        let tskv = TsKv::open(opt).await.unwrap();
        // let db = Arc::new(Db::new(Arc::new(tskv)));
        let db = Arc::new(
            server::instance::make_cnosdbms(Arc::new(tskv)).expect("Failed to build dbms."),
        );
        let (sender, receiver) = channel::unbounded();
        let server_join_handle = spawn(async move { serve(http_host, db, sender) });

        spawn(async move {
            while let Ok(task) = receiver.recv().await {
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

        server_join_handle.await.unwrap().await.unwrap();
    }
}
