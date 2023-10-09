use std::convert::Infallible as StdInfallible;
use std::net::SocketAddr;
use std::sync::Arc;

use warp::{hyper, Filter};

use super::http::HttpServer;
use crate::error::MetaError;
use crate::store::command::*;
use crate::store::storage::StateMachine;

pub async fn start_singe_meta_server(path: String, cluster_name: String, addr: String) {
    let db_path = format!("{}/meta/{}.data", path, 0);
    let storage = StateMachine::open(db_path).unwrap();
    let storage = Arc::new(storage);

    let init_data = crate::store::config::MetaInit {
        cluster_name,
        admin_user: models::auth::user::ROOT.to_string(),
        system_tenant: models::schema::DEFAULT_CATALOG.to_string(),
        default_database: vec![
            models::schema::USAGE_SCHEMA.to_string(),
            models::schema::DEFAULT_DATABASE.to_string(),
        ],
    };
    super::init::init_meta(storage.clone(), init_data).await;

    let server = SingleServer { storage };
    tracing::info!("single meta http server start addr: {}", addr);
    tokio::spawn(async move { server.start(addr).await });
}

pub struct SingleServer {
    pub storage: Arc<StateMachine>,
}

impl SingleServer {
    pub async fn start(&self, addr: String) {
        let addr: SocketAddr = addr.parse().unwrap();
        warp::serve(self.routes()).run(addr).await;
    }

    fn routes(
        &self,
    ) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
        self.read()
            .or(self.write())
            .or(self.watch())
            .or(self.debug())
    }

    fn with_storage(
        &self,
    ) -> impl Filter<Extract = (Arc<StateMachine>,), Error = StdInfallible> + Clone {
        let storage = self.storage.clone();
        warp::any().map(move || storage.clone())
    }

    fn read(&self) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
        warp::path!("read")
            .and(warp::body::bytes())
            .and(self.with_storage())
            .and_then(
                |req: hyper::body::Bytes, storage: Arc<StateMachine>| async move {
                    let req: ReadCommand = serde_json::from_slice(&req)
                        .map_err(MetaError::from)
                        .map_err(warp::reject::custom)?;

                    let rsp = storage.process_read_command(&req);
                    let res: Result<String, warp::Rejection> = Ok(rsp);
                    res
                },
            )
    }

    fn write(&self) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
        warp::path!("write")
            .and(warp::body::bytes())
            .and(self.with_storage())
            .and_then(
                |req: hyper::body::Bytes, storage: Arc<StateMachine>| async move {
                    let req: WriteCommand = serde_json::from_slice(&req)
                        .map_err(MetaError::from)
                        .map_err(warp::reject::custom)?;

                    let rsp = storage.process_write_command(&req);
                    let res: Result<String, warp::Rejection> = Ok(rsp);
                    res
                },
            )
    }

    fn watch(&self) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
        warp::path!("watch")
            .and(warp::body::bytes())
            .and(self.with_storage())
            .and_then(
                |req: hyper::body::Bytes, storage: Arc<StateMachine>| async move {
                    let data = HttpServer::process_watch(req, storage)
                        .await
                        .map_err(warp::reject::custom)?;

                    let res: Result<String, warp::Rejection> = Ok(data);
                    res
                },
            )
    }

    fn debug(&self) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
        warp::path!("debug").and(self.with_storage()).and_then(
            |storage: Arc<StateMachine>| async move {
                let data = HttpServer::process_debug(storage)
                    .await
                    .map_err(warp::reject::custom)?;

                let res: Result<String, warp::Rejection> = Ok(data);
                res
            },
        )
    }
}
