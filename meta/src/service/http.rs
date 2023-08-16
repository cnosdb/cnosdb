use std::convert::Infallible as StdInfallible;
use std::sync::Arc;

use replication::apply_store::{ApplyStorage, HashMapSnapshotData};
use replication::network_server::RaftHttpAdmin;
use replication::raft_node::RaftNode;
use trace::info;
use warp::{hyper, Filter};

use crate::error::MetaError;
use crate::store::command::*;
use crate::store::storage::StateMachine;

pub struct HttpServer {
    pub node: Arc<RaftNode>,
    pub storage: Arc<StateMachine>,
    pub raft_admin: Arc<RaftHttpAdmin>,
}

impl HttpServer {
    pub fn routes(
        &self,
    ) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
        self.raft_admin
            .routes()
            .or(self.read())
            .or(self.write())
            .or(self.dump())
            .or(self.restore())
            .or(self.debug())
            .or(self.debug_pprof())
            .or(self.debug_backtrace())
    }

    fn with_raft_node(
        &self,
    ) -> impl Filter<Extract = (Arc<RaftNode>,), Error = StdInfallible> + Clone {
        let node = self.node.clone();
        warp::any().map(move || node.clone())
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
            .and(self.with_raft_node())
            .and_then(|req: hyper::body::Bytes, node: Arc<RaftNode>| async move {
                match node.raw_raft().client_write(req.to_vec()).await {
                    Ok(rsp) => {
                        let resp = warp::reply::with_status(rsp.data, http::StatusCode::OK);
                        let res: Result<warp::reply::WithStatus<Vec<u8>>, warp::Rejection> =
                            Ok(resp);
                        return res;
                    }

                    Err(err) => {
                        info!("http write error: {:?}", err);
                        if let Some(openraft::error::ForwardToLeader {
                            leader_id: Some(_leader_id),
                            leader_node: Some(leader_node),
                        }) = err.forward_to_leader()
                        {
                            let resp = warp::reply::with_status(
                                leader_node.address.clone().into_bytes(),
                                http::StatusCode::PERMANENT_REDIRECT,
                            );
                            let res: Result<warp::reply::WithStatus<Vec<u8>>, warp::Rejection> =
                                Ok(resp);
                            return res;
                        } else {
                            let resp = warp::reply::with_status(
                                err.to_string().into_bytes(),
                                http::StatusCode::INTERNAL_SERVER_ERROR,
                            );
                            let res: Result<warp::reply::WithStatus<Vec<u8>>, warp::Rejection> =
                                Ok(resp);
                            return res;
                        }
                    }
                }
            })
    }

    // curl -XPOST http://127.0.0.1:8901/dump --o ./meta_dump.data
    // curl -XPOST http://127.0.0.1:8901/restore --data-binary "@./meta_dump.data"
    fn dump(&self) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
        warp::path!("dump").and(self.with_storage()).and_then(
            |storage: Arc<StateMachine>| async move {
                let data = storage
                    .snapshot()
                    .await
                    .map_err(MetaError::from)
                    .map_err(warp::reject::custom)?;

                let data: HashMapSnapshotData = serde_json::from_slice(&data)
                    .map_err(MetaError::from)
                    .map_err(warp::reject::custom)?;

                let mut rsp = "".to_string();
                for (key, val) in data.map.iter() {
                    rsp = rsp + &format!("{}: {}\n", key, val);
                }

                let res: Result<String, warp::Rejection> = Ok(rsp);
                res
            },
        )
    }

    fn restore(
        &self,
    ) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
        warp::path!("restore")
            .and(warp::body::bytes())
            .and(self.with_raft_node())
            .and_then(|req: hyper::body::Bytes, node: Arc<RaftNode>| async move {
                info!("restore data length:{}", req.len());

                let mut count = 0;
                let req = String::from_utf8_lossy(&req).to_string();
                let lines: Vec<&str> = req.split('\n').collect();
                for line in lines {
                    let strs: Vec<&str> = line.splitn(2, ": ").collect();
                    if strs.len() != 2 {
                        continue;
                    }

                    let command = WriteCommand::Set {
                        key: strs[0].to_string(),
                        value: strs[1].to_string(),
                    };

                    let data = serde_json::to_vec(&command)
                        .map_err(MetaError::from)
                        .map_err(warp::reject::custom)?;

                    if let Err(err) = node.raw_raft().client_write(data).await {
                        return Ok(err.to_string());
                    }

                    count += 1;
                }

                let data = format!("Restore Data Success, Total: {} ", count);
                let res: Result<String, warp::Rejection> = Ok(data);

                res
            })
    }

    fn debug(&self) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
        warp::path!("debug").and(self.with_storage()).and_then(
            |storage: Arc<StateMachine>| async move {
                let data = storage
                    .snapshot()
                    .await
                    .map_err(MetaError::from)
                    .map_err(warp::reject::custom)?;

                let data: HashMapSnapshotData = serde_json::from_slice(&data)
                    .map_err(MetaError::from)
                    .map_err(warp::reject::custom)?;

                let mut rsp = "****** ------------------------------------- ******\n".to_string();
                for (key, val) in data.map.iter() {
                    rsp = rsp + &format!("* {}: {}\n", key, val);
                }
                rsp += "****** ------------------------------------- ******\n";

                let res: Result<String, warp::Rejection> = Ok(rsp);
                res
            },
        )
    }

    fn debug_pprof(
        &self,
    ) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
        warp::path!("debug" / "pprof").and_then(|| async move {
            let rsp = Self::cpu_pprof().await;

            let res: Result<String, warp::Rejection> = Ok(rsp);
            res
        })
    }

    async fn cpu_pprof() -> String {
        #[cfg(unix)]
        {
            match utils::pprof_tools::gernate_pprof().await {
                Ok(v) => v,
                Err(v) => v,
            }
        }
        #[cfg(not(unix))]
        {
            "/debug/pprof only supported on *unix systems.".to_string()
        }
    }

    fn debug_backtrace(
        &self,
    ) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
        warp::path!("debug" / "backtrace").and_then(|| async move {
            let rsp = utils::backtrace::backtrace();

            let res: Result<String, warp::Rejection> = Ok(rsp);
            res
        })
    }
}

// pub async fn watch(
//     app: Data<MetaApp>,
//     req: Json<(String, String, HashSet<String>, u64)>, //client id, cluster,version
// ) -> actix_web::Result<impl Responder> {
//     info!("watch all  args: {:?}", req);
//     let client = req.0 .0;
//     let cluster = req.0 .1;
//     let tenants = req.0 .2;
//     let base_ver = req.0 .3;
//     let mut follow_ver = base_ver;

//     let mut notify = {
//         let sm = app.store.state_machine.read().await;
//         let watch_data = sm.read_change_logs(&cluster, &tenants, follow_ver);
//         info!(
//             "{} {}.{}: change logs: {:?} ",
//             client, base_ver, follow_ver, watch_data
//         );

//         if watch_data.need_return(base_ver) {
//             let data = response_encode(Ok(watch_data));
//             let response: Result<CommandResp, Infallible> = Ok(data);
//             return Ok(Json(response));
//         }

//         sm.watch.subscribe()
//     };

//     let now = std::time::Instant::now();
//     loop {
//         let _ = tokio::time::timeout(tokio::time::Duration::from_secs(20), notify.recv()).await;

//         let sm = app.store.state_machine.read().await;
//         let watch_data = sm.read_change_logs(&cluster, &tenants, follow_ver);
//         info!(
//             "{} {}.{}: change logs: {:?} ",
//             client, base_ver, follow_ver, watch_data
//         );
//         if watch_data.need_return(base_ver) || now.elapsed() > Duration::from_secs(30) {
//             let data = response_encode(Ok(watch_data));
//             let response: Result<CommandResp, Infallible> = Ok(data);
//             return Ok(Json(response));
//         }

//         if follow_ver < watch_data.max_ver {
//             follow_ver = watch_data.max_ver;
//         }
//     }
// }
