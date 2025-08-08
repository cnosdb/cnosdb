use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use axum::body::Bytes;
use replication::raft_node::RaftNode;
use tokio::sync::RwLock;

pub use self::http_api::create_router;
use crate::error::MetaResult;
use crate::store::storage::StateMachine;

#[derive(Clone)]
pub struct HttpServer {
    pub node: Arc<RaftNode>,
    pub storage: Arc<RwLock<StateMachine>>,
}

mod http_api {
    use std::fmt::Write as _;

    use axum::body::Bytes;
    use axum::extract::{Json, Path, State};
    use axum::http::StatusCode;
    use axum::{routing, Router};

    use super::{process_cpu_pprof, process_watch, process_watch_meta_membership, HttpServer};
    use crate::error::MetaError;
    use crate::store::command::{ReadCommand, WriteCommand};
    use crate::store::dump::dump_impl;

    pub fn create_router(server: HttpServer) -> Router {
        Router::new()
            .route("/read", routing::any(read))
            .route("/write", routing::any(write))
            .route("/watch", routing::any(watch))
            .route(
                "/watch_meta_membership",
                routing::any(watch_meta_membership),
            )
            .route("/dump", routing::any(dump))
            .route("/dump/sql/ddl/{cluster}/{tenant}", routing::any(dump_sql))
            .route("/restore", routing::any(restore))
            .route("/debug", routing::any(debug))
            .route("/debug_pprof", routing::any(debug_pprof))
            .route("/debug_backtrace", routing::any(debug_backtrace))
            .route("/is_initialized", routing::get(is_initialized))
            .with_state(server)
    }

    async fn read(
        State(HttpServer { storage, .. }): State<HttpServer>,
        Json(cmd): Json<ReadCommand>,
    ) -> (StatusCode, String) {
        let resp = storage.read().await.process_read_command(&cmd);
        (StatusCode::OK, resp)
    }

    async fn write(
        State(HttpServer { node, .. }): State<HttpServer>,
        body: Bytes,
    ) -> (StatusCode, Vec<u8>) {
        let app_data = body.to_vec();
        match node.raw_raft().client_write(app_data).await {
            Ok(resp) => (StatusCode::OK, resp.data),
            Err(err) => {
                trace::info!("http write error: {:?}", err);
                if let Some(openraft::error::ForwardToLeader {
                    leader_id: Some(_leader_id),
                    leader_node: Some(leader_node),
                }) = err.forward_to_leader()
                {
                    (
                        http::StatusCode::PERMANENT_REDIRECT,
                        leader_node.address.as_bytes().to_vec(),
                    )
                } else {
                    (
                        http::StatusCode::INTERNAL_SERVER_ERROR,
                        err.to_string().into_bytes(),
                    )
                }
            }
        }
    }

    async fn watch(
        State(HttpServer { storage, .. }): State<HttpServer>,
        body: Bytes,
    ) -> (StatusCode, String) {
        match process_watch(body, storage).await {
            Ok(data) => (StatusCode::OK, data),
            Err(e) => {
                trace::error!("watch error: {e:?}");
                (StatusCode::INTERNAL_SERVER_ERROR, e.to_string())
            }
        }
    }

    async fn watch_meta_membership(
        State(HttpServer { node, .. }): State<HttpServer>,
    ) -> (StatusCode, String) {
        match node.raw_raft().ensure_linearizable().await {
            Ok(_) => match process_watch_meta_membership(node).await {
                Ok(data) => (StatusCode::OK, data),
                Err(e) => {
                    trace::error!("watch_meta_membership error: {e:?}");
                    (StatusCode::INTERNAL_SERVER_ERROR, e.to_string())
                }
            },
            Err(err) => {
                if let Some(openraft::error::ForwardToLeader {
                    leader_id: Some(_leader_id),
                    leader_node: Some(leader_node),
                }) = err.forward_to_leader()
                {
                    (StatusCode::PERMANENT_REDIRECT, leader_node.address.clone())
                } else {
                    (StatusCode::INTERNAL_SERVER_ERROR, err.to_string())
                }
            }
        }
    }

    // curl -XPOST http://127.0.0.1:8901/dump --o ./meta_dump.data
    // curl -XPOST http://127.0.0.1:8901/restore --data-binary "@./meta_dump.data"
    async fn dump(State(HttpServer { storage, .. }): State<HttpServer>) -> (StatusCode, String) {
        match storage.write().await.backup() {
            Ok(data) => {
                let mut buf = String::with_capacity(8 * data.map.len());
                for (key, val) in data.map.iter() {
                    writeln!(&mut buf, "{key}: {val}").unwrap();
                }
                (StatusCode::OK, buf)
            }
            Err(e) => {
                trace::error!("dump error: {e:?}");
                (StatusCode::INTERNAL_SERVER_ERROR, e.to_string())
            }
        }
    }

    async fn dump_sql(
        State(HttpServer { storage, .. }): State<HttpServer>,
        Path(cluster): Path<String>,
        Path(tenant): Path<Option<String>>,
    ) -> (StatusCode, String) {
        let storage = storage.read().await;
        match dump_impl(&cluster, tenant.as_deref(), &storage).await {
            Ok(data) => (StatusCode::OK, data),
            Err(e) => {
                trace::error!("dump sql error: {:?}", e);
                (StatusCode::INTERNAL_SERVER_ERROR, e.to_string())
            }
        }
    }

    async fn restore(
        State(HttpServer { node, .. }): State<HttpServer>,
        body: String,
    ) -> (StatusCode, String) {
        trace::info!("restore data length:{}", body.len());

        let mut count = 0;
        let lines: Vec<&str> = body.split('\n').collect();
        for line in lines {
            let strs: Vec<&str> = line.splitn(2, ": ").collect();
            if strs.len() != 2 {
                continue;
            }

            let command = WriteCommand::Set {
                key: strs[0].to_string(),
                value: strs[1].to_string(),
            };

            let data = match serde_json::to_vec(&command).map_err(MetaError::from) {
                Ok(data) => data,
                Err(e) => {
                    trace::error!("restore data parse error: {e:?}");
                    return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string());
                }
            };

            if let Err(e) = node.raw_raft().client_write(data).await {
                return (StatusCode::OK, e.to_string());
            }

            count += 1;
        }

        (
            StatusCode::OK,
            format!("Restore Data Success, Total: {count} "),
        )
    }

    async fn debug(State(HttpServer { storage, .. }): State<HttpServer>) -> (StatusCode, String) {
        match storage.write().await.debug_data() {
            Ok(data) => (StatusCode::OK, data),
            Err(e) => {
                trace::error!("debug error: {:?}", e);
                (StatusCode::INTERNAL_SERVER_ERROR, e.to_string())
            }
        }
    }

    async fn debug_pprof() -> String {
        process_cpu_pprof().await
    }

    async fn debug_backtrace() -> String {
        utils::backtrace::backtrace()
    }

    async fn is_initialized(
        State(HttpServer { node, .. }): State<HttpServer>,
    ) -> (StatusCode, String) {
        match node.is_initialized().await {
            Ok(initialized) => (
                StatusCode::OK,
                format!("{{\"initialized\": {initialized}}}",),
            ),
            Err(e) => {
                trace::error!("Failed to check if raft is initialized: {e}");
                (StatusCode::INTERNAL_SERVER_ERROR, e.to_string())
            }
        }
    }
}

pub async fn process_watch(req: Bytes, storage: Arc<RwLock<StateMachine>>) -> MetaResult<String> {
    let req: (String, String, HashSet<String>, u64) = serde_json::from_slice(&req)?;
    let (client, cluster, tenants, base_ver) = req;
    trace::debug!(
        "watch all  args: client-id: {}, cluster: {}, tenants: {:?}, version: {}",
        client,
        cluster,
        tenants,
        base_ver
    );

    let mut notify = {
        let watch_data = storage
            .read()
            .await
            .read_change_logs(&cluster, &tenants, base_ver);
        if watch_data.need_return(base_ver) {
            return Ok(crate::store::storage::response_encode(Ok(watch_data)));
        }

        storage.read().await.watch.subscribe()
    };

    let mut follow_ver = base_ver;
    let now = std::time::Instant::now();
    loop {
        let _ = tokio::time::timeout(Duration::from_secs(20), notify.recv()).await;

        let watch_data = storage
            .read()
            .await
            .read_change_logs(&cluster, &tenants, follow_ver);
        trace::debug!("watch notify {} {}.{}", client, base_ver, follow_ver);
        if watch_data.need_return(base_ver) || now.elapsed() > Duration::from_secs(30) {
            return Ok(crate::store::storage::response_encode(Ok(watch_data)));
        }

        if follow_ver < watch_data.max_ver {
            follow_ver = watch_data.max_ver;
        }
    }
}

pub async fn process_watch_meta_membership(node: Arc<RaftNode>) -> MetaResult<String> {
    let nodes = node
        .raft_metrics()
        .membership_config
        .membership()
        .nodes()
        .map(|(_key, value)| value.address.clone())
        .collect::<Vec<String>>();

    Ok(crate::store::storage::response_encode(Ok(nodes)))
}

async fn process_cpu_pprof() -> String {
    #[cfg(unix)]
    {
        match utils::pprof_tools::generate_pprof().await {
            Ok(v) => v,
            Err(v) => v,
        }
    }
    #[cfg(not(unix))]
    {
        "/debug/pprof only supported on *unix systems.".to_string()
    }
}
