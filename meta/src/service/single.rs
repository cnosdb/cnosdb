use std::collections::HashSet;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use axum::body::Bytes;
use config::tskv::MetaConfig;
use models::schema::database_schema::{DatabaseConfig, DatabaseOptions};
use models::schema::DEFAULT_DATABASE;
use tokio::net::TcpListener;
use tokio::sync::RwLock;

use crate::error::MetaResult;
use crate::service::init::MetaInit;
use crate::store::storage::StateMachine;

pub async fn start_singe_meta_server<P: AsRef<Path>>(
    dir: P,
    cluster_name: String,
    config: &MetaConfig,
    size: usize,
) {
    trace::info!("CnosDB meta config: {:?}", config);

    let listen_addr = &config.service_addr[0];
    let listener = TcpListener::bind(listen_addr).await.unwrap();

    let db_path = dir.as_ref().join("meta").join("0.data");
    let mut storage = StateMachine::open(db_path, size).unwrap();

    let mut usage_schema_config = DatabaseConfig::default();
    usage_schema_config.set_max_memcache_size(config.usage_schema_cache_size);
    let mut cluster_schema_config = DatabaseConfig::default();
    cluster_schema_config.set_max_memcache_size(config.cluster_schema_cache_size);
    let default_database = vec![
        (
            String::from(DEFAULT_DATABASE),
            DatabaseConfig::default(),
            DatabaseOptions::default(),
        ),
        (
            String::from(models::schema::USAGE_SCHEMA),
            usage_schema_config,
            DatabaseOptions::default(),
        ),
        (
            String::from(models::schema::CLUSTER_SCHEMA),
            cluster_schema_config,
            DatabaseOptions::default(),
        ),
    ];

    let meta_init = MetaInit::new(
        cluster_name,
        models::auth::user::ROOT.to_string(),
        models::auth::user::ROOT_PWD.to_string(),
        models::schema::DEFAULT_CATALOG.to_string(),
        default_database,
    );
    meta_init.init_meta(&mut storage).await;

    trace::info!("single meta http server start addr: {listen_addr}");

    let server = SingleServer {
        addr: listen_addr.to_string(),
        storage: Arc::new(RwLock::new(storage)),
    };
    tokio::spawn(async move { axum::serve(listener, http_api::create_router(server)) });
}

#[derive(Clone)]
pub struct SingleServer {
    pub addr: String,
    pub storage: Arc<RwLock<StateMachine>>,
}

mod http_api {
    use std::fmt::Write as _;

    use axum::body::Bytes;
    use axum::extract::{Json, Path, State};
    use axum::http::StatusCode;
    use axum::{routing, Router};

    use super::{process_watch, SingleServer};
    use crate::error::MetaError;
    use crate::store::command::{ReadCommand, WriteCommand};
    use crate::store::dump::dump_impl;

    pub fn create_router(server: SingleServer) -> Router {
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
            .with_state(server)
    }

    async fn read(
        State(SingleServer { storage, .. }): State<SingleServer>,
        Json(cmd): Json<ReadCommand>,
    ) -> String {
        storage.read().await.process_read_command(&cmd)
    }

    async fn write(
        State(SingleServer { storage, .. }): State<SingleServer>,
        Json(cmd): Json<WriteCommand>,
    ) -> String {
        storage.write().await.process_write_command(&cmd).await
    }

    async fn watch(
        State(SingleServer { storage, .. }): State<SingleServer>,
        body: Bytes,
    ) -> (StatusCode, String) {
        match process_watch(body, storage).await {
            Ok(data) => (StatusCode::OK, data),
            Err(e) => {
                trace::error!("watch error: {:?}", e);
                (StatusCode::INTERNAL_SERVER_ERROR, e.to_string())
            }
        }
    }

    async fn watch_meta_membership(
        State(SingleServer { addr, .. }): State<SingleServer>,
    ) -> String {
        let nodes = vec![addr];
        crate::store::storage::response_encode(Ok(nodes))
    }

    async fn dump(
        State(SingleServer { storage, .. }): State<SingleServer>,
    ) -> (StatusCode, String) {
        match storage.write().await.backup().map_err(MetaError::from) {
            Ok(data) => {
                let mut buf = String::with_capacity(8 * data.map.len());
                for (key, val) in data.map.iter() {
                    write!(&mut buf, "{key}: {val}\n").unwrap();
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
        State(SingleServer { storage, .. }): State<SingleServer>,
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
        State(SingleServer { storage, .. }): State<SingleServer>,
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

            let _ = storage.write().await.process_write_command(&command).await;

            count += 1;
        }

        (
            StatusCode::OK,
            format!("Restore Data Success, Total: {count} "),
        )
    }

    async fn debug(
        State(SingleServer { storage, .. }): State<SingleServer>,
    ) -> (StatusCode, String) {
        match storage.write().await.debug_data() {
            Ok(data) => (StatusCode::OK, data),
            Err(e) => {
                trace::error!("debug error: {e:?}");
                (StatusCode::INTERNAL_SERVER_ERROR, e.to_string())
            }
        }
    }
}

pub async fn process_watch(req: Bytes, storage: Arc<RwLock<StateMachine>>) -> MetaResult<String> {
    let req: (String, String, HashSet<String>, u64) = serde_json::from_slice(&req)?;
    let (client, cluster, tenants, base_ver) = req;
    trace::debug!(
        "watch all  args: client-id: {client}, cluster: {cluster}, tenants: {tenants:?}, version: {base_ver}",
    );

    let mut notify = {
        let storage = storage.read().await;
        let watch_data = storage.read_change_logs(&cluster, &tenants, base_ver);
        if watch_data.need_return(base_ver) {
            return Ok(crate::store::storage::response_encode(Ok(watch_data)));
        }

        storage.watch.subscribe()
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
