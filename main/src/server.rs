use std::net::{SocketAddr, ToSocketAddrs};
use std::sync::Arc;
use std::time::Duration;

use coordinator::service::{CoordService, CoordinatorRef};
use memory_pool::MemoryPoolRef;
use meta::model::meta_admin::AdminMeta;
use meta::model::MetaRef;
use metrics::metric_register::MetricsRegister;
use models::auth::auth_cache::AuthCache;
use models::auth::user::User;
use models::utils::build_address;
use query::instance::make_cnosdbms;
use snafu::{Backtrace, Snafu};
use spi::server::dbms::DBMSRef;
use tokio::runtime::Runtime;
use tokio::sync::oneshot::Sender;
use tokio::task::JoinHandle;
use tokio::time;
use trace::error;
use trace_http::ctx::SpanContextExtractor;
use tskv::{EngineRef, TsKv};

use crate::flight_sql::FlightSqlServiceAdapter;
use crate::http::http_service::{HttpService, ServerMode};
use crate::meta_single::meta_service::MetaService;
use crate::rpc::grpc_service::GrpcService;
use crate::spi::service::ServiceRef;
use crate::tcp::tcp_service::TcpService;
use crate::vector::vector_grpc_service::VectorGrpcService;

pub type Result<T, E = Error> = std::result::Result<T, E>;

const DEFAULT_NODE_IP: &str = "0.0.0.0";

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Please inject DBMS.\nBacktrace:\n{}", backtrace))]
    NotFoundDBMS { backtrace: Backtrace },

    #[snafu(display("Ensure the format of certificate and private_key is correct."))]
    IdentityFormat,

    #[snafu(display("Ensure the TLS configuration is correct"))]
    TLSConfig,

    #[snafu(display("Server Common Error : {}", reason))]
    Common { reason: String },
}

impl From<tonic::transport::Error> for Error {
    fn from(_: tonic::transport::Error) -> Self {
        Self::IdentityFormat
    }
}

impl From<std::io::Error> for Error {
    fn from(_: std::io::Error) -> Self {
        Self::TLSConfig
    }
}

pub struct ServiceHandle<R> {
    pub name: String,
    join_handle: JoinHandle<R>,
    shutdown: Sender<()>,
}

impl<R> ServiceHandle<R> {
    pub fn new(name: String, join_handle: JoinHandle<R>, shutdown: Sender<()>) -> Self {
        ServiceHandle {
            name,
            join_handle,
            shutdown,
        }
    }
    pub async fn shutdown(self, force: bool) {
        if force {
            self.join_handle.abort();
            return;
        }
        let _ = self.shutdown.send(());
        let msg = format!("shutting down service {}", self.name);
        self.join_handle.await.expect(&msg);
    }
}

#[derive(Default)]
pub struct Server {
    services: Vec<ServiceRef>,
}

impl Server {
    pub fn add_service(&mut self, service: ServiceRef) {
        self.services.push(service);
    }

    pub fn start(&mut self) -> Result<()> {
        for x in self.services.iter_mut() {
            x.start().expect("service start");
        }
        Ok(())
    }

    pub async fn stop(&mut self, force: bool) {
        for x in self.services.iter_mut() {
            x.stop(force).await;
        }
    }
}

pub(crate) struct ServiceBuilder {
    pub cpu: usize,
    pub config: config::Config,
    pub runtime: Arc<Runtime>,
    pub memory_pool: MemoryPoolRef,
    pub metrics_register: Arc<MetricsRegister>,
    pub span_context_extractor: Arc<SpanContextExtractor>,
}

async fn regular_report_node_metrics(meta: MetaRef, heartbeat_interval: u64) {
    let mut interval = time::interval(Duration::from_secs(heartbeat_interval));

    loop {
        interval.tick().await;

        if let Err(e) = meta.report_node_metrics().await {
            error!("{}", e);
        }
    }
}

fn build_default_address(port: u16) -> String {
    build_address(DEFAULT_NODE_IP, port)
}

impl ServiceBuilder {
    pub async fn build_storage_server(&self, server: &mut Server) -> Option<EngineRef> {
        let meta = self.create_meta().await;
        meta.add_data_node().await.unwrap();
        tokio::spawn(regular_report_node_metrics(
            meta.clone(),
            self.config.heartbeat.report_time_interval_secs,
        ));

        let kv_inst = self
            .create_tskv(meta.clone(), self.runtime.clone(), self.memory_pool.clone())
            .await;
        let coord = self.create_coord(meta, Some(kv_inst.clone())).await;

        let auth_cache = Arc::new(AuthCache::new(1024, Some(Duration::from_secs(3600))));

        let dbms = self
            .create_dbms(coord.clone(), self.memory_pool.clone(), auth_cache.clone())
            .await;

        if let Some(http_service) =
            self.create_http_if_enabled(dbms.clone(), coord.clone(), ServerMode::Store, auth_cache)
        {
            server.add_service(Box::new(http_service));
        }

        if let Some(grpc_service) = self.create_grpc_if_enabled(kv_inst.clone(), coord.clone()) {
            server.add_service(Box::new(grpc_service));
        }

        if let Some(tcp_service) = self.create_tcp_if_enabled(coord.clone()) {
            server.add_service(Box::new(tcp_service));
        }

        if let Some(vector_service) =
            self.create_vector_grpc_if_enabled(coord.clone(), dbms.clone())
        {
            server.add_service(Box::new(vector_service));
        }

        Some(kv_inst)
    }

    pub async fn build_query_server(&self, server: &mut Server) -> Option<EngineRef> {
        let meta = self.create_meta().await;
        let coord = self.create_coord(meta, None).await;

        let auth_cache = Arc::new(AuthCache::new(1024, Some(Duration::from_secs(3600))));

        let dbms = self
            .create_dbms(coord.clone(), self.memory_pool.clone(), auth_cache.clone())
            .await;

        if let Some(http_service) = self.create_http_if_enabled(
            dbms.clone(),
            coord.clone(),
            ServerMode::Query,
            auth_cache.clone(),
        ) {
            server.add_service(Box::new(http_service));
        }

        if let Some(flight_sql_service) =
            self.create_flight_sql_if_enabled(dbms.clone(), auth_cache.clone())
        {
            server.add_service(Box::new(flight_sql_service));
        }

        None
    }

    pub async fn build_query_storage(&self, server: &mut Server) -> Option<EngineRef> {
        let meta = self.create_meta().await;
        meta.add_data_node().await.unwrap();
        tokio::spawn(regular_report_node_metrics(
            meta.clone(),
            self.config.heartbeat.report_time_interval_secs,
        ));

        let kv_inst = self
            .create_tskv(meta.clone(), self.runtime.clone(), self.memory_pool.clone())
            .await;
        let coord = self.create_coord(meta, Some(kv_inst.clone())).await;

        let auth_cache = Arc::new(AuthCache::new(1024, Some(Duration::from_secs(3600))));

        let dbms = self
            .create_dbms(coord.clone(), self.memory_pool.clone(), auth_cache.clone())
            .await;

        if let Some(http_service) = self.create_http_if_enabled(
            dbms.clone(),
            coord.clone(),
            ServerMode::Bundle,
            auth_cache.clone(),
        ) {
            server.add_service(Box::new(http_service));
        }

        if let Some(grpc_service) = self.create_grpc_if_enabled(kv_inst.clone(), coord.clone()) {
            server.add_service(Box::new(grpc_service));
        }

        if let Some(flight_sql_service) =
            self.create_flight_sql_if_enabled(dbms.clone(), auth_cache.clone())
        {
            server.add_service(Box::new(flight_sql_service));
        }

        if let Some(tcp_service) = self.create_tcp_if_enabled(coord.clone()) {
            server.add_service(Box::new(tcp_service));
        }

        if let Some(vector_service) =
            self.create_vector_grpc_if_enabled(coord.clone(), dbms.clone())
        {
            server.add_service(Box::new(vector_service));
        }

        Some(kv_inst)
    }

    pub async fn build_singleton(&self, server: &mut Server) -> Option<EngineRef> {
        let meta_service = MetaService::new(self.cpu, self.config.clone());
        meta_service.start().await.unwrap();

        // wait meta server ready!
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        self.build_query_storage(server).await
    }

    async fn create_meta(&self) -> MetaRef {
        let meta: MetaRef = AdminMeta::new(self.config.clone()).await;

        meta
    }

    async fn create_tskv(
        &self,
        meta: MetaRef,
        runtime: Arc<Runtime>,
        memory_pool: MemoryPoolRef,
    ) -> EngineRef {
        let options = tskv::Options::from(&self.config);
        let kv = TsKv::open(
            meta,
            options.clone(),
            runtime,
            memory_pool,
            self.metrics_register.clone(),
        )
        .await
        .unwrap();

        let kv: EngineRef = Arc::new(kv);

        kv
    }

    async fn create_dbms(
        &self,
        coord: CoordinatorRef,
        memory_pool: MemoryPoolRef,
        auth_cache: Arc<AuthCache<String, User>>,
    ) -> DBMSRef {
        let options = tskv::Options::from(&self.config);
        let dbms = make_cnosdbms(coord, options.clone(), memory_pool, auth_cache)
            .await
            .expect("make dbms");

        let dbms: DBMSRef = Arc::new(dbms);

        dbms
    }

    async fn create_coord(&self, meta: MetaRef, kv: Option<EngineRef>) -> CoordinatorRef {
        let _options = tskv::Options::from(&self.config);

        let coord: CoordinatorRef = CoordService::new(
            self.runtime.clone(),
            kv,
            meta,
            self.config.clone(),
            self.config.hinted_off.clone(),
            self.metrics_register.clone(),
        )
        .await;

        coord
    }

    fn create_http_if_enabled(
        &self,
        dbms: DBMSRef,
        coord: CoordinatorRef,
        mode: ServerMode,
        auth_cache: Arc<AuthCache<String, User>>,
    ) -> Option<HttpService> {
        let default_http_addr = match self.config.cluster.http_listen_port {
            Some(port) => build_default_address(port),
            None => return None,
        };

        let addr = default_http_addr
            .to_socket_addrs()
            .map_err(|e| {
                format!(
                    "Cannot resolve http_listen_addr '{}': {}",
                    default_http_addr, e
                )
            })
            .unwrap()
            .collect::<Vec<SocketAddr>>()
            .first()
            .copied()
            .expect("Config http_listen_addr cannot be empty.");

        Some(HttpService::new(
            dbms,
            coord,
            addr,
            self.config.security.tls_config.clone(),
            self.config.query.query_sql_limit,
            self.config.query.write_sql_limit,
            mode,
            self.metrics_register.clone(),
            self.span_context_extractor.clone(),
            auth_cache,
        ))
    }

    fn create_grpc_if_enabled(&self, kv: EngineRef, coord: CoordinatorRef) -> Option<GrpcService> {
        let default_grpc_addr = match self.config.cluster.grpc_listen_port {
            Some(port) => build_default_address(port),
            None => return None,
        };

        let addr = default_grpc_addr
            .to_socket_addrs()
            .map_err(|e| {
                format!(
                    "Cannot resolve grpc_listen_addr '{}': {}",
                    default_grpc_addr, e
                )
            })
            .unwrap()
            .collect::<Vec<SocketAddr>>()
            .first()
            .copied()
            .expect("Config grpc_listen_addr cannot be empty.");

        Some(GrpcService::new(
            self.runtime.clone(),
            kv,
            coord,
            addr,
            None,
            self.metrics_register.clone(),
            self.span_context_extractor.clone(),
        ))
    }

    fn create_vector_grpc_if_enabled(
        &self,
        coord: CoordinatorRef,
        dbms: DBMSRef,
    ) -> Option<VectorGrpcService> {
        let default_vector_grpc_addr = match self.config.cluster.vector_listen_port {
            Some(port) => build_default_address(port),
            None => return None,
        };

        let addr = default_vector_grpc_addr
            .to_socket_addrs()
            .map_err(|e| {
                format!(
                    "Cannot resolve vector_grpc_listen_addr '{}': {}",
                    default_vector_grpc_addr, e
                )
            })
            .unwrap()
            .collect::<Vec<SocketAddr>>()
            .first()
            .copied()
            .expect("Config vector_grpc_listen_addr cannot be empty.");

        Some(VectorGrpcService::new(
            coord,
            dbms,
            addr,
            None,
            self.metrics_register.clone(),
            self.span_context_extractor.clone(),
        ))
    }

    fn create_tcp_if_enabled(&self, coord: CoordinatorRef) -> Option<TcpService> {
        let default_tcp_addr = match self.config.cluster.tcp_listen_port {
            Some(port) => build_default_address(port),
            None => return None,
        };

        Some(TcpService::new(coord, default_tcp_addr))
    }

    fn create_flight_sql_if_enabled(
        &self,
        dbms: DBMSRef,
        auth_cache: Arc<AuthCache<String, User>>,
    ) -> Option<FlightSqlServiceAdapter> {
        let default_flight_sql_addr = match self.config.cluster.flight_rpc_listen_port {
            Some(port) => build_default_address(port),
            None => return None,
        };
        let tls_config = self.config.security.tls_config.clone();

        let addr = default_flight_sql_addr
            .to_socket_addrs()
            .map_err(|e| {
                format!(
                    "Cannot resolve flight_rpc_listen_addr '{}': {}",
                    default_flight_sql_addr, e
                )
            })
            .unwrap()
            .collect::<Vec<SocketAddr>>()
            .first()
            .copied()
            .expect("Config flight_rpc_listen_addr cannot be empty.");

        Some(FlightSqlServiceAdapter::new(
            dbms,
            addr,
            tls_config,
            self.span_context_extractor.clone(),
            auth_cache,
        ))
    }
}
