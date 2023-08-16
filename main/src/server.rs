use std::net::{SocketAddr, ToSocketAddrs};
use std::sync::Arc;
use std::time::Duration;

use coordinator::service::{CoordService, CoordinatorRef};
use memory_pool::MemoryPoolRef;
use meta::model::meta_admin::AdminMeta;
use meta::model::MetaRef;
use metrics::metric_register::MetricsRegister;
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
    IdentityFormatError,

    #[snafu(display("Ensure the TLS configuration is correct"))]
    TLSConfigError,

    #[snafu(display("Server Common Error : {}", reason))]
    Common { reason: String },
}

impl From<tonic::transport::Error> for Error {
    fn from(_: tonic::transport::Error) -> Self {
        Self::IdentityFormatError
    }
}

impl From<std::io::Error> for Error {
    fn from(_: std::io::Error) -> Self {
        Self::TLSConfigError
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
    build_address(DEFAULT_NODE_IP.to_owned(), port)
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
        let dbms = self
            .create_dbms(coord.clone(), self.memory_pool.clone())
            .await;
        let http_service =
            Box::new(self.create_http(dbms.clone(), coord.clone(), ServerMode::Store));
        let grpc_service = Box::new(self.create_grpc(kv_inst.clone(), coord.clone()));
        if let Some(port) = self.config.cluster.vector_listen_port {
            let vector_service =
                Box::new(self.create_vector_grpc(coord.clone(), dbms.clone(), port));
            server.add_service(vector_service);
        }
        let tcp_service = Box::new(self.create_tcp(coord.clone()));

        server.add_service(http_service);
        server.add_service(grpc_service);
        server.add_service(tcp_service);

        Some(kv_inst)
    }

    pub async fn build_query_server(&self, server: &mut Server) -> Option<EngineRef> {
        let meta = self.create_meta().await;
        let coord = self.create_coord(meta, None).await;
        let dbms = self
            .create_dbms(coord.clone(), self.memory_pool.clone())
            .await;
        let http_service =
            Box::new(self.create_http(dbms.clone(), coord.clone(), ServerMode::Query));
        let flight_sql_service = Box::new(self.create_flight_sql(dbms.clone()));

        server.add_service(http_service);
        server.add_service(flight_sql_service);

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
        let dbms = self
            .create_dbms(coord.clone(), self.memory_pool.clone())
            .await;
        let flight_sql_service = Box::new(self.create_flight_sql(dbms.clone()));
        let grpc_service = Box::new(self.create_grpc(kv_inst.clone(), coord.clone()));
        if let Some(port) = self.config.cluster.vector_listen_port {
            let vector_service =
                Box::new(self.create_vector_grpc(coord.clone(), dbms.clone(), port));
            server.add_service(vector_service);
        }
        let http_service =
            Box::new(self.create_http(dbms.clone(), coord.clone(), ServerMode::Bundle));
        let tcp_service = Box::new(self.create_tcp(coord.clone()));

        server.add_service(http_service);
        server.add_service(grpc_service);
        server.add_service(flight_sql_service);
        server.add_service(tcp_service);

        Some(kv_inst)
    }

    pub async fn build_singleton(&self, server: &mut Server) -> Option<EngineRef> {
        meta::service::single::start_singe_meta_server(
            self.config.storage.path.clone(),
            self.config.cluster.name.clone(),
            self.config.cluster.meta_service_addr[0].clone(),
        )
        .await;

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

    async fn create_dbms(&self, coord: CoordinatorRef, memory_pool: MemoryPoolRef) -> DBMSRef {
        let options = tskv::Options::from(&self.config);
        let dbms = make_cnosdbms(coord, options.clone(), memory_pool)
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

    fn create_http(&self, dbms: DBMSRef, coord: CoordinatorRef, mode: ServerMode) -> HttpService {
        let default_http_addr = build_default_address(self.config.cluster.http_listen_port);

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

        HttpService::new(
            dbms,
            coord,
            addr,
            self.config.security.tls_config.clone(),
            self.config.query.query_sql_limit,
            self.config.query.write_sql_limit,
            mode,
            self.metrics_register.clone(),
            self.span_context_extractor.clone(),
        )
    }

    fn create_grpc(&self, kv: EngineRef, coord: CoordinatorRef) -> GrpcService {
        let default_grpc_addr = build_default_address(self.config.cluster.grpc_listen_port);

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

        GrpcService::new(
            self.runtime.clone(),
            kv,
            coord,
            addr,
            None,
            self.metrics_register.clone(),
            self.span_context_extractor.clone(),
        )
    }

    fn create_vector_grpc(
        &self,
        coord: CoordinatorRef,
        dbms: DBMSRef,
        port: u16,
    ) -> VectorGrpcService {
        let default_vector_grpc_addr = build_default_address(port);

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

        VectorGrpcService::new(
            coord,
            dbms,
            addr,
            None,
            self.metrics_register.clone(),
            self.span_context_extractor.clone(),
        )
    }

    fn create_tcp(&self, coord: CoordinatorRef) -> TcpService {
        let default_tcp_addr = build_default_address(self.config.cluster.tcp_listen_port);

        TcpService::new(coord, default_tcp_addr)
    }

    fn create_flight_sql(&self, dbms: DBMSRef) -> FlightSqlServiceAdapter {
        let tls_config = self.config.security.tls_config.clone();
        let default_flight_sql_addr =
            build_default_address(self.config.cluster.flight_rpc_listen_port);

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

        FlightSqlServiceAdapter::new(dbms, addr, tls_config, self.span_context_extractor.clone())
    }
}
