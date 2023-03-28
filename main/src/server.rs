use std::net::{SocketAddr, ToSocketAddrs};
use std::sync::Arc;
use std::time::Duration;

use coordinator::service::{CoordService, CoordinatorRef};
use memory_pool::MemoryPoolRef;
use meta::error::MetaResult;
use meta::model::meta_manager::RemoteMetaManager;
use meta::model::{MetaManager, MetaRef};
use metrics::metric_register::MetricsRegister;
use query::instance::make_cnosdbms;
use snafu::{Backtrace, Snafu};
use spi::server::dbms::DBMSRef;
use tokio::runtime::Runtime;
use tokio::sync::oneshot::Sender;
use tokio::task::JoinHandle;
use tokio::time;
use tskv::{EngineRef, TsKv};

use crate::flight_sql::FlightSqlServiceAdapter;
use crate::http::http_service::{HttpService, ServerMode};
use crate::meta_single::meta_service::MetaService;
use crate::rpc::grpc_service::GrpcService;
use crate::spi::service::ServiceRef;
use crate::tcp::tcp_service::TcpService;

pub type Result<T, E = Error> = std::result::Result<T, E>;

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
}

#[allow(unreachable_code)]
async fn regualar_get_disk_info(meta: Arc<dyn MetaManager>) -> MetaResult<()> {
    let mut interval = time::interval(Duration::from_secs(300));

    loop {
        interval.tick().await;

        meta.admin_meta().add_data_node().await.unwrap();
    }

    Ok(())
}

impl ServiceBuilder {
    pub async fn build_storage_server(&self, server: &mut Server) -> Option<EngineRef> {
        let meta = self.create_meta().await;

        tokio::spawn(regualar_get_disk_info(meta.clone()));

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

        tokio::spawn(regualar_get_disk_info(meta.clone()));

        let kv_inst = self
            .create_tskv(meta.clone(), self.runtime.clone(), self.memory_pool.clone())
            .await;
        let coord = self.create_coord(meta, Some(kv_inst.clone())).await;
        let dbms = self
            .create_dbms(coord.clone(), self.memory_pool.clone())
            .await;
        let flight_sql_service = Box::new(self.create_flight_sql(dbms.clone()));
        let grpc_service = Box::new(self.create_grpc(kv_inst.clone(), coord.clone()));
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
        let meta_service = MetaService::new(self.cpu, self.config.clone());
        meta_service.start().await.unwrap();
        self.build_query_storage(server).await
    }

    async fn create_meta(&self) -> MetaRef {
        let meta: MetaRef = RemoteMetaManager::new(
            self.config.cluster.clone(),
            self.config.storage.path.clone(),
        )
        .await;

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
            self.config.cluster.clone(),
            self.config.hinted_off.clone(),
            self.metrics_register.clone(),
        )
        .await;

        coord
    }

    fn create_http(&self, dbms: DBMSRef, coord: CoordinatorRef, mode: ServerMode) -> HttpService {
        let addr = self
            .config
            .cluster
            .http_listen_addr
            .to_socket_addrs()
            .map_err(|e| {
                format!(
                    "Cannot resolve http_listen_addr '{}': {}",
                    self.config.cluster.http_listen_addr, e
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
        )
    }

    fn create_grpc(&self, kv: EngineRef, coord: CoordinatorRef) -> GrpcService {
        let addr = self
            .config
            .cluster
            .grpc_listen_addr
            .to_socket_addrs()
            .map_err(|e| {
                format!(
                    "Cannot resolve grpc_listen_addr '{}': {}",
                    self.config.cluster.grpc_listen_addr, e
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
            self.config.security.tls_config.clone(),
            self.metrics_register.clone(),
        )
    }

    fn create_tcp(&self, coord: CoordinatorRef) -> TcpService {
        TcpService::new(coord, self.config.cluster.tcp_listen_addr.clone())
    }

    fn create_flight_sql(&self, dbms: DBMSRef) -> FlightSqlServiceAdapter {
        let tls_config = self.config.security.tls_config.clone();
        let addr = self
            .config
            .cluster
            .flight_rpc_listen_addr
            .to_socket_addrs()
            .map_err(|e| {
                format!(
                    "Cannot resolve flight_rpc_listen_addr '{}': {}",
                    self.config.cluster.flight_rpc_listen_addr, e
                )
            })
            .unwrap()
            .collect::<Vec<SocketAddr>>()
            .first()
            .copied()
            .expect("Config flight_rpc_listen_addr cannot be empty.");

        FlightSqlServiceAdapter::new(dbms, addr, tls_config)
    }
}
