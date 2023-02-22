use std::net::SocketAddr;
use std::sync::Arc;

use coordinator::service::{CoordService, CoordinatorRef};
use meta::meta_manager::RemoteMetaManager;
use meta::MetaRef;
use query::instance::make_cnosdbms;
use snafu::{Backtrace, Snafu};
use spi::server::dbms::DBMSRef;
use tokio::runtime::Runtime;
use tokio::sync::oneshot::Sender;
use tokio::task::JoinHandle;
use tskv::engine::EngineRef;
use tskv::TsKv;

use crate::flight_sql::FlightSqlServiceAdapter;
use crate::http::http_service::{HttpService, ServerMode};
use crate::rpc::grpc_service::GrpcService;
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

pub type ServiceRef = Box<dyn Service + Send + Sync>;
#[async_trait::async_trait]
pub trait Service {
    fn start(&mut self) -> Result<()>;
    async fn stop(&mut self, force: bool);
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
    pub config: config::Config,
    pub runtime: Arc<Runtime>,
}

impl ServiceBuilder {
    pub async fn build_stroage_server(&self, server: &mut Server) -> Option<EngineRef> {
        let meta = self.create_meta().await;
        meta.admin_meta().add_data_node().await.unwrap();

        let kv_inst = self.create_tskv(meta.clone(), self.runtime.clone()).await;
        let coord = self.create_coord(meta, Some(kv_inst.clone())).await;
        let dbms = self.create_dbms(coord.clone()).await;
        let tcp_service = Box::new(self.create_tcp(coord.clone()).await);
        let http_service = Box::new(
            self.create_http(dbms.clone(), coord.clone(), ServerMode::Store)
                .await,
        );
        let grpc_service = Box::new(self.create_grpc(kv_inst.clone()).await);

        server.add_service(http_service);
        server.add_service(grpc_service);
        server.add_service(tcp_service);

        Some(kv_inst)
    }

    pub async fn build_query_server(&self, server: &mut Server) -> Option<EngineRef> {
        let meta = self.create_meta().await;
        let coord = self.create_coord(meta, None).await;
        let dbms = self.create_dbms(coord.clone()).await;
        let http_service = Box::new(
            self.create_http(dbms.clone(), coord.clone(), ServerMode::Query)
                .await,
        );
        let flight_sql_service = Box::new(self.create_flight_sql(dbms.clone()).await);

        server.add_service(http_service);
        server.add_service(flight_sql_service);

        None
    }

    pub async fn build_query_stroage(&self, server: &mut Server) -> Option<EngineRef> {
        let meta = self.create_meta().await;
        meta.admin_meta().add_data_node().await.unwrap();

        let kv_inst = self.create_tskv(meta.clone(), self.runtime.clone()).await;
        let coord = self.create_coord(meta, Some(kv_inst.clone())).await;
        let dbms = self.create_dbms(coord.clone()).await;
        let tcp_service = Box::new(self.create_tcp(coord.clone()).await);
        let flight_sql_service = Box::new(self.create_flight_sql(dbms.clone()).await);
        let grpc_service = Box::new(self.create_grpc(kv_inst.clone()).await);
        let http_service = Box::new(
            self.create_http(dbms.clone(), coord.clone(), ServerMode::Bundle)
                .await,
        );

        server.add_service(http_service);
        server.add_service(grpc_service);
        server.add_service(tcp_service);
        server.add_service(flight_sql_service);

        Some(kv_inst)
    }

    async fn create_meta(&self) -> MetaRef {
        let meta: MetaRef = RemoteMetaManager::new(self.config.cluster.clone()).await;

        meta
    }

    async fn create_tskv(&self, meta: MetaRef, runtime: Arc<Runtime>) -> EngineRef {
        let options = tskv::Options::from(&self.config);
        let kv = TsKv::open(meta, options.clone(), runtime).await.unwrap();

        let kv: EngineRef = Arc::new(kv);

        kv
    }

    async fn create_dbms(&self, coord: CoordinatorRef) -> DBMSRef {
        let options = tskv::Options::from(&self.config);
        let dbms = make_cnosdbms(coord, options.clone())
            .await
            .expect("make dbms");

        let dbms: DBMSRef = Arc::new(dbms);

        dbms
    }

    async fn create_coord(&self, meta: MetaRef, kv: Option<EngineRef>) -> CoordinatorRef {
        let options = tskv::Options::from(&self.config);

        let coord: CoordinatorRef = CoordService::new(
            kv,
            meta,
            self.config.cluster.clone(),
            self.config.hintedoff.clone(),
        )
        .await;

        coord
    }

    async fn create_tcp(&self, coord: CoordinatorRef) -> TcpService {
        let host = self
            .config
            .cluster
            .tcp_listen_addr
            .parse::<SocketAddr>()
            .expect("Invalid tcp host");

        TcpService::new(coord, host)
    }

    async fn create_http(
        &self,
        dbms: DBMSRef,
        coord: CoordinatorRef,
        mode: ServerMode,
    ) -> HttpService {
        let tls_config = self.config.security.tls_config.clone();
        let host = self
            .config
            .cluster
            .http_listen_addr
            .parse::<SocketAddr>()
            .expect("Invalid tcp host");

        HttpService::new(
            dbms,
            coord,
            host,
            tls_config,
            self.config.query.query_sql_limit,
            self.config.query.write_sql_limit,
            mode,
        )
    }

    async fn create_grpc(&self, kv: EngineRef) -> GrpcService {
        let tls_config = self.config.security.tls_config.clone();
        let host = self
            .config
            .cluster
            .grpc_listen_addr
            .parse::<SocketAddr>()
            .expect("Invalid tcp host");

        GrpcService::new(kv, host, tls_config)
    }

    async fn create_flight_sql(&self, dbms: DBMSRef) -> FlightSqlServiceAdapter {
        let tls_config = self.config.security.tls_config.clone();
        let host = self
            .config
            .cluster
            .flight_rpc_listen_addr
            .parse::<SocketAddr>()
            .expect("Invalid tcp host");

        FlightSqlServiceAdapter::new(dbms, host, tls_config)
    }
}
