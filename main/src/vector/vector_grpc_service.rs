use std::net::SocketAddr;
use std::sync::Arc;

use config::tskv::TLSConfig;
use coordinator::service::CoordinatorRef;
use metrics::metric_register::MetricsRegister;
use protos::vector::vector_server::VectorServer;
use spi::server::dbms::DBMSRef;
use tokio::sync::oneshot;
use tonic::transport::{Identity, Server, ServerTlsConfig};
use trace::http::tower_layer::TraceLayer;

use crate::server::ServiceHandle;
use crate::spi::service::{Service, ServieceType};
use crate::vector::vector_server::VectorService;
use crate::{info, server};

pub struct VectorGrpcService {
    addr: SocketAddr,
    coord: CoordinatorRef,
    dbms: DBMSRef,
    tls_config: Option<TLSConfig>,
    metrics_register: Arc<MetricsRegister>,
    auto_generate_span: bool,
    handle: Option<ServiceHandle<Result<(), tonic::transport::Error>>>,
}

impl VectorGrpcService {
    pub fn new(
        coord: CoordinatorRef,
        dbms: DBMSRef,
        addr: SocketAddr,
        tls_config: Option<TLSConfig>,
        metrics_register: Arc<MetricsRegister>,
        auto_generate_span: bool,
    ) -> Self {
        Self {
            addr,
            coord,
            dbms,
            tls_config,
            metrics_register,
            auto_generate_span,
            handle: None,
        }
    }
}

macro_rules! build_grpc_server {
    ($tls_config:expr, $auto_generate_span:expr) => {{
        let trace_layer = TraceLayer::new($auto_generate_span, "grpc_vector");
        let mut server = Server::builder().layer(trace_layer);

        if let Some(TLSConfig {
            certificate,
            private_key,
        }) = $tls_config
        {
            let cert = std::fs::read(certificate)?;
            let key = std::fs::read(private_key)?;
            let identity = Identity::from_pem(cert, key);
            server = server.tls_config(ServerTlsConfig::new().identity(identity))?;
        }

        server
    }};
}

#[async_trait::async_trait]
impl Service for VectorGrpcService {
    fn start(&mut self) -> server::Result<()> {
        let (shutdown, rx) = oneshot::channel();
        let vector_service =
            VectorServer::new(VectorService::new(self.coord.clone(), self.dbms.clone()));
        let mut grpc_builder = build_grpc_server!(&self.tls_config, self.auto_generate_span);
        let grpc_router = grpc_builder.add_service(vector_service);
        let server = grpc_router.serve_with_shutdown(self.addr, async {
            rx.await.ok();
            info!("grpc_vector server graceful shutdown!");
        });
        info!("grpc_vector server start addr: {}", self.addr);
        let grpc_handle = tokio::spawn(server);
        self.handle = Some(ServiceHandle::new(
            "grpc_vector service".to_string(),
            grpc_handle,
            shutdown,
        ));
        Ok(())
    }

    async fn stop(&mut self, force: bool) {
        if let Some(stop) = self.handle.take() {
            stop.shutdown(force).await
        };
    }
    fn get_coord(&self) -> CoordinatorRef {
        self.coord.clone()
    }
    fn get_type(&self) -> ServieceType {
        ServieceType::VectorGrpcService
    }
}
