use std::net::SocketAddr;
use std::sync::Arc;

use config::TLSConfig;
use coordinator::service::CoordinatorRef;
use metrics::metric_register::MetricsRegister;
use protos::kv_service::tskv_service_server::TskvServiceServer;
use spi::server::dbms::DBMSRef;
use tokio::sync::oneshot;
use tonic::transport::{Identity, Server, ServerTlsConfig};
use tskv::engine::EngineRef;

use crate::rpc::tskv::TskvServiceImpl;
use crate::server::{Service, ServiceHandle};
use crate::{info, server};

pub struct GrpcService {
    addr: SocketAddr,
    kv_inst: EngineRef,
    coord: CoordinatorRef,
    tls_config: Option<TLSConfig>,
    metrics_register: Arc<MetricsRegister>,
    handle: Option<ServiceHandle<Result<(), tonic::transport::Error>>>,
}

impl GrpcService {
    pub fn new(
        kv_inst: EngineRef,
        coord: CoordinatorRef,
        addr: SocketAddr,
        tls_config: Option<TLSConfig>,
        metrics_register: Arc<MetricsRegister>,
    ) -> Self {
        Self {
            addr,
            coord,
            kv_inst,
            tls_config,
            metrics_register,
            handle: None,
        }
    }
}

fn build_grpc_server(tls_config: &Option<TLSConfig>) -> server::Result<Server> {
    let server = Server::builder();
    if tls_config.is_none() {
        return Ok(server);
    }

    let TLSConfig {
        certificate,
        private_key,
    } = tls_config.as_ref().unwrap();
    let cert = std::fs::read(certificate)?;
    let key = std::fs::read(private_key)?;
    let identity = Identity::from_pem(cert, key);
    let server = server.tls_config(ServerTlsConfig::new().identity(identity))?;

    Ok(server)
}

#[async_trait::async_trait]
impl Service for GrpcService {
    fn start(&mut self) -> server::Result<()> {
        let (shutdown, rx) = oneshot::channel();
        let tskv_grpc_service = TskvServiceServer::new(TskvServiceImpl {
            kv_inst: self.kv_inst.clone(),
            coord: self.coord.clone(),
            metrics_register: self.metrics_register.clone(),
        });
        let mut grpc_builder = build_grpc_server(&self.tls_config)?;
        let grpc_router = grpc_builder.add_service(tskv_grpc_service);
        let server = grpc_router.serve_with_shutdown(self.addr, async {
            rx.await.ok();
            info!("grpc server graceful shutdown!");
        });
        info!("grpc server start addr: {}", self.addr);
        let grpc_handle = tokio::spawn(server);
        self.handle = Some(ServiceHandle::new(
            "grpc service".to_string(),
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
}
