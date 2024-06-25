use std::net::SocketAddr;
use std::sync::Arc;

use config::tskv::TLSConfig;
use coordinator::service::CoordinatorRef;
use metrics::metric_register::MetricsRegister;
use protos::jaeger_storage_v1::span_reader_plugin_server::SpanReaderPluginServer;
use protos::jaeger_storage_v1::span_writer_plugin_server::SpanWriterPluginServer;
use spi::server::dbms::DBMSRef;
use tokio::sync::oneshot;
use tonic::transport::{Identity, Server, ServerTlsConfig};
use trace::http::tower_layer::TraceLayer;

use crate::jaeger::jaeger_read_server::JaegerReadService;
use crate::jaeger::jaeger_write_server::JaegerWriteService;
use crate::server::ServiceHandle;
use crate::spi::service::Service;
use crate::{info, server};

pub struct JaegerGrpcService {
    addr: SocketAddr,
    coord: CoordinatorRef,
    dbms: DBMSRef,
    tls_config: Option<TLSConfig>,
    metrics_register: Arc<MetricsRegister>,
    auto_generate_span: bool,
    handle: Option<ServiceHandle<Result<(), tonic::transport::Error>>>,
}

impl JaegerGrpcService {
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
        let trace_layer = TraceLayer::new($auto_generate_span, "grpc_jaeger");
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
impl Service for JaegerGrpcService {
    fn start(&mut self) -> server::Result<()> {
        let (shutdown, rx) = oneshot::channel();
        let jaeger_write_service = SpanWriterPluginServer::new(JaegerWriteService::new(
            self.coord.clone(),
            self.dbms.clone(),
        ));
        let jaeger_read_service =
            SpanReaderPluginServer::new(JaegerReadService::new(self.coord.clone()));
        let mut grpc_builder = build_grpc_server!(&self.tls_config, self.auto_generate_span);
        let grpc_router = grpc_builder
            .add_service(jaeger_write_service)
            .add_service(jaeger_read_service);
        let server = grpc_router.serve_with_shutdown(self.addr, async {
            rx.await.ok();
            info!("grpc_jaeger server graceful shutdown!");
        });
        info!("grpc_jaeger server start addr: {}", self.addr);
        let grpc_handle = tokio::spawn(server);
        self.handle = Some(ServiceHandle::new(
            "grpc_jaeger service".to_string(),
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
