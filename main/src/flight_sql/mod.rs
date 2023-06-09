use std::net::SocketAddr;
use std::sync::Arc;

use arrow_flight::flight_service_server::FlightServiceServer;
use config::TLSConfig;
use spi::server::dbms::DBMSRef;
use tokio::sync::oneshot;
use tonic::transport::{Identity, Server, ServerTlsConfig};
use trace::{info, TraceExporter};
use trace_http::ctx::TraceHeaderParser;
use trace_http::tower_layer::TraceLayer;

use self::flight_sql_server::FlightSqlServiceImpl;
use crate::flight_sql::auth_middleware::basic_call_header_authenticator::BasicCallHeaderAuthenticator;
use crate::flight_sql::auth_middleware::generated_bearer_token_authenticator::GeneratedBearerTokenAuthenticator;
use crate::server::ServiceHandle;
use crate::spi::service::Service;

mod auth_middleware;
pub mod flight_sql_server;
mod utils;

pub struct FlightSqlServiceAdapter {
    dbms: DBMSRef,

    addr: SocketAddr,
    tls_config: Option<TLSConfig>,
    trace_collector: Option<Arc<dyn TraceExporter>>,
    handle: Option<ServiceHandle<Result<(), tonic::transport::Error>>>,
}

impl FlightSqlServiceAdapter {
    pub fn new(
        dbms: DBMSRef,
        addr: SocketAddr,
        tls_config: Option<TLSConfig>,
        trace_collector: Option<Arc<dyn TraceExporter>>,
    ) -> Self {
        Self {
            dbms,
            addr,
            tls_config,
            trace_collector,
            handle: None,
        }
    }
}

#[async_trait::async_trait]
impl Service for FlightSqlServiceAdapter {
    fn start(&mut self) -> crate::server::Result<()> {
        let (shutdown, rx) = oneshot::channel();

        let server = Server::builder();

        let server = if let Some(TLSConfig {
            certificate,
            private_key,
        }) = self.tls_config.as_ref()
        {
            let cert = std::fs::read(certificate)?;
            let key = std::fs::read(private_key)?;
            let identity = Identity::from_pem(cert, key);
            server.tls_config(ServerTlsConfig::new().identity(identity))?
        } else {
            server
        };

        let trace_layer = TraceLayer::new(
            TraceHeaderParser::new(),
            self.trace_collector.clone(),
            "flight sql",
        );

        let authenticator = GeneratedBearerTokenAuthenticator::new(
            BasicCallHeaderAuthenticator::new(self.dbms.clone()),
        );
        let svc =
            FlightServiceServer::new(FlightSqlServiceImpl::new(self.dbms.clone(), authenticator));

        let server = server
            .layer(trace_layer)
            .add_service(svc)
            .serve_with_shutdown(self.addr, async {
                rx.await.ok();
                info!("flight rpc server graceful shutdown!");
            });

        let handle = tokio::spawn(server);
        self.handle = Some(ServiceHandle::new(
            "flight rpc service".to_string(),
            handle,
            shutdown,
        ));

        info!("flight rpc server start addr: {}", self.addr);

        Ok(())
    }

    async fn stop(&mut self, force: bool) {
        if let Some(stop) = self.handle.take() {
            stop.shutdown(force).await
        };
    }
}
