use std::{net::SocketAddr, sync::Arc};

use arrow_flight::flight_service_server::FlightServiceServer;
use spi::server::dbms::DBMSRef;
use tokio::sync::oneshot;
use tonic::transport::Server;
use trace::info;
use warp::trace::Info;

use crate::server::{Service, ServiceHandle};

use self::flight_sql_server::FlightSqlServiceImpl;

pub mod flight_sql_server;
mod utils;

pub struct FlightSqlServiceAdapter {
    dbms: DBMSRef,

    addr: SocketAddr,
    handle: Option<ServiceHandle<Result<(), tonic::transport::Error>>>,
}

impl FlightSqlServiceAdapter {
    pub fn new(dbms: DBMSRef, addr: SocketAddr) -> Self {
        Self {
            dbms,
            addr,
            handle: None,
        }
    }
}

#[async_trait::async_trait]
impl Service for FlightSqlServiceAdapter {
    fn start(&mut self) -> crate::server::Result<()> {
        let (shutdown, rx) = oneshot::channel();

        let svc = FlightServiceServer::new(FlightSqlServiceImpl::new(self.dbms.clone()));
        let server = Server::builder()
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
