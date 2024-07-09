use coordinator::service::CoordinatorRef;

use crate::server;

pub type ServiceRef = Box<dyn Service + Send + Sync>;

#[async_trait::async_trait]
pub trait Service {
    fn start(&mut self) -> server::Result<()>;
    async fn stop(&mut self, force: bool);
    fn get_coord(&self) -> CoordinatorRef;
    fn get_type(&self) -> ServieceType;
}

pub enum ServieceType {
    ReportService,
    HttpService,
    RpcService,
    FlightSqlService,
    TcpService,
    VectorGrpcService,
}
