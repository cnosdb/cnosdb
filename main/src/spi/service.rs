use crate::server;

pub type ServiceRef = Box<dyn Service + Send + Sync>;

#[async_trait::async_trait]
pub trait Service {
    fn start(&mut self) -> server::Result<()>;
    async fn stop(&mut self, force: bool);
}
