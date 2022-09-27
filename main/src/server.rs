use snafu::Backtrace;
use tokio::sync::oneshot::Sender;
use tokio::task::JoinHandle;

use snafu::Snafu;

pub type Result<T, E = Error> = std::result::Result<T, E>;
#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Please inject DBMS.\nBacktrace:\n{}", backtrace))]
    NotFoundDBMS { backtrace: Backtrace },
    // #[snafu(display("Failed to start service. err: {}", source))]
    // StartService { source: query::spi::server::ServerError },
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

pub struct Server {
    services: Vec<ServiceRef>,
}

impl Server {
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

#[derive(Default)]
pub struct Builder {
    // service
    services: Vec<ServiceRef>,
}

impl Builder {
    pub fn with_services(self, services: Vec<ServiceRef>) -> Self {
        Self { services }
    }

    pub fn add_service(mut self, service: ServiceRef) -> Self {
        self.services.push(service);
        self
    }

    pub fn build(self) -> Result<Server> {
        Ok(Server {
            services: self.services,
        })
    }
}
