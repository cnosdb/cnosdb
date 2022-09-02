use config::Config;
use snafu::ResultExt;
use spi::{server::dbms::DBMSRef, service::ServiceRef};

use snafu::OptionExt;

use super::NotFoundDBMSSnafu;
use super::Result;
use super::StartServiceSnafu;

pub struct Server {
    // service
    services: Vec<ServiceRef>,
    // todo node manager
    // todo security(eg. ssl、kerberos、others)
    // instance
    dbms: DBMSRef,
}

impl Server {
    pub async fn start(&self) -> Result<()> {
        for svc in self.services.iter() {
            svc.start(self.dbms.clone()).context(StartServiceSnafu)?;
        }

        Ok(())
    }

    pub fn stop(&self) {
        self.services.iter().for_each(|s| s.stop())
    }
}

pub struct Builder {
    config: Config,
    // service
    services: Vec<ServiceRef>,
    // todo node manager
    // todo security(eg. ssl、kerberos、others)
    // instance
    dbms: Option<DBMSRef>,
}

impl Builder {
    pub fn new(config: Config) -> Self {
        Self {
            config,
            services: Default::default(),
            dbms: None,
        }
    }

    pub fn with_services(self, services: Vec<ServiceRef>) -> Self {
        Self { services, ..self }
    }

    pub fn add_service(mut self, service: ServiceRef) -> Self {
        self.services.push(service);
        self
    }

    pub fn with_dbms(self, dbms: DBMSRef) -> Self {
        Self {
            dbms: Some(dbms),
            ..self
        }
    }

    pub fn build(self) -> Result<Server> {
        let services = self.services;
        let dbms = self.dbms.context(NotFoundDBMSSnafu)?;

        Ok(Server { services, dbms })
    }
}
