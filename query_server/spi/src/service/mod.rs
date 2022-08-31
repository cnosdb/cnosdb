pub mod protocol;

use std::sync::Arc;

use models::define_result;
use snafu::{Backtrace, Snafu};

use crate::server::dbms::DBMSRef;

define_result!(ServiceError);

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum ServiceError {
    #[snafu(display("Failed to start {} service.\nBacktrace:\n{}", identifier, backtrace))]
    StartFailed {
        identifier: String,
        backtrace: Backtrace,
    },
}

pub type ServiceRef = Arc<dyn Service + Send + Sync>;

pub trait Service {
    fn start(&self, dbms: DBMSRef) -> Result<()>;
    fn stop(&self);
}
