use models::define_result;
use snafu::{Backtrace, Snafu};

pub mod instance;
pub mod server;

define_result!(Error);

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Please inject DBMS.\nBacktrace:\n{}", backtrace))]
    NotFoundDBMS { backtrace: Backtrace },

    #[snafu(display("Failed to start service. err: {}", source))]
    StartService { source: spi::service::ServiceError },
}
