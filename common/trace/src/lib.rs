use std::sync::{Arc, Mutex, Once};

use once_cell::sync::Lazy;
pub use tracing::{debug, error, info, instrument, trace, warn};
use tracing_appender::non_blocking::WorkerGuard;
use tracing_appender::{non_blocking, rolling};
use tracing_error::ErrorLayer;
use tracing_subscriber::filter::EnvFilter;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{fmt, Registry};

/// only use for unit test
/// parameter only use for first call
pub fn init_default_global_tracing(dir: &str, file_name: &str, level: &str) {
    static START: Once = Once::new();

    START.call_once(|| {
        let mut g = GLOBAL_UT_LOG_GUARD.as_ref().lock().unwrap();
        *g = Some(init_global_tracing(dir, file_name, level));
    });
}

static GLOBAL_UT_LOG_GUARD: Lazy<Arc<Mutex<Option<Vec<WorkerGuard>>>>> =
    Lazy::new(|| Arc::new(Mutex::new(None)));

pub fn init_global_tracing(dir: &str, file_name: &str, level: &str) -> Vec<WorkerGuard> {
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(level));
    let formatting_layer = fmt::layer().pretty().with_writer(std::io::stderr);

    let file_appender = rolling::daily(dir, file_name);
    let (non_blocking_appender, guard) = non_blocking(file_appender);
    let file_layer = fmt::layer().with_writer(non_blocking_appender);

    let guards = vec![guard];

    Registry::default()
        .with(env_filter)
        .with(ErrorLayer::default())
        .with(formatting_layer)
        .with(file_layer)
        .init();

    match color_eyre::install() {
        Ok(_) => (),
        Err(_) => {
            debug!("already init color eyre");
        }
    }

    debug!("log trace init successful");

    guards
}
///Use this macro to wrap the expression, you can output the error log
#[macro_export]
macro_rules! log_error {
    ($e: expr) => {
        error!("{}", &$e);
        #[allow(unused_must_use)] {$e}
    };

    ($e: expr, $($arg: tt),+) => {
        error!("{}, {}", format!($($arg)+).as_str(), &$e);
        #[allow(unused_must_use)] {&$e}
    };
}
/// Use this macro to wrap the expression, you can output the warning log
#[macro_export]
macro_rules! log_warn {
    ($e: expr) => {
        warn!("{}", &$e);
        #[allow(unused_must_use)] {$e}
    };

    ($e: expr, $($arg: tt),+) => {
        warn!("{}, {}", format!($($arg)+).as_str(), &$e);
        #[allow(unused_must_use)] {&$e}
    };
}

#[cfg(test)]
mod tests {
    use crate::{error, info, init_default_global_tracing, instrument};
    #[instrument]
    fn return_err() {
        error!("wrong");
    }

    #[test]
    #[instrument]
    fn test_init() {
        init_default_global_tracing("trace", "trace.log", "debug");
        info!("hello");
        init_default_global_tracing("trace", "trace.log", "debug");
        info!("hello");
        return_err();
    }
}
