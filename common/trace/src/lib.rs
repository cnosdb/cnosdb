use std::net::SocketAddr;
use std::sync::Arc;

use config::TokioTrace;
use once_cell::sync::Lazy;
use parking_lot::{Mutex, Once};
use time::UtcOffset;
use tracing::metadata::LevelFilter;
pub use tracing::{debug, error, info, instrument, trace, warn};
pub use tracing_appender::non_blocking::WorkerGuard;
use tracing_appender::{non_blocking, rolling};
use tracing_error::ErrorLayer;
use tracing_subscriber::fmt::time::OffsetTime;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{fmt, EnvFilter, Layer, Registry};

const TOKIO_TRACE: &str = ",tokio=trace,runtime=trace";
/// only use for unit test
/// parameter only use for first call
pub fn init_default_global_tracing(dir: &str, file_name: &str, level: &str) {
    static START: Once = Once::new();

    START.call_once(|| {
        let mut g = GLOBAL_UT_LOG_GUARD.as_ref().lock();
        *g = Some(init_global_tracing(dir, level, file_name, None));
    });
}

static GLOBAL_UT_LOG_GUARD: Lazy<Arc<Mutex<Option<Vec<WorkerGuard>>>>> =
    Lazy::new(|| Arc::new(Mutex::new(None)));

pub fn get_env_filter(log_level: &str, tokio_trace: Option<&TokioTrace>) -> EnvFilter {
    EnvFilter::try_from_default_env().unwrap_or_else(|_| {
        if tokio_trace.is_some() {
            let level = log_level.to_string() + TOKIO_TRACE;
            EnvFilter::new(level)
        } else {
            EnvFilter::new(log_level)
        }
    })
}

pub fn init_process_global_tracing(
    log_path: &str,
    log_level: &str,
    log_file_prefix_name: &str,
    tokio_trace: Option<&TokioTrace>,
    guard: &Lazy<Arc<Mutex<Option<Vec<WorkerGuard>>>>>,
) {
    static START: Once = Once::new();

    START.call_once(|| {
        let mut g = guard.as_ref().lock();
        *g = Some(init_global_tracing(
            log_path,
            log_level,
            log_file_prefix_name,
            tokio_trace,
        ));
    });
}

pub fn init_global_tracing(
    log_path: &str,
    log_level: &str,
    log_file_prefix_name: &str,
    tokio_trace: Option<&TokioTrace>,
) -> Vec<WorkerGuard> {
    let log_level = log_level.to_string() + ",actix_web::middleware::logger=warn";
    let env_filter = get_env_filter(&log_level, tokio_trace);
    let local_time = OffsetTime::new(
        UtcOffset::current_local_offset().unwrap_or(UtcOffset::UTC),
        time::format_description::well_known::Rfc3339,
    );
    let formatting_layer = fmt::layer()
        .pretty()
        .with_timer(local_time.clone())
        .with_writer(std::io::stderr)
        .with_filter(LevelFilter::DEBUG);

    let file_appender = rolling::daily(log_path, log_file_prefix_name);
    let (non_blocking_appender, guard) = non_blocking(file_appender);
    let file_layer = fmt::layer()
        .with_timer(local_time)
        .with_writer(non_blocking_appender)
        .with_filter(LevelFilter::DEBUG);

    let guards = vec![guard];

    let registry_builder = Registry::default()
        .with(env_filter)
        .with(ErrorLayer::default())
        .with(formatting_layer)
        .with(file_layer);

    if let Some(tokio_trace) = tokio_trace {
        let console_layer = console_subscriber::ConsoleLayer::builder()
            .server_addr(tokio_trace.addr.parse::<SocketAddr>().unwrap())
            .spawn();
        registry_builder.with(console_layer).init()
    } else {
        registry_builder.init()
    }

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
