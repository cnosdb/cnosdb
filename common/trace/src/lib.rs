pub mod exporter;
/// Part of the code fork influxdb_iox
pub mod id;
pub mod span;
pub mod span_ctx;

use std::net::SocketAddr;
use std::path::Path;
use std::str::FromStr;
use std::sync::Arc;

use config::TokioTrace;
pub use exporter::*;
pub use id::*;
use once_cell::sync::Lazy;
use parking_lot::{Mutex, Once};
pub use span::*;
pub use span_ctx::*;
use time::UtcOffset;
use tracing::metadata::LevelFilter;
pub use tracing::{debug, error, info, instrument, trace, warn};
pub use tracing_appender::non_blocking::WorkerGuard;
use tracing_appender::{non_blocking, rolling};
use tracing_error::ErrorLayer;
use tracing_subscriber::fmt::time::OffsetTime;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{filter, fmt, EnvFilter, Layer, Registry};

/// only use for unit test
/// parameter only use for first call
pub fn init_default_global_tracing(dir: impl AsRef<Path>, file_name: &str, level: &str) {
    static START: Once = Once::new();

    START.call_once(|| {
        let mut g = GLOBAL_UT_LOG_GUARD.as_ref().lock();
        *g = Some(init_global_tracing(dir, level, file_name, None));
    });
}

static GLOBAL_UT_LOG_GUARD: Lazy<Arc<Mutex<Option<Vec<WorkerGuard>>>>> =
    Lazy::new(|| Arc::new(Mutex::new(None)));

pub fn env_filter(level: impl ToString) -> EnvFilter {
    EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(level.to_string()))
}

pub fn targets_filter(level: LevelFilter, defined_tokio_trace: bool) -> filter::Targets {
    let mut filter = filter::Targets::new()
        .with_targets(vec![
            // Workspace crates,
            // make sure all workspace members are here.
            ("client", level),
            ("config", level),
            ("coordinator", level),
            ("e2e_test", level),
            ("error_code", level),
            ("error_code_macro", level),
            ("http_protocol", level),
            ("limiter_bucket", level),
            ("lru_cache", level),
            ("cnosdb", level),
            ("memory_pool", level),
            ("meta", level),
            ("metrics", level),
            ("models", level),
            ("protos", level),
            ("protocol_parser", level),
            ("query", level),
            ("spi", level),
            ("sqllogicaltests", level),
            ("test", level),
            ("trace", level),
            ("tskv", level),
            ("utils", level),
            ("replication", level),
        ])
        .with_targets(vec![
            // Third-party crates
            ("actix_web::middleware::logger", LevelFilter::WARN),
        ]);
    if defined_tokio_trace {
        filter = filter.with_targets(vec![
            ("tokio", LevelFilter::TRACE),
            ("runtime", LevelFilter::TRACE),
        ]);
    }
    filter
}

pub fn init_process_global_tracing(
    log_path: impl AsRef<Path>,
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
    log_path: impl AsRef<Path>,
    log_level: &str,
    log_file_prefix_name: &str,
    tokio_trace: Option<&TokioTrace>,
) -> Vec<WorkerGuard> {
    let tracing_level = LevelFilter::from_str(log_level).unwrap_or(LevelFilter::WARN);

    let local_time = OffsetTime::new(
        UtcOffset::current_local_offset().unwrap_or(UtcOffset::UTC),
        time::format_description::well_known::Iso8601::DEFAULT,
    );
    let formatting_layer = fmt::layer()
        .with_ansi(false)
        .with_timer(local_time.clone())
        .with_writer(std::io::stderr)
        .with_filter(LevelFilter::DEBUG);

    let file_appender = rolling::daily(log_path, log_file_prefix_name);
    let (non_blocking_appender, guard) = non_blocking(file_appender);
    let file_layer = fmt::layer()
        .with_ansi(false)
        .with_timer(local_time)
        .with_writer(non_blocking_appender)
        .with_filter(LevelFilter::DEBUG);

    let guards = vec![guard];

    let registry_builder = Registry::default()
        .with(ErrorLayer::default())
        .with(formatting_layer)
        .with(file_layer)
        .with(targets_filter(tracing_level, tokio_trace.is_some()));

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
