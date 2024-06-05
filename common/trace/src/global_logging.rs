use std::net::SocketAddr;
use std::path::Path;
use std::str::FromStr;
use std::sync::{Once, OnceLock};

use config::common::LogConfig;
use time::UtcOffset;
use tracing::debug;
use tracing::level_filters::LevelFilter;
use tracing_appender::non_blocking;
use tracing_appender::non_blocking::WorkerGuard;
use tracing_appender::rolling::{RollingFileAppender, Rotation};
use tracing_error::ErrorLayer;
use tracing_subscriber::fmt::time::OffsetTime;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{filter, fmt, Registry};

static GLOBAL_LOG_GUARD: OnceLock<Vec<WorkerGuard>> = OnceLock::new();
static START_LOGGING: Once = Once::new();

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
            ("macros", level),
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
            ("datafusion", level),
            ("arrow", level),
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

pub fn init_global_logging(log_config: &LogConfig, file_name_prefix: &str) {
    START_LOGGING.call_once(|| {
        let tracing_level = LevelFilter::from_str(&log_config.level).unwrap_or(LevelFilter::WARN);

        let local_time = OffsetTime::new(
            UtcOffset::current_local_offset().unwrap_or(UtcOffset::UTC),
            time::format_description::well_known::Iso8601::DEFAULT,
        );
        let formatting_layer = fmt::layer()
            .with_ansi(false)
            .with_timer(local_time.clone())
            .with_writer(std::io::stderr);

        let rotation = match log_config.file_rotation.as_str() {
            "daily" => Rotation::DAILY,
            "hourly" => Rotation::HOURLY,
            "minutely" => Rotation::MINUTELY,
            "never" => Rotation::NEVER,
            _ => {
                eprintln!(
                    "unrecognized file_rotation: {}, default to [never]",
                    log_config.file_rotation
                );
                Rotation::NEVER
            }
        };

        let mut file_appender_builder = RollingFileAppender::builder()
            .filename_prefix(file_name_prefix)
            .rotation(rotation);

        if let Some(count) = log_config.max_file_count {
            file_appender_builder = file_appender_builder.max_log_files(count);
        }

        let file_appender = file_appender_builder.build(&log_config.path).unwrap();
        let (non_blocking_appender, guard) = non_blocking(file_appender);
        let file_layer = fmt::layer()
            .with_ansi(false)
            .with_timer(local_time)
            .with_writer(non_blocking_appender);

        let guards = vec![guard];
        GLOBAL_LOG_GUARD.get_or_init(|| guards);

        let registry_builder = Registry::default()
            .with(ErrorLayer::default())
            .with(formatting_layer)
            .with(file_layer)
            .with(targets_filter(
                tracing_level,
                log_config.tokio_trace.is_some(),
            ));

        if let Some(tokio_trace) = &log_config.tokio_trace {
            let console_layer = console_subscriber::ConsoleLayer::builder()
                .server_addr(tokio_trace.addr.parse::<SocketAddr>().unwrap())
                .spawn();
            registry_builder.with(console_layer).init()
        } else {
            registry_builder.init()
        }

        debug!("log trace init successful");
    });
}

/// only use for unit test
/// parameter only use for first call
pub fn init_default_global_tracing(dir: impl AsRef<Path>, file_name: &str, level: &str) {
    let log_config = LogConfig {
        level: level.to_owned(),
        path: dir.as_ref().to_string_lossy().to_string(),
        max_file_count: None,
        file_rotation: "daily".to_owned(),
        tokio_trace: None,
    };
    init_global_logging(&log_config, file_name);
}

#[cfg(test)]
mod tests {
    use crate::global_logging::init_default_global_tracing;
    use crate::{error, info, instrument};
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
