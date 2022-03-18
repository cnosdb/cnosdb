pub use log::{info, debug, warn, trace, error, LevelFilter};
use simplelog::{Config, SharedLogger, TerminalMode,
                ColorChoice, WriteLogger, CombinedLogger, TermLogger};
use std::fs::File;

pub fn init(level: LevelFilter, file_path: &str) {
    let mut logger = Vec::<Box<dyn SharedLogger>>::new();
    let config = Config::default();

    logger.push(TermLogger::new(
        level, config.clone(),
        TerminalMode::Mixed,
        ColorChoice::Auto));

    logger.push(WriteLogger::new(
        level, config.clone(),
        File::create(file_path).unwrap()));
    CombinedLogger::init(logger).unwrap();
}