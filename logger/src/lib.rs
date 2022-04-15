pub use log::{debug, error, info, trace, warn, LevelFilter};
use simplelog::{
    ColorChoice, CombinedLogger, Config, SharedLogger, TermLogger, TerminalMode, WriteLogger,
};
use std::fs::File;

pub fn init(level: LevelFilter, file_path: &str) {
    let mut logger = Vec::<Box<dyn SharedLogger>>::new();
    let config = Config::default();

    logger.push(TermLogger::new(
        level,
        config.clone(),
        TerminalMode::Mixed,
        ColorChoice::Auto,
    ));

    // file
    logger.push(WriteLogger::new(
        level,
        config.clone(),
        File::create(file_path).unwrap(),
    ));
    CombinedLogger::init(logger).unwrap();
}

#[test]
fn test() {
    use log::LevelFilter::Trace;
    init(Trace, "./test.log");
    info!("hello log");
}
