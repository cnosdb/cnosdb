use std::fs::File;

pub use log::{debug, error, info, trace, warn, LevelFilter};
use log4rs;

pub fn init() {
    log4rs::init_file("../logger/tskv_log.yaml", Default::default()).unwrap();
}

pub fn init_with_config_path(path: &str) {
    log4rs::init_file(path, Default::default()).unwrap();
}

#[test]
fn test() {
    init();
    info!("hello log");
}
