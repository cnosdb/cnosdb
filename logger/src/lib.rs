pub use log::{debug, error, info, trace, warn, LevelFilter};

pub fn init() {
    match log4rs::init_file("../logger/tskv_log.yaml", Default::default()) {
        Ok(_) => (),
        Err(e) => {
            info!("{}", e);
        },
    };
}

pub fn init_with_config_path(path: &str) {
    match log4rs::init_file(path, Default::default()) {
        Ok(_) => (),
        Err(e) => {
            info!("{}", e);
        },
    };
}

#[test]
fn test() {
    init();
    info!("hello log");
}
