use std::{fs::File, io::prelude::Read, path::Path};

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Opt {
    pub id: u64,
    pub http_addr: String,
    pub snapshot_path: String,
    pub journal_path: String,
    pub snapshot_per_events: u32,
    pub logs_path: String,
    pub logs_level: String,
}

pub fn get_opt(path: impl AsRef<Path>) -> Opt {
    let path = path.as_ref();
    let mut file = match File::open(path) {
        Ok(file) => file,
        Err(err) => panic!(
            "Failed to open configurtion file '{}': {}",
            path.display(),
            err
        ),
    };
    let mut content = String::new();
    if let Err(err) = file.read_to_string(&mut content) {
        panic!(
            "Failed to read configurtion file '{}': {}",
            path.display(),
            err
        );
    }
    let config: Opt = match toml::from_str(&content) {
        Ok(config) => config,
        Err(err) => panic!(
            "Failed to parse configurtion file '{}': {}",
            path.display(),
            err
        ),
    };
    config
}

#[cfg(test)]
mod test {
    use crate::store::config::Opt;

    #[test]
    fn test() {
        let config_str = r#"
id = 1
http_addr = "127.0.0.1:21001"

logs_level = "warn"
logs_path = "/tmp/cnosdb/logs"
snapshot_path = "/tmp/cnosdb/meta/snapshot"
journal_path = "/tmp/cnosdb/meta/journal"
snapshot_per_events = 500
"#;

        let config: Opt = toml::from_str(config_str).unwrap();
        assert!(toml::to_string_pretty(&config).is_ok());
        dbg!(config);
    }
}
