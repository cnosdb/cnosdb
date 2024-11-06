use macros::EnvKeys;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, EnvKeys)]
pub struct MetaGlobalConfig {
    pub node_id: u64,
    pub cluster_name: String,
    pub raft_node_host: String,
    pub listen_port: u16,
    pub grpc_enable_gzip: bool,
    pub data_path: String,
}

impl Default for MetaGlobalConfig {
    fn default() -> Self {
        Self {
            node_id: 1,
            cluster_name: String::from("cluster_xxx"),
            raft_node_host: String::from("127.0.0.1"),
            listen_port: 8901,
            grpc_enable_gzip: false,
            data_path: String::from("/var/lib/cnosdb/meta"),
        }
    }
}
