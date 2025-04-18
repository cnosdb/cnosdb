use derive_traits::Keys;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Keys)]
pub struct MetaGlobalConfig {
    #[serde(default = "MetaGlobalConfig::default_node_id")]
    pub node_id: u64,
    #[serde(default = "MetaGlobalConfig::default_cluster_name")]
    pub cluster_name: String,
    #[serde(default = "MetaGlobalConfig::default_raft_node_host")]
    pub raft_node_host: String,
    #[serde(default = "MetaGlobalConfig::default_listen_port")]
    pub listen_port: u16,
    #[serde(default = "MetaGlobalConfig::default_grpc_enable_gzip")]
    pub grpc_enable_gzip: bool,
    #[serde(default = "MetaGlobalConfig::default_data_path")]
    pub data_path: String,
}

impl MetaGlobalConfig {
    fn default_node_id() -> u64 {
        1
    }

    fn default_cluster_name() -> String {
        "cluster_xxx".to_string()
    }

    fn default_raft_node_host() -> String {
        "127.0.0.1".to_string()
    }

    fn default_listen_port() -> u16 {
        8901
    }

    fn default_grpc_enable_gzip() -> bool {
        false
    }

    fn default_data_path() -> String {
        "/var/lib/cnosdb/meta".to_string()
    }
}

impl Default for MetaGlobalConfig {
    fn default() -> Self {
        Self {
            node_id: Self::default_node_id(),
            cluster_name: Self::default_cluster_name(),
            raft_node_host: Self::default_raft_node_host(),
            listen_port: Self::default_listen_port(),
            grpc_enable_gzip: Self::default_grpc_enable_gzip(),
            data_path: Self::default_data_path(),
        }
    }
}
