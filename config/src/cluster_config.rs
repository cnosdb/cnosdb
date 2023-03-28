use std::net::ToSocketAddrs;
use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::check::{CheckConfig, CheckConfigItemResult, CheckConfigResult};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ClusterConfig {
    #[serde(default = "ClusterConfig::default_node_id")]
    pub node_id: u64,
    #[serde(default = "ClusterConfig::default_name")]
    pub name: String,
    #[serde(default = "ClusterConfig::default_meta_service_addr")]
    pub meta_service_addr: String,

    #[serde(default = "ClusterConfig::default_http_listen_addr")]
    pub http_listen_addr: String,
    #[serde(default = "ClusterConfig::default_grpc_listen_addr")]
    pub grpc_listen_addr: String,
    #[serde(default = "ClusterConfig::default_flight_rpc_listen_addr")]
    pub flight_rpc_listen_addr: String,
    #[serde(default = "ClusterConfig::default_tcp_listen_addr")]
    pub tcp_listen_addr: String,
    #[serde(default = "ClusterConfig::default_store_metrics")]
    pub store_metrics: bool,

    #[serde(default = "ClusterConfig::default_cold_data_server")]
    pub cold_data_server: bool,
}

impl ClusterConfig {
    fn default_node_id() -> u64 {
        100
    }

    fn default_name() -> String {
        "cluster_xxx".to_string()
    }

    fn default_meta_service_addr() -> String {
        "127.0.0.1:8901".to_string()
    }

    fn default_http_listen_addr() -> String {
        "127.0.0.1:8902".to_string()
    }

    fn default_grpc_listen_addr() -> String {
        "127.0.0.1:8903".to_string()
    }

    fn default_flight_rpc_listen_addr() -> String {
        "127.0.0.1:8904".to_string()
    }

    fn default_tcp_listen_addr() -> String {
        "127.0.0.1:8905".to_string()
    }

    fn default_store_metrics() -> bool {
        true
    }

    fn default_cold_data_server() -> bool {
        false
    }

    pub fn override_by_env(&mut self) {
        if let Ok(name) = std::env::var("CNOSDB_CLUSTER_NAME") {
            self.name = name;
        }
        if let Ok(meta) = std::env::var("CNOSDB_CLUSTER_META") {
            self.meta_service_addr = meta;
        }
        if let Ok(id) = std::env::var("CNOSDB_NODE_ID") {
            self.node_id = id.parse::<u64>().unwrap();
        }

        if let Ok(val) = std::env::var("CNOSDB_HTTP_LISTEN_ADDR") {
            self.http_listen_addr = val;
        }

        if let Ok(val) = std::env::var("CNOSDB_GRPC_LISTEN_ADDR") {
            self.grpc_listen_addr = val;
        }

        if let Ok(val) = std::env::var("CNOSDB_FLIGHT_RPC_LISTEN_ADDR") {
            self.flight_rpc_listen_addr = val;
        }

        if let Ok(val) = std::env::var("CNOSDB_TCP_LISTEN_ADDR") {
            self.flight_rpc_listen_addr = val;
        }

        if let Ok(val) = std::env::var("CNOSDB_STORE_METRICS") {
            self.store_metrics = val.parse::<bool>().unwrap();
        }
    }
}

impl Default for ClusterConfig {
    fn default() -> Self {
        Self {
            node_id: Self::default_node_id(),
            name: Self::default_name(),
            meta_service_addr: Self::default_meta_service_addr(),
            http_listen_addr: Self::default_http_listen_addr(),
            grpc_listen_addr: Self::default_grpc_listen_addr(),
            flight_rpc_listen_addr: Self::default_flight_rpc_listen_addr(),
            tcp_listen_addr: Self::default_tcp_listen_addr(),
            store_metrics: Self::default_store_metrics(),
            cold_data_server: Self::default_cold_data_server(),
        }
    }
}

impl CheckConfig for ClusterConfig {
    fn check(&self, _: &crate::Config) -> Option<CheckConfigResult> {
        let config_name = Arc::new("cluster".to_string());
        let mut ret = CheckConfigResult::default();

        if self.name.is_empty() {
            ret.add_warn(CheckConfigItemResult {
                config: config_name.clone(),
                item: "name".to_string(),
                message: "'name' is empty".to_string(),
            });
        }

        if let Err(e) = self.meta_service_addr.to_socket_addrs() {
            ret.add_error(CheckConfigItemResult {
                config: config_name.clone(),
                item: "meta_service_addr".to_string(),
                message: format!("Cannot resolve 'meta_service_addr': {}", e),
            });
        }
        if let Err(e) = self.http_listen_addr.to_socket_addrs() {
            ret.add_error(CheckConfigItemResult {
                config: config_name.clone(),
                item: "http_listen_addr".to_string(),
                message: format!("Cannot resolve 'http_listen_addr': {}", e),
            });
        }
        if let Err(e) = self.grpc_listen_addr.to_socket_addrs() {
            ret.add_error(CheckConfigItemResult {
                config: config_name.clone(),
                item: "grpc_listen_addr".to_string(),
                message: format!("Cannot resolve 'grpc_listen_addr': {}", e),
            });
        }
        if let Err(e) = self.flight_rpc_listen_addr.to_socket_addrs() {
            ret.add_error(CheckConfigItemResult {
                config: config_name,
                item: "flight_rpc_listen_addr".to_string(),
                message: format!("Cannot resolve 'flight_rpc_listen_addr': {}", e),
            });
        }

        if ret.is_empty() {
            None
        } else {
            Some(ret)
        }
    }
}
