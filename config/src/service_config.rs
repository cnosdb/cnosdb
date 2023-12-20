use std::net::ToSocketAddrs;
use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::check::{CheckConfig, CheckConfigItemResult, CheckConfigResult};
use crate::override_by_env::{entry_override, entry_override_option, OverrideByEnv};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ServiceConfig {
    #[serde(default = "ServiceConfig::default_http_listen_port")]
    pub http_listen_port: Option<u16>,
    #[serde(default = "ServiceConfig::default_grpc_listen_port")]
    pub grpc_listen_port: Option<u16>,
    #[serde(default = "ServiceConfig::default_grpc_enable_gzip")]
    pub grpc_enable_gzip: bool,
    #[serde(default = "ServiceConfig::default_flight_rpc_listen_port")]
    pub flight_rpc_listen_port: Option<u16>,
    #[serde(default = "ServiceConfig::default_tcp_listen_port")]
    pub tcp_listen_port: Option<u16>,
    #[serde(default = "ServiceConfig::default_vector_listen_port")]
    pub vector_listen_port: Option<u16>,
    #[serde(default = "ServiceConfig::default_enable_report")]
    pub enable_report: bool,
}

impl ServiceConfig {
    fn default_http_listen_port() -> Option<u16> {
        None
    }

    fn default_grpc_listen_port() -> Option<u16> {
        None
    }

    fn default_grpc_enable_gzip() -> bool {
        false
    }

    fn default_flight_rpc_listen_port() -> Option<u16> {
        None
    }

    fn default_tcp_listen_port() -> Option<u16> {
        None
    }

    fn default_vector_listen_port() -> Option<u16> {
        None
    }

    fn default_enable_report() -> bool {
        true
    }
}

impl Default for ServiceConfig {
    fn default() -> Self {
        Self {
            http_listen_port: ServiceConfig::default_http_listen_port(),
            grpc_listen_port: ServiceConfig::default_grpc_listen_port(),
            grpc_enable_gzip: ServiceConfig::default_grpc_enable_gzip(),
            flight_rpc_listen_port: ServiceConfig::default_flight_rpc_listen_port(),
            tcp_listen_port: ServiceConfig::default_tcp_listen_port(),
            vector_listen_port: ServiceConfig::default_vector_listen_port(),
            enable_report: ServiceConfig::default_enable_report(),
        }
    }
}

impl OverrideByEnv for ServiceConfig {
    fn override_by_env(&mut self) {
        entry_override_option(
            &mut self.http_listen_port,
            "CNOSDB_SERVICE_HTTP_LISTEN_PORT",
        );
        entry_override_option(
            &mut self.grpc_listen_port,
            "CNOSDB_SERVICE_GRPC_LISTEN_PORT",
        );
        entry_override(
            &mut self.grpc_enable_gzip,
            "CNOSDB_SERVICE_GRPC_ENABLE_GZIP",
        );
        entry_override_option(
            &mut self.flight_rpc_listen_port,
            "CNOSDB_SERVICE_FLIGHT_RPC_LISTEN_PORT",
        );
        entry_override_option(&mut self.tcp_listen_port, "CNOSDB_SERVICE_TCP_LISTEN_PORT");
        entry_override_option(
            &mut self.vector_listen_port,
            "CNOSDB_SERVICE_VECTOR_LISTEN_PORT",
        );
        entry_override(&mut self.enable_report, "CNOSDB_SERVICE_ENABLE_REPORT");
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct HttpServiceConfig {
    pub tcp_listen_port: Option<u16>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct InternalServiceConfig {
    pub grpc_listen_port: Option<u16>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FlightRpcServiceConfig {
    pub tcp_listen_port: Option<u16>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct OpenTSDBServiceConfig {
    pub tcp_listen_port: Option<u16>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct VectorServiceConfig {
    pub tcp_listen_port: Option<u16>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReportServiceConfig {
    pub enabled: bool,
}

impl CheckConfig for ServiceConfig {
    fn check(&self, config: &crate::Config) -> Option<CheckConfigResult> {
        let config_name = Arc::new("service".to_string());
        let mut ret = CheckConfigResult::default();

        if let Some(port) = self.http_listen_port {
            let default_http_addr = format!("{}:{}", &config.global.host, port);
            if let Err(e) = default_http_addr.to_socket_addrs() {
                ret.add_error(CheckConfigItemResult {
                    config: config_name.clone(),
                    item: default_http_addr,
                    message: format!("Cannot resolve 'http_listen_addr': {}", e),
                });
            }
        }

        if let Some(port) = self.grpc_listen_port {
            let default_grpc_addr = format!("{}:{}", &config.global.host, port);
            if let Err(e) = default_grpc_addr.to_socket_addrs() {
                ret.add_error(CheckConfigItemResult {
                    config: config_name.clone(),
                    item: default_grpc_addr,
                    message: format!("Cannot resolve 'grpc_listen_addr': {}", e),
                });
            }
        }

        if let Some(port) = self.flight_rpc_listen_port {
            let default_flight_rpc_addr = format!("{}:{}", &config.global.host, port);
            if let Err(e) = default_flight_rpc_addr.to_socket_addrs() {
                ret.add_error(CheckConfigItemResult {
                    config: config_name.clone(),
                    item: default_flight_rpc_addr,
                    message: format!("Cannot resolve 'flight_rpc_listen_addr': {}", e),
                });
            }
        }

        if let Some(port) = self.tcp_listen_port {
            let default_tcp_addr = format!("{}:{}", &config.global.host, port);
            if let Err(e) = default_tcp_addr.to_socket_addrs() {
                ret.add_error(CheckConfigItemResult {
                    config: config_name.clone(),
                    item: default_tcp_addr,
                    message: format!("Cannot resolve 'tcp_listen_addr': {}", e),
                });
            }
        }

        if let Some(port) = self.vector_listen_port {
            let default_vector_addr = format!("{}:{}", &config.global.host, port);
            if let Err(e) = default_vector_addr.to_socket_addrs() {
                ret.add_error(CheckConfigItemResult {
                    config: config_name,
                    item: default_vector_addr,
                    message: format!("Cannot resolve 'vector_listen_addr': {}", e),
                });
            }
        }

        if ret.is_empty() {
            None
        } else {
            Some(ret)
        }
    }
}
