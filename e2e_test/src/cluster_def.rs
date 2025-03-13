#![cfg(test)]
#![allow(unused)]

use std::net::SocketAddrV4;
use std::path::{Path, PathBuf};
use std::time::Duration;

use config::meta::Opt as MetaStoreConfig;
use config::tskv::Config as CnosdbConfig;

use crate::utils::global::{E2eContext, LOOPBACK_IP};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CnosdbClusterDefinition {
    pub meta_cluster_def: Vec<MetaNodeDefinition>,
    pub data_cluster_def: Vec<DataNodeDefinition>,
}

impl CnosdbClusterDefinition {
    /// Create CnosdbClusterDefinition by meta_ids and data_ids,
    /// all ids are typed as u8, and greater than 0.
    ///
    /// If meta_ids is empty, then data nodes will run in singleton mode.
    pub fn with_ids(meta_ids: &[u8], data_ids: &[u8]) -> Self {
        let meta_cluster_def: Vec<MetaNodeDefinition> = meta_ids
            .iter()
            .map(|id| MetaNodeDefinition::new(*id))
            .collect();

        let data_cluster_def: Vec<DataNodeDefinition> = data_ids
            .iter()
            .map(|id| DataNodeDefinition::new(*id, meta_ids))
            .collect();

        CnosdbClusterDefinition {
            meta_cluster_def,
            data_cluster_def,
        }
    }
}

impl std::fmt::Display for CnosdbClusterDefinition {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if !self.meta_cluster_def.is_empty() {
            writeln!(f, "## meta")?;
            for meta_def in self.meta_cluster_def.iter() {
                writeln!(f, "* {meta_def}")?;
            }
        }
        writeln!(f, "## data:")?;
        for data_def in self.data_cluster_def.iter() {
            writeln!(f, "* {data_def}")?;
        }
        Ok(())
    }
}

fn meta_id_to_port(id: u8) -> u16 {
    8901 + (id - 1) as u16 * 10
}

fn data_id_to_http_port(id: u8) -> u16 {
    8902 + (id - 1) as u16 * 10
}

fn data_id_to_coord_service_port(id: u8) -> u16 {
    8903 + (id - 1) as u16 * 10
}

fn data_id_to_flight_service_port(id: u8) -> u16 {
    8904 + (id - 1) as u16 * 10
}

fn data_id_to_opentsdb_service_port(id: u8) -> u16 {
    8905 + (id - 1) as u16 * 10
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetaNodeDefinition {
    pub id: u8,
    /// The config file name, default is `config_{port}.toml`.
    pub config_file_name: String,
    pub host_port: SocketAddrV4,
}

impl MetaNodeDefinition {
    pub fn new(id: u8) -> Self {
        if id == 0 {
            panic!("id must be greater than 0");
        }
        let meta_port = meta_id_to_port(id);
        Self {
            id,
            config_file_name: format!("config_{meta_port}.toml"),
            host_port: SocketAddrV4::new(LOOPBACK_IP, meta_port),
        }
    }

    /// Reset ports to avoid conflict.
    pub fn reset_ports(&mut self, ctx: &E2eContext) {
        let host_port = ctx
            .next_addr(*self.host_port.ip())
            .expect("get a valid port");
        self.host_port = host_port;
    }

    /// Returns $test_dir/meta/config/$config_file_name
    pub fn to_config_path(&self, base_dir: impl AsRef<Path>) -> PathBuf {
        base_dir
            .as_ref()
            .join("config")
            .join(&self.config_file_name)
    }

    /// Update the given meta node config with self.
    pub fn update_config(&self, config: &mut MetaStoreConfig) {
        config.global.node_id = self.id as u64;
        config.global.raft_node_host = self.host_port.ip().to_string();
        config.global.listen_port = self.host_port.port();
        config.sys_config.system_database_replica = 1;
    }
}

impl Default for MetaNodeDefinition {
    fn default() -> Self {
        Self {
            id: 1,
            config_file_name: "config_8901.toml".to_string(),
            host_port: SocketAddrV4::new(LOOPBACK_IP, 8901),
        }
    }
}

impl std::fmt::Display for MetaNodeDefinition {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "node_id: {}, config_file: '{}', host_port: '{}'",
            self.id, self.config_file_name, self.host_port,
        )
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum DeploymentMode {
    QueryTskv,
    Tskv,
    Query,
    Singleton,
}

impl std::str::FromStr for DeploymentMode {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mode = match s.to_ascii_lowercase().as_str() {
            "query_tskv" => Self::QueryTskv,
            "tskv" => Self::Tskv,
            "query" => Self::Query,
            "singleton" => Self::Singleton,
            _ => {
                return Err(
                    "deployment must be one of [query_tskv, tskv, query, singleton]".to_string(),
                )
            }
        };
        Ok(mode)
    }
}

impl std::fmt::Display for DeploymentMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DeploymentMode::QueryTskv => write!(f, "query_tskv"),
            DeploymentMode::Tskv => write!(f, "tskv"),
            DeploymentMode::Query => write!(f, "query"),
            DeploymentMode::Singleton => write!(f, "singleton"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DataNodeDefinition {
    pub id: u8,
    /// The config file name, default is `config_{port}.toml`.
    pub config_file_name: String,
    pub mode: DeploymentMode,

    /// Meta services address.
    pub meta_host_ports: Vec<SocketAddrV4>,

    /// HTTP service listen port, default 8902.
    pub http_host_port: SocketAddrV4,
    /// GRPC service listen port, default 8903.
    pub coord_service_port: Option<u16>,
    pub grpc_enable_gzip: bool,
    /// ArrowFlight service listen port, default 8904.
    pub flight_service_port: Option<u16>,
    /// TCP service listen port, default 8905.
    pub opentsdb_service_port: Option<u16>,

    /// Heartbeat interval of the Raft replication algorithm.
    pub heartbeat_interval: Duration,

    /// Use TLS or not.
    pub enable_tls: bool,
}

impl DataNodeDefinition {
    pub fn new(id: u8, meta_ids: &[u8]) -> Self {
        if id == 0 || meta_ids.iter().any(|id| *id == 0) {
            panic!("id must be greater than 0");
        }
        let data_http_port = data_id_to_http_port(id);
        let (mode, meta_host_ports) = if meta_ids.is_empty() {
            (
                DeploymentMode::Singleton,
                vec![SocketAddrV4::new(LOOPBACK_IP, meta_id_to_port(id))],
            )
        } else {
            (
                DeploymentMode::QueryTskv,
                meta_ids
                    .iter()
                    .map(|meta_id| SocketAddrV4::new(LOOPBACK_IP, meta_id_to_port(*meta_id)))
                    .collect(),
            )
        };
        Self {
            id,
            config_file_name: format!("config_{data_http_port}.toml"),
            mode,
            meta_host_ports,
            http_host_port: SocketAddrV4::new(LOOPBACK_IP, data_http_port),
            coord_service_port: Some(data_id_to_coord_service_port(id)),
            grpc_enable_gzip: false,
            flight_service_port: Some(data_id_to_flight_service_port(id)),
            opentsdb_service_port: Some(data_id_to_opentsdb_service_port(id)),
            heartbeat_interval: Duration::from_millis(100),
            enable_tls: false,
        }
    }

    /// Reset ports to avoid conflict.
    pub fn reset_ports(&mut self, ctx: &E2eContext) {
        let default_host = *self.http_host_port.ip();

        self.http_host_port = ctx.next_addr(default_host).expect("get a valid port");
        if let Some(p) = self.coord_service_port.as_mut() {
            *p = ctx
                .next_addr(default_host)
                .expect("get a valid port")
                .port();
        }
        if let Some(p) = self.flight_service_port.as_mut() {
            *p = ctx
                .next_addr(default_host)
                .expect("get a valid port")
                .port();
        }
        if let Some(p) = self.opentsdb_service_port.as_mut() {
            *p = ctx
                .next_addr(default_host)
                .expect("get a valid port")
                .port();
        }
    }

    /// Returns $test_dir/data/config/$config_file_name
    pub fn to_config_path(&self, base_dir: impl AsRef<Path>) -> PathBuf {
        base_dir
            .as_ref()
            .join("config")
            .join(&self.config_file_name)
    }

    /// Generate meta service address list, for `MetaConfig::service_addr`.
    pub fn meta_service_addr(&self) -> Vec<String> {
        self.meta_host_ports
            .iter()
            .map(|addr| addr.to_string())
            .collect()
    }

    /// Update the given data node config with self.
    pub fn update_config(&self, config: &mut CnosdbConfig) {
        config.global.store_metrics = false;
        config.global.node_id = self.id as u64;
        config.deployment.mode = self.mode.to_string();
        config.meta.service_addr = self.meta_service_addr();
        config.service.http_listen_port = Some(self.http_host_port.port());
        config.service.grpc_listen_port = self.coord_service_port;
        config.service.grpc_enable_gzip = self.grpc_enable_gzip;
        config.service.flight_rpc_listen_port = self.flight_service_port;
        config.service.tcp_listen_port = self.opentsdb_service_port;
        config.cluster.heartbeat_interval = self.heartbeat_interval;
    }
}

impl Default for DataNodeDefinition {
    fn default() -> Self {
        Self {
            id: 1,
            config_file_name: "config_8902.toml".to_string(),
            mode: DeploymentMode::QueryTskv,
            meta_host_ports: vec![SocketAddrV4::new(LOOPBACK_IP, 8901)],
            http_host_port: SocketAddrV4::new(LOOPBACK_IP, 8902),
            coord_service_port: Some(8903),
            grpc_enable_gzip: false,
            flight_service_port: Some(8904),
            opentsdb_service_port: Some(8905),
            heartbeat_interval: Duration::from_millis(100),
            enable_tls: false,
        }
    }
}

impl std::fmt::Display for DataNodeDefinition {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "node_id: {}, config_file: '{}', mode: {}, meta_host_ports: [",
            self.id, self.config_file_name, self.mode,
        )?;
        for (i, host_port) in self.meta_host_ports.iter().enumerate() {
            write!(f, "'{host_port}'")?;
            if i < self.meta_host_ports.len() - 1 {
                write!(f, ", ")?;
            }
        }
        write!(
            f,
            "], http_host_port: {}, coord_service_port: {:?}, grpc_enable_gzip: {}, flight_service_port: {:?}, opentsdb_service_port: {:?}, heartbeat_interval: {}ms, enable_tls: {}",
            self.http_host_port, self.coord_service_port, self.grpc_enable_gzip, self.flight_service_port, self.opentsdb_service_port,  self.heartbeat_interval.as_millis(), self.enable_tls,
        )
    }
}

pub fn one_data(id: u8) -> CnosdbClusterDefinition {
    CnosdbClusterDefinition::with_ids(&[], &[id])
}

pub fn one_meta_one_data(meta_id: u8, data_id: u8) -> CnosdbClusterDefinition {
    CnosdbClusterDefinition::with_ids(&[meta_id], &[data_id])
}

pub fn one_meta_two_data_bundled() -> CnosdbClusterDefinition {
    CnosdbClusterDefinition::with_ids(&[1], &[1, 2])
}

pub fn one_meta_two_data_separated() -> CnosdbClusterDefinition {
    let mut cluster_def = CnosdbClusterDefinition::with_ids(&[1], &[1, 2]);
    cluster_def.data_cluster_def[0].mode = DeploymentMode::Tskv;
    cluster_def.data_cluster_def[1].mode = DeploymentMode::Query;
    cluster_def
}

pub fn one_meta_three_data() -> CnosdbClusterDefinition {
    CnosdbClusterDefinition::with_ids(&[1], &[1, 2, 3])
}

pub fn three_meta_two_data_bundled() -> CnosdbClusterDefinition {
    CnosdbClusterDefinition::with_ids(&[1, 2, 3], &[1, 2])
}

pub fn three_meta_two_data_separated() -> CnosdbClusterDefinition {
    let mut cluster_def = CnosdbClusterDefinition::with_ids(&[1, 2, 3], &[1, 2]);
    cluster_def.data_cluster_def[0].mode = DeploymentMode::Tskv;
    cluster_def.data_cluster_def[1].mode = DeploymentMode::Query;
    cluster_def
}

pub fn three_meta_three_data() -> CnosdbClusterDefinition {
    CnosdbClusterDefinition::with_ids(&[1, 2, 3], &[1, 2, 3])
}

#[test]
fn test_cnosdb_cluster_definition_factory() {
    {
        // Test one_data()
        let id = 1_u8;
        assert_eq!(
            one_data(id),
            CnosdbClusterDefinition {
                meta_cluster_def: vec![],
                data_cluster_def: vec![DataNodeDefinition {
                    mode: DeploymentMode::Singleton,
                    ..Default::default()
                }],
            }
        );
    }
    {
        // Test one_meta_one_data()
        let (meta_id, data_id) = (1_u8, 1_u8);
        let cluster_def = CnosdbClusterDefinition {
            meta_cluster_def: vec![MetaNodeDefinition::default()],
            data_cluster_def: vec![DataNodeDefinition::default()],
        };
        assert_eq!(one_meta_one_data(meta_id, data_id), cluster_def);
    }
    {
        // Test one_meta_two_data_bundled
        let cluster_def = CnosdbClusterDefinition {
            meta_cluster_def: vec![MetaNodeDefinition::default()],
            data_cluster_def: vec![
                DataNodeDefinition::default(),
                DataNodeDefinition {
                    id: 2,
                    config_file_name: "config_8912.toml".to_string(),
                    mode: DeploymentMode::QueryTskv,
                    meta_host_ports: vec![SocketAddrV4::new(LOOPBACK_IP, 8901)],
                    http_host_port: SocketAddrV4::new(LOOPBACK_IP, 8912),
                    coord_service_port: Some(8913),
                    grpc_enable_gzip: false,
                    flight_service_port: Some(8914),
                    opentsdb_service_port: Some(8915),
                    heartbeat_interval: Duration::from_millis(100),
                    enable_tls: false,
                },
            ],
        };
        assert_eq!(one_meta_two_data_bundled(), cluster_def);
    }
}
