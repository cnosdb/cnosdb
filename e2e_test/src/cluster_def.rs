#![allow(dead_code)]

use std::path::{Path, PathBuf};
use std::time::Duration;

use config::meta::Opt as MetaStoreConfig;
use config::tskv::Config as CnosdbConfig;

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
    pub config_file_name: String,
    pub host_port: String,
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
            host_port: format!("127.0.0.1:{meta_port}"),
        }
    }

    /// Returns $test_dir/meta/config/$config_file_name
    pub fn to_config_path(&self, base_dir: impl AsRef<Path>) -> PathBuf {
        base_dir
            .as_ref()
            .join("config")
            .join(&self.config_file_name)
    }

    pub fn to_host_port(&self) -> (String, u16) {
        match self.host_port.split_once(':') {
            Some((host, port)) => (host.to_string(), port.parse::<u16>().unwrap()),
            None => {
                panic!("Cannot split host_port {} by ':'", self.host_port);
            }
        }
    }

    pub fn update_config(&self, config: &mut MetaStoreConfig) {
        config.id = self.id as u64;
        let (host, port) = self.to_host_port();
        config.host = host;
        config.port = port;
        config.system_database_replica = 1;
    }
}

impl Default for MetaNodeDefinition {
    fn default() -> Self {
        Self {
            id: 1,
            config_file_name: "config_8901.toml".to_string(),
            host_port: "127.0.0.1:8901".to_string(),
        }
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
    pub config_file_name: String,
    pub mode: DeploymentMode,
    pub http_host_port: String,
    pub heartbeat_interval: Duration,
    pub meta_host_ports: Vec<String>,
    pub coord_service_port: Option<u16>,
    pub flight_service_port: Option<u16>,
    pub opentsdb_service_port: Option<u16>,
    pub grpc_enable_gzip: bool,
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
                vec![format!("127.0.0.1:{}", meta_id_to_port(id))],
            )
        } else {
            (
                DeploymentMode::QueryTskv,
                meta_ids
                    .iter()
                    .map(|meta_id| format!("127.0.0.1:{}", meta_id_to_port(*meta_id)))
                    .collect(),
            )
        };
        Self {
            id,
            config_file_name: format!("config_{data_http_port}.toml"),
            mode,
            http_host_port: format!("127.0.0.1:{data_http_port}"),
            meta_host_ports,
            coord_service_port: Some(data_id_to_coord_service_port(id)),
            flight_service_port: Some(data_id_to_flight_service_port(id)),
            opentsdb_service_port: Some(data_id_to_opentsdb_service_port(id)),
            heartbeat_interval: Duration::from_millis(100),
            grpc_enable_gzip: false,
        }
    }

    /// Returns $test_dir/data/config/$config_file_name
    pub fn to_config_path(&self, base_dir: impl AsRef<Path>) -> PathBuf {
        base_dir
            .as_ref()
            .join("config")
            .join(&self.config_file_name)
    }

    pub fn to_host_port(&self) -> (String, u16) {
        match self.http_host_port.split_once(':') {
            Some((host, port)) => (host.to_string(), port.parse::<u16>().unwrap()),
            None => {
                panic!("Cannot split host_port {} by ':'", self.http_host_port);
            }
        }
    }

    pub fn update_config(&self, config: &mut CnosdbConfig) {
        config.global.store_metrics = false;
        config.global.node_id = self.id as u64;
        config.deployment.mode = self.mode.to_string();
        config.service.http_listen_port = Some(self.to_host_port().1);
        config.meta.service_addr = self.meta_host_ports.clone();
        config.service.grpc_listen_port = self.coord_service_port;
        config.service.flight_rpc_listen_port = self.flight_service_port;
        config.service.tcp_listen_port = self.opentsdb_service_port;
        config.cluster.heartbeat_interval = self.heartbeat_interval;
        config.service.grpc_enable_gzip = self.grpc_enable_gzip;
    }
}

impl Default for DataNodeDefinition {
    fn default() -> Self {
        Self {
            id: 1,
            config_file_name: "config_8902.toml".to_string(),
            mode: DeploymentMode::QueryTskv,
            http_host_port: "127.0.0.1:8902".to_string(),
            meta_host_ports: vec!["127.0.0.1:8901".to_string()],
            coord_service_port: Some(8903),
            flight_service_port: Some(8904),
            opentsdb_service_port: Some(8905),
            heartbeat_interval: Duration::from_millis(100),
            grpc_enable_gzip: false,
        }
    }
}

pub fn one_data(id: u8) -> DataNodeDefinition {
    DataNodeDefinition::new(id, &[])
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
        let data_http_port = 8902 + (id - 1) as u16 * 10;
        let data_node_def = DataNodeDefinition {
            config_file_name: format!("config_{data_http_port}.toml"),
            mode: DeploymentMode::Singleton,
            http_host_port: format!("127.0.0.1:{data_http_port}"),
            meta_host_ports: vec!["127.0.0.1:8901".to_string()],
            ..Default::default()
        };
        assert_eq!(one_data(id), data_node_def);
    }
    {
        // Test one_meta_one_data()
        let (meta_id, data_id) = (1_u8, 1_u8);
        let meta_port = 8901 + (meta_id - 1) as u16 * 10;
        let data_http_port = 8902 + (data_id - 1) as u16 * 10;
        let meta_host_port = format!("127.0.0.1:{meta_port}");
        let cluster_def = CnosdbClusterDefinition {
            meta_cluster_def: vec![MetaNodeDefinition {
                id: 1,
                config_file_name: format!("config_{meta_port}.toml"),
                host_port: meta_host_port.clone(),
            }],
            data_cluster_def: vec![DataNodeDefinition {
                id: 1,
                config_file_name: format!("config_{data_http_port}.toml"),
                mode: DeploymentMode::QueryTskv,
                http_host_port: format!("127.0.0.1:{data_http_port}"),
                meta_host_ports: vec![meta_host_port],
                coord_service_port: Some(8903 + (data_id - 1) as u16 * 10),
                flight_service_port: Some(8904 + (data_id - 1) as u16 * 10),
                opentsdb_service_port: Some(8905 + (data_id - 1) as u16 * 10),
                heartbeat_interval: Duration::from_millis(100),
                grpc_enable_gzip: false,
            }],
        };
        assert_eq!(one_meta_one_data(meta_id, data_id), cluster_def);
    }
    {
        // Test one_meta_two_data_bundled
        let cluster_def = CnosdbClusterDefinition {
            meta_cluster_def: vec![MetaNodeDefinition {
                id: 1,
                config_file_name: "config_8901.toml".to_string(),
                host_port: "127.0.0.1:8901".to_string(),
            }],
            data_cluster_def: vec![
                DataNodeDefinition::default(),
                DataNodeDefinition {
                    id: 2,
                    config_file_name: "config_8912.toml".to_string(),
                    mode: DeploymentMode::QueryTskv,
                    http_host_port: "127.0.0.1:8912".to_string(),
                    meta_host_ports: vec!["127.0.0.1:8901".to_string()],
                    coord_service_port: Some(8913),
                    flight_service_port: Some(8914),
                    opentsdb_service_port: Some(8915),
                    heartbeat_interval: Duration::from_millis(100),
                    grpc_enable_gzip: false,
                },
            ],
        };
        assert_eq!(one_meta_two_data_bundled(), cluster_def);
    }
}
