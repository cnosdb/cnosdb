use std::collections::HashMap;
use std::fmt::Debug;

use config::Config;
use models::meta_data::*;
use models::node_info::NodeStatus;
use models::utils::{build_address, now_timestamp_secs};
use tokio::sync::RwLock;
use tonic::transport::{Channel, Endpoint};
use trace::error;

use crate::client::MetaHttpClient;
use crate::error::{MetaError, MetaResult};
use crate::model::AdminMeta;
use crate::store::command::{self, EntryLog};
use crate::store::key_path;

#[derive(Debug)]
pub struct RemoteAdminMeta {
    config: Config,
    data_nodes: RwLock<HashMap<u64, NodeInfo>>,
    conn_map: RwLock<HashMap<u64, Channel>>,

    client: MetaHttpClient,
    path: String,
}

impl RemoteAdminMeta {
    pub fn new(config: Config, storage_path: String) -> Self {
        let meta_service_addr = config.cluster.meta_service_addr.clone();
        let meta_url = meta_service_addr.join(";");

        Self {
            config,
            conn_map: RwLock::new(HashMap::new()),
            data_nodes: RwLock::new(HashMap::new()),
            client: MetaHttpClient::new(meta_url),
            path: storage_path,
        }
    }

    async fn sync_all_data_node(&self) -> MetaResult<u64> {
        let req = command::ReadCommand::DataNodes(self.config.cluster.name.clone());
        let (resp, version) = self.client.read::<(Vec<NodeInfo>, u64)>(&req).await?;
        {
            let mut nodes = self.data_nodes.write().await;
            for item in resp.iter() {
                nodes.insert(item.id, item.clone());
            }
        }

        Ok(version)
    }

    pub fn sys_info() -> SysInfo {
        let mut info = SysInfo::default();

        if let Ok(val) = sys_info::disk_info() {
            info.disk_free = val.free;
        }

        if let Ok(val) = sys_info::mem_info() {
            info.mem_free = val.free;
        }

        if let Ok(val) = sys_info::loadavg() {
            info.cpu_load = val.one;
        }

        info
    }
}

#[async_trait::async_trait]
impl AdminMeta for RemoteAdminMeta {
    async fn sync_all(&self) -> MetaResult<u64> {
        self.sync_all_data_node().await
    }

    async fn add_data_node(&self) -> MetaResult<()> {
        let mut attribute = NodeAttribute::default();
        if self.config.node_basic.cold_data_server {
            attribute = NodeAttribute::Cold;
        }

        let node = NodeInfo {
            attribute,
            id: self.config.node_basic.node_id,
            grpc_addr: build_address(
                self.config.host.clone(),
                self.config.cluster.grpc_listen_port,
            ),
            http_addr: build_address(
                self.config.host.clone(),
                self.config.cluster.http_listen_port,
            ),
        };

        let req =
            command::WriteCommand::AddDataNode(self.config.cluster.name.clone(), node.clone());
        let rsp = self.client.write::<command::StatusResponse>(&req).await?;
        if rsp.code != command::META_REQUEST_SUCCESS {
            return Err(MetaError::CommonError {
                msg: format!("add data node err: {} {}", rsp.code, rsp.msg),
            });
        }

        self.data_nodes.write().await.insert(node.id, node);

        Ok(())
    }

    async fn data_nodes(&self) -> Vec<NodeInfo> {
        let mut nodes = vec![];
        for (_, val) in self.data_nodes.read().await.iter() {
            nodes.push(val.clone())
        }

        nodes
    }

    async fn node_info_by_id(&self, id: u64) -> MetaResult<NodeInfo> {
        if let Some(val) = self.data_nodes.read().await.get(&id) {
            return Ok(val.clone());
        }

        Err(MetaError::NotFoundNode { id })
    }

    async fn get_node_conn(&self, node_id: u64) -> MetaResult<Channel> {
        if let Some(val) = self.conn_map.read().await.get(&node_id) {
            return Ok(val.clone());
        }

        let info = self.node_info_by_id(node_id).await?;
        let connector =
            Endpoint::from_shared(format!("http://{}", info.grpc_addr)).map_err(|err| {
                MetaError::ConnectMetaError {
                    msg: err.to_string(),
                }
            })?;

        let channel = connector
            .connect()
            .await
            .map_err(|err| MetaError::ConnectMetaError {
                msg: err.to_string(),
            })?;

        self.conn_map.write().await.insert(node_id, channel.clone());

        return Ok(channel);
    }

    async fn retain_id(&self, count: u32) -> MetaResult<u32> {
        let req = command::WriteCommand::RetainID(self.config.cluster.name.clone(), count);
        let rsp = self.client.write::<command::StatusResponse>(&req).await?;
        if rsp.code != command::META_REQUEST_SUCCESS {
            return Err(MetaError::CommonError {
                msg: format!("retain id err: {} {}", rsp.code, rsp.msg),
            });
        }

        let id = serde_json::from_str::<u32>(&rsp.msg).unwrap_or(0);
        if id == 0 {
            return Err(MetaError::CommonError {
                msg: format!("retain id err: {} ", rsp.msg),
            });
        }

        Ok(id)
    }

    async fn process_watch_log(&self, entry: &EntryLog) -> MetaResult<()> {
        let strs: Vec<&str> = entry.key.split('/').collect();

        let len = strs.len();
        if len == 4 && strs[2] == key_path::DATA_NODES {
            if let Ok(node_id) = serde_json::from_str::<u64>(strs[3]) {
                if entry.tye == command::ENTRY_LOG_TYPE_SET {
                    if let Ok(info) = serde_json::from_str::<NodeInfo>(&entry.val) {
                        self.data_nodes.write().await.insert(node_id, info);
                    }
                } else if entry.tye == command::ENTRY_LOG_TYPE_DEL {
                    self.data_nodes.write().await.remove(&node_id);
                    self.conn_map.write().await.remove(&node_id);
                }
            }
        }

        Ok(())
    }

    async fn report_node_metrics(&self) -> MetaResult<()> {
        let disk_free = match get_disk_info(&self.path) {
            Ok(size) => size,
            Err(e) => {
                error!("Failed to get disk info '{}': {}", self.path, e);
                0
            }
        };

        let mut status = NodeStatus::default();
        const MIN_AVALIBLE_DISK_SPACE: u64 = 1024 * 1024 * 1024;
        if disk_free < MIN_AVALIBLE_DISK_SPACE {
            status = NodeStatus::NoDiskSpace;
        }

        let node_metrics = NodeMetrics {
            id: self.config.node_basic.node_id,
            disk_free,
            time: now_timestamp_secs(),
            status,
        };

        let req = command::WriteCommand::ReportNodeMetrics(
            self.config.cluster.name.clone(),
            node_metrics.clone(),
        );
        let rsp = self.client.write::<command::StatusResponse>(&req).await?;
        if rsp.code != command::META_REQUEST_SUCCESS {
            return Err(MetaError::CommonError {
                msg: format!("report node metrics err: {} {}", rsp.code, rsp.msg),
            });
        }

        Ok(())
    }
}
