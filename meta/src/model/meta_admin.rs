use std::collections::HashMap;
use std::fmt::Debug;

use config::ClusterConfig;
use models::meta_data::*;
use tokio::sync::RwLock;
use tonic::transport::{Channel, Endpoint};
use trace::info;

use crate::client::MetaHttpClient;
use crate::error::{MetaError, MetaResult};
use crate::model::AdminMeta;
use crate::store::command::{self, EntryLog};
use crate::store::key_path;

#[derive(Debug)]
pub struct RemoteAdminMeta {
    config: ClusterConfig,
    data_nodes: RwLock<HashMap<u64, NodeInfo>>,
    conn_map: RwLock<HashMap<u64, Channel>>,

    client: MetaHttpClient,
    path: String,
}

impl RemoteAdminMeta {
    pub fn new(config: ClusterConfig, storage_path: String) -> Self {
        let meta_url = config.meta_service_addr.clone();

        Self {
            config,
            conn_map: RwLock::new(HashMap::new()),
            data_nodes: RwLock::new(HashMap::new()),
            client: MetaHttpClient::new(meta_url),
            path: storage_path,
        }
    }

    async fn sync_all_data_node(&self) -> MetaResult<u64> {
        let req = command::ReadCommand::DataNodes(self.config.name.clone());
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
        let mut disk_free_ = 0;

        match get_disk_info(&self.path) {
            Ok(disk_free) => disk_free_ = disk_free,
            Err(e) => info!("{}", e),
        }

        let is_cold_server = self.config.cold_data_server;

        let node = NodeInfo {
            status: 0,
            id: self.config.node_id,
            disk_free: disk_free_,
            is_cold: is_cold_server,
            grpc_addr: self.config.grpc_listen_addr.clone(),
            http_addr: self.config.http_listen_addr.clone(),
        };

        let req = command::WriteCommand::AddDataNode(self.config.name.clone(), node.clone());
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
        let req = command::WriteCommand::RetainID(self.config.name.clone(), count);
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

    fn heartbeat(&self) {}
}
