use std::sync::Arc;
use std::sync::Mutex;

use openraft::error::ClientWriteError;
use openraft::error::ForwardToLeader;
use openraft::AnyError;

use openraft::error::NetworkError;
use openraft::error::RPCError;
use openraft::error::RemoteError;
use openraft::raft::ClientWriteResponse;

use serde::de::DeserializeOwned;
use serde::Deserialize;
use serde::Serialize;

use crate::error::{MetaError, MetaResult};
use crate::store::command::*;
use crate::store::state_machine::CommandResp;
use crate::{ClusterNode, ClusterNodeId, TypeConfig};

pub type WriteError =
    RPCError<ClusterNodeId, ClusterNode, ClientWriteError<ClusterNodeId, ClusterNode>>;

#[derive(Debug)]
pub struct MetaHttpClient {
    inner: surf::Client,
    pub leader: Arc<Mutex<(ClusterNodeId, String)>>,
}

impl MetaHttpClient {
    pub fn new(leader_id: ClusterNodeId, leader_addr: String) -> Self {
        Self {
            inner: surf::Client::new(),
            leader: Arc::new(Mutex::new((leader_id, leader_addr))),
        }
    }

    pub async fn read<T>(&self, req: &ReadCommand) -> MetaResult<T>
    where
        T: for<'a> Deserialize<'a>,
    {
        let rsp: CommandResp = self.send_rpc_to_leader("read", Some(req)).await?;

        let rsp = serde_json::from_str::<T>(&rsp).map_err(|err| MetaError::MetaClientErr {
            msg: err.to_string(),
        })?;

        Ok(rsp)
    }

    pub async fn write<T>(&self, req: &WriteCommand) -> MetaResult<T>
    where
        T: for<'a> Deserialize<'a>,
    {
        let rsp: ClientWriteResponse<TypeConfig> =
            self.send_rpc_to_leader("write", Some(req)).await?;

        let rsp = serde_json::from_str::<T>(&rsp.data).map_err(|err| MetaError::MetaClientErr {
            msg: err.to_string(),
        })?;

        Ok(rsp)
    }

    pub async fn watch<T>(&self, req: &(String, String, String, u64)) -> MetaResult<T>
    where
        T: for<'a> Deserialize<'a>,
    {
        let rsp: CommandResp = self.send_rpc_to_leader("watch", Some(req)).await?;

        let rsp = serde_json::from_str::<T>(&rsp).map_err(|err| MetaError::MetaClientErr {
            msg: err.to_string(),
        })?;

        Ok(rsp)
    }

    //////////////////////////////////////////////////

    async fn send_rpc_to_leader<Req, Resp, Err>(
        &self,
        uri: &str,
        req: Option<&Req>,
    ) -> Result<Resp, RPCError<ClusterNodeId, ClusterNode, Err>>
    where
        Req: Serialize + 'static,
        Resp: Serialize + DeserializeOwned,
        Err: std::error::Error
            + Serialize
            + DeserializeOwned
            + TryInto<ForwardToLeader<ClusterNodeId, ClusterNode>>
            + Clone,
    {
        let mut n_retry = 3;

        loop {
            let res: Result<Resp, RPCError<ClusterNodeId, ClusterNode, Err>> =
                self.do_send_rpc_to_leader(uri, req).await;

            let rpc_err = match res {
                Ok(x) => return Ok(x),
                Err(rpc_err) => rpc_err,
            };

            if let RPCError::RemoteError(remote_err) = &rpc_err {
                let forward_err_res = <Err as TryInto<
                    ForwardToLeader<ClusterNodeId, ClusterNode>,
                >>::try_into(remote_err.source.clone());

                if let Ok(ForwardToLeader {
                    leader_id: Some(leader_id),
                    leader_node: Some(leader_node),
                    ..
                }) = forward_err_res
                {
                    {
                        let mut t = self.leader.lock().unwrap();
                        *t = (leader_id, leader_node.api_addr);
                    }

                    n_retry -= 1;
                    if n_retry > 0 {
                        continue;
                    }
                }
            }

            return Err(rpc_err);
        }
    }

    async fn do_send_rpc_to_leader<Req, Resp, Err>(
        &self,
        uri: &str,
        req: Option<&Req>,
    ) -> Result<Resp, RPCError<ClusterNodeId, ClusterNode, Err>>
    where
        Req: Serialize + 'static,
        Resp: Serialize + DeserializeOwned,
        Err: std::error::Error + Serialize + DeserializeOwned,
    {
        let (leader_id, url) = {
            let t = self.leader.lock().unwrap();
            let target_addr = &t.1;
            (t.0, format!("http://{}/{}", target_addr, uri))
        };

        /*-------------------surf client--------------------------- */
        let mut resp = if let Some(r) = req {
            self.inner
                .post(url.clone())
                .body(surf::Body::from_json(r).unwrap())
        } else {
            self.inner.get(url.clone())
        }
        .await
        .map_err(|e| RPCError::Network(NetworkError::new(&AnyError::error(e.to_string()))))?;

        let res: Result<Resp, Err> = resp
            .body_json()
            .await
            .map_err(|e| RPCError::Network(NetworkError::new(&AnyError::error(e.to_string()))))?;

        res.map_err(|e| RPCError::RemoteError(RemoteError::new(leader_id, e)))

        /*-------------------reqwest client--------------------------- */
        // let resp = if let Some(r) = req {
        //     self.inner.post(url.clone()).json(r)
        // } else {
        //     self.inner.get(url.clone())
        // }
        // .send()
        // .await
        // .map_err(|e| RPCError::Network(NetworkError::new(&e)))?;

        // let res: Result<Resp, Err> = resp
        //     .json()
        //     .await
        //     .map_err(|e| RPCError::Network(NetworkError::new(&e)))?;

        // res.map_err(|e| RPCError::RemoteError(RemoteError::new(leader_id, e)))
    }
}

#[cfg(test)]
mod test {
    use crate::{
        client::MetaHttpClient,
        store::command::{self, UpdateVnodeReplSetArgs},
    };
    use std::{thread, time};

    use models::{
        meta_data::{NodeInfo, VnodeInfo},
        schema::DatabaseSchema,
    };

    #[tokio::test]
    #[ignore]
    async fn test_meta_client() {
        let cluster = "cluster_xxx".to_string();
        let tenant = "tenant_test".to_string();

        //let hand = tokio::spawn(watch_tenant("cluster_xxx", "tenant_test"));

        let client = MetaHttpClient::new(3, "127.0.0.1:21003".to_string());

        let node = NodeInfo {
            id: 111,
            tcp_addr: "".to_string(),
            http_addr: "127.0.0.1:8888".to_string(),
            status: 0,
        };

        let req = command::WriteCommand::AddDataNode(cluster.clone(), node);
        let rsp = client.write::<command::StatusResponse>(&req).await;
        println!("=== add data: {:?}", rsp);
        thread::sleep(time::Duration::from_secs(3));

        let req = command::WriteCommand::CreateDB(
            cluster.clone(),
            tenant.clone(),
            DatabaseSchema::new(&tenant, "test_db"),
        );
        let rsp = client.write::<command::TenaneMetaDataResp>(&req).await;
        println!("=== create db: {:?}", rsp);
        thread::sleep(time::Duration::from_secs(3));

        let req = command::WriteCommand::CreateBucket(
            cluster.clone(),
            tenant.clone(),
            "test_db".to_string(),
            1667456711000000000,
        );
        let rsp = client.write::<command::TenaneMetaDataResp>(&req).await;
        println!("=== create bucket: {:?}", rsp);
        thread::sleep(time::Duration::from_secs(3));

        let req = command::WriteCommand::CreateDB(
            cluster,
            tenant.clone(),
            DatabaseSchema::new(&tenant, "test_db2"),
        );
        let rsp = client.write::<command::TenaneMetaDataResp>(&req).await;
        println!("=== create db2: {:?}", rsp);
        thread::sleep(time::Duration::from_secs(3));

        // thread::sleep(time::Duration::from_secs(300));
    }

    #[tokio::test]
    #[ignore]
    async fn test_update_replication_set() {
        let cluster = "cluster_xxx".to_string();
        let tenant = "cnosdb".to_string();
        let db_name = "my_db".to_string();

        let args = UpdateVnodeReplSetArgs {
            cluster,
            tenant,
            db_name,
            bucket_id: 8,
            repl_id: 9,
            del_info: vec![VnodeInfo { id: 11, node_id: 0 }],
            add_info: vec![
                VnodeInfo {
                    id: 333,
                    node_id: 1333,
                },
                VnodeInfo {
                    id: 444,
                    node_id: 1444,
                },
            ],
        };

        let req = command::WriteCommand::UpdateVnodeReplSet(args);

        let client = MetaHttpClient::new(1, "127.0.0.1:21001".to_string());
        let rsp = client.write::<command::StatusResponse>(&req).await;
        println!("=========: {:?}", rsp);
    }

    #[tokio::test]
    #[ignore]
    async fn test_meta_watch() {
        let mut request = (
            "client_123".to_string(),
            "cluster_xx".to_string(),
            "tenant_xx".to_string(),
            0,
        );

        let client = MetaHttpClient::new(1, "127.0.0.1:21001".to_string());
        loop {
            let watch_data = client.watch::<command::WatchData>(&request).await.unwrap();
            println!("{:?}", watch_data);

            if !watch_data.entry_logs.is_empty() {
                println!("{}", watch_data.entry_logs[0].val)
            }

            request.3 = watch_data.max_ver;
        }
    }

    #[tokio::test]
    async fn test_json_encode() {
        let req = command::WriteCommand::CreateDB(
            "clusterxx".to_string(),
            "tenantxx".to_string(),
            DatabaseSchema::new("tenantxx", "test_db2"),
        );

        let strs: Vec<&str> = "/cluster/tenane/db".split('/').collect();
        let strs2 = &strs[1..];
        println!("{:?}", strs2);

        let data = serde_json::to_string(&req).unwrap();
        println!("{}", data);
    }
}
