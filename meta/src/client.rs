use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use openraft::error::{ClientWriteError, ForwardToLeader, NetworkError, RPCError, RemoteError};
use openraft::raft::ClientWriteResponse;
use openraft::AnyError;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use crate::error::{MetaError, MetaResult};
use crate::limiter::local_request_limiter::{LocalBucketRequest, LocalBucketResponse};
use crate::store::command::*;
use crate::store::state_machine::CommandResp;
use crate::{ClusterNode, ClusterNodeId, TypeConfig};

pub type WriteError =
    RPCError<ClusterNodeId, ClusterNode, ClientWriteError<ClusterNodeId, ClusterNode>>;

#[derive(Debug, Clone)]
pub struct MetaHttpClient {
    inner: Arc<reqwest::Client>,
    addrs: Vec<String>,
    pub leader: Arc<Mutex<String>>,
}

impl MetaHttpClient {
    pub fn new(addr: String) -> Self {
        let mut addrs = vec![];
        let list: Vec<&str> = addr.split(';').collect();
        for item in list.iter() {
            addrs.push(item.to_string());
        }
        addrs.sort();
        let leader_addr = addrs[0].clone();

        Self {
            inner: Arc::new(reqwest::Client::new()),
            addrs,
            leader: Arc::new(Mutex::new(leader_addr)),
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

    pub async fn watch<T>(&self, req: &(String, String, HashSet<String>, u64)) -> MetaResult<T>
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

    fn switch_leader(&self) {
        let mut t = self.leader.lock().unwrap();

        if let Ok(index) = self.addrs.binary_search(&t) {
            let index = (index + 1) % self.addrs.len();
            *t = self.addrs[index].clone();
        } else {
            *t = self.addrs[0].clone();
        }
    }

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

        let ttl = tokio::time::Duration::from_secs(60);
        loop {
            let res: Result<Resp, RPCError<ClusterNodeId, ClusterNode, Err>> =
                match tokio::time::timeout(ttl, self.do_send_rpc_to_leader(uri, req)).await {
                    Ok(res) => res,
                    Err(timeout) => Err(RPCError::Network(NetworkError::new(&AnyError::error(
                        format!("http call meta: {} timeout, {}", uri, timeout),
                    )))),
                };

            let rpc_err = match res {
                Ok(x) => return Ok(x),
                Err(rpc_err) => rpc_err,
            };

            if let RPCError::RemoteError(remote_err) = &rpc_err {
                let forward_err_res = <Err as TryInto<
                    ForwardToLeader<ClusterNodeId, ClusterNode>,
                >>::try_into(remote_err.source.clone());

                if let Ok(ForwardToLeader {
                    leader_id: Some(_),
                    leader_node: Some(leader_node),
                    ..
                }) = forward_err_res
                {
                    {
                        let mut t = self.leader.lock().unwrap();
                        *t = leader_node.api_addr;
                    }

                    n_retry -= 1;
                    if n_retry > 0 {
                        continue;
                    }
                } else {
                    self.switch_leader();
                }
            } else {
                self.switch_leader();
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
        let url = format!("http://{}/{}", self.leader.lock().unwrap(), uri);

        let resp = if let Some(r) = req {
            self.inner.post(url.clone()).json(r)
        } else {
            self.inner.get(url.clone())
        }
        .send()
        .await
        .map_err(|e| RPCError::Network(NetworkError::new(&e)))?;

        let res: Result<Resp, Err> = resp
            .json()
            .await
            .map_err(|e| RPCError::Network(NetworkError::new(&e)))?;

        res.map_err(|e| RPCError::RemoteError(RemoteError::new(0, e)))
    }

    pub async fn limiter_request(
        &self,
        cluster: &str,
        tenant: &str,
        request: LocalBucketRequest,
    ) -> MetaResult<LocalBucketResponse> {
        let req = WriteCommand::LimiterRequest {
            cluster: cluster.to_string(),
            tenant: tenant.to_string(),
            request,
        };
        self.write::<LocalBucketResponse>(&req).await
    }
}

#[cfg(test)]
mod test {
    use std::collections::HashSet;
    use std::{thread, time};

    use models::meta_data::{NodeAttribute, NodeInfo, VnodeAllInfo, VnodeInfo, VnodeStatus};
    use models::schema::DatabaseSchema;
    use tokio::sync::mpsc::channel;
    use tokio::time::timeout;

    use crate::client::MetaHttpClient;
    use crate::store::command::{self, UpdateVnodeArgs, UpdateVnodeReplSetArgs};

    #[tokio::test]
    #[ignore]
    async fn test_meta_client() {
        let cluster = "cluster_xxx".to_string();
        let tenant = "tenant_test".to_string();

        //let hand = tokio::spawn(watch_tenant("cluster_xxx", "tenant_test"));

        let client = MetaHttpClient::new("127.0.0.1:8901".to_string());

        let req = command::ReadCommand::TenaneMetaData(cluster.clone(), "cnosdb".to_string());
        let rsp = client
            .read::<command::TenaneMetaDataResp>(&req)
            .await
            .unwrap();
        println!("read tenant data: {}", serde_json::to_string(&rsp).unwrap());

        let node = NodeInfo {
            id: 111,
            attribute: NodeAttribute::Hot,
            grpc_addr: "".to_string(),
            http_addr: "127.0.0.1:8888".to_string(),
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
            del_info: vec![VnodeInfo::new(11, 0)],
            add_info: vec![VnodeInfo::new(333, 1333), VnodeInfo::new(444, 1444)],
        };

        let req = command::WriteCommand::UpdateVnodeReplSet(args);

        let client = MetaHttpClient::new("127.0.0.1:8901".to_string());
        let rsp = client.write::<command::StatusResponse>(&req).await;
        println!("=========: {:?}", rsp);
    }

    #[tokio::test]
    #[ignore]
    async fn test_update_vnode() {
        let cluster = "cluster_xxx".to_string();

        let mut args = UpdateVnodeArgs {
            cluster,
            vnode_info: VnodeAllInfo::default(),
        };

        args.vnode_info.status = VnodeStatus::Broken;

        let req = command::WriteCommand::UpdateVnode(args);

        let client = MetaHttpClient::new("127.0.0.1:8901".to_string());
        let rsp = client.write::<command::StatusResponse>(&req).await;
        println!("=========: {:?}", rsp);
    }

    #[tokio::test]
    #[ignore]
    async fn test_meta_watch() {
        let mut request = (
            "client_123".to_string(),
            "cluster_xx".to_string(),
            HashSet::from(["tenant_xx".to_string()]),
            0,
        );

        let client = MetaHttpClient::new("127.0.0.1:8901".to_string());
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
    #[ignore]
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

    #[tokio::test]
    #[ignore]
    async fn test_timeout() {
        let (send, mut recv) = channel(1024);
        send.send(123).await.unwrap();
        loop {
            match timeout(tokio::time::Duration::from_secs(3), recv.recv()).await {
                Ok(val) => println!("recv data: {:?}", val),
                Err(tt) => println!("err timeout: {}", tt),
            }
        }
    }
}
