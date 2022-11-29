use std::sync::Arc;
use std::sync::Mutex;

use openraft::error::ClientWriteError;
use openraft::error::ForwardToLeader;

use openraft::error::NetworkError;
use openraft::error::RPCError;
use openraft::error::RemoteError;

use serde::de::DeserializeOwned;
use serde::Deserialize;
use serde::Serialize;

use crate::meta_client::MetaError;
use crate::meta_client::MetaResult;
use crate::store::command::*;
use crate::store::state_machine::CommandResp;
use crate::ExampleTypeConfig;
use crate::NodeId;

pub type WriteError = RPCError<ExampleTypeConfig, ClientWriteError<NodeId>>;
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Empty {}

#[derive(Debug)]
pub struct MetaHttpClient {
    //inner: reqwest::Client,
    pub leader: Arc<Mutex<(NodeId, String)>>,
}

impl MetaHttpClient {
    pub fn new(leader_id: NodeId, leader_addr: String) -> Self {
        Self {
            //inner: reqwest::Client::new(),
            leader: Arc::new(Mutex::new((leader_id, leader_addr))),
        }
    }

    pub fn read<T>(&self, req: &ReadCommand) -> MetaResult<T>
    where
        T: for<'a> Deserialize<'a>,
    {
        let rsp = self.do_request("read", Some(req))?;

        let rsp = serde_json::from_str::<T>(&rsp).map_err(|err| MetaError::MetaClientErr {
            msg: err.to_string(),
        })?;

        Ok(rsp)
    }

    pub fn write<T>(&self, req: &WriteCommand) -> MetaResult<T>
    where
        T: for<'a> Deserialize<'a>,
    {
        let rsp = self.do_request("write", Some(req))?;

        let rsp = serde_json::from_str::<T>(&rsp).map_err(|err| MetaError::MetaClientErr {
            msg: err.to_string(),
        })?;

        Ok(rsp)
    }

    fn do_request<Req>(&self, uri: &str, req: Option<&Req>) -> Result<CommandResp, WriteError>
    where
        Req: Serialize + 'static,
    {
        self.send_rpc_to_leader(uri, req)
    }

    //////////////////////////////////////////////////

    fn send_rpc_to_leader<Req, Resp, Err>(
        &self,
        uri: &str,
        req: Option<&Req>,
    ) -> Result<Resp, RPCError<ExampleTypeConfig, Err>>
    where
        Req: Serialize + 'static,
        Resp: Serialize + DeserializeOwned,
        Err: std::error::Error
            + Serialize
            + DeserializeOwned
            + TryInto<ForwardToLeader<NodeId>>
            + Clone,
    {
        let mut n_retry = 3;

        loop {
            let res: Result<Resp, RPCError<ExampleTypeConfig, Err>> =
                self.do_send_rpc_to_leader(uri, req);

            let rpc_err = match res {
                Ok(x) => return Ok(x),
                Err(rpc_err) => rpc_err,
            };

            if let RPCError::RemoteError(remote_err) = &rpc_err {
                let forward_err_res =
                    <Err as TryInto<ForwardToLeader<NodeId>>>::try_into(remote_err.source.clone());

                if let Ok(ForwardToLeader {
                    leader_id: Some(leader_id),
                    leader_node: Some(leader_node),
                    ..
                }) = forward_err_res
                {
                    {
                        let mut t = self.leader.lock().unwrap();
                        *t = (leader_id, leader_node.addr);
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

    fn do_send_rpc_to_leader<Req, Resp, Err>(
        &self,
        uri: &str,
        req: Option<&Req>,
    ) -> Result<Resp, RPCError<ExampleTypeConfig, Err>>
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

        let resp = if let Some(r) = req {
            ureq::post(&url).send_json(r)
        } else {
            ureq::get(&url).call()
        }
        .map_err(|e| RPCError::Network(NetworkError::new(&e)))?;

        let res: Result<Resp, Err> = resp
            .into_json()
            .map_err(|e| RPCError::Network(NetworkError::new(&e)))?;

        res.map_err(|e| RPCError::RemoteError(RemoteError::new(leader_id, e)))
    }
}

#[cfg(test)]
mod test {
    use crate::{client::MetaHttpClient, store::command};

    use models::meta_data::NodeInfo;

    #[tokio::test]
    async fn test_meta_client() {
        let client = MetaHttpClient::new(1, "127.0.0.1:21001".to_string());

        let cluster = "cluster_xxx".to_string();
        let mut node = NodeInfo::default();
        node.id = 111;
        node.http_addr = "127.0.0.1:8888".to_string();
        let req = command::WriteCommand::AddDataNode(cluster.clone(), node.clone());
        let rsp = client.write::<command::StatusResponse>(&req);
        println!("{:?}", serde_json::to_string(&req).unwrap());
        println!("{:?}", rsp);

        let req = command::ReadCommand::DataNodes(cluster.clone());
        let rsp = client.read::<Vec<NodeInfo>>(&req);
        println!("{:?}", serde_json::to_string(&req).unwrap());
        println!("{:?}", rsp);
    }
}
