use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::Mutex;

use openraft::error::AddLearnerError;
use openraft::error::CheckIsLeaderError;
use openraft::error::ClientWriteError;
use openraft::error::ForwardToLeader;
use openraft::error::Infallible;
use openraft::error::InitializeError;
use openraft::error::NetworkError;
use openraft::error::RPCError;
use openraft::error::RemoteError;
use openraft::raft::AddLearnerResponse;
use openraft::raft::ClientWriteResponse;
use openraft::RaftMetrics;
use reqwest::Client;
use serde::de::DeserializeOwned;
use serde::Deserialize;
use serde::Serialize;

use crate::NodeId;
use crate::KvReq;
use crate::ExampleTypeConfig;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Empty {}

pub struct MetaHttpClient {
    pub leader: Arc<Mutex<(NodeId, String)>>,
    pub inner: Client,
}

impl MetaHttpClient {
    pub fn new(leader_id: NodeId, leader_addr: String) -> Self {
        Self {
            leader: Arc::new(Mutex::new((leader_id, leader_addr))),
            inner: Client::new(),
        }
    }

    pub async fn write(
        &self,
        req: &KvReq,
    ) -> Result<
        ClientWriteResponse<ExampleTypeConfig>,
        RPCError<ExampleTypeConfig, ClientWriteError<NodeId>>,
    > {
        self.send_rpc_to_leader("write", Some(req)).await
    }

    pub async fn read(
        &self,
        req: &String,
    ) -> Result<String, RPCError<ExampleTypeConfig, Infallible>> {
        self.do_send_rpc_to_leader("read", Some(req)).await
    }

    pub async fn consistent_read(
        &self,
        req: &String,
    ) -> Result<String, RPCError<ExampleTypeConfig, CheckIsLeaderError<NodeId>>> {
        self.do_send_rpc_to_leader("consistent_read", Some(req))
            .await
    }

    pub async fn init(
        &self,
    ) -> Result<(), RPCError<ExampleTypeConfig, InitializeError<NodeId>>> {
        self.do_send_rpc_to_leader("init", Some(&Empty {})).await
    }

    pub async fn add_learner(
        &self,
        req: (NodeId, String),
    ) -> Result<
        AddLearnerResponse<NodeId>,
        RPCError<ExampleTypeConfig, AddLearnerError<NodeId>>,
    > {
        self.send_rpc_to_leader("add-learner", Some(&req)).await
    }

    pub async fn change_membership(
        &self,
        req: &BTreeSet<NodeId>,
    ) -> Result<
        ClientWriteResponse<ExampleTypeConfig>,
        RPCError<ExampleTypeConfig, ClientWriteError<NodeId>>,
    > {
        self.send_rpc_to_leader("change-membership", Some(req))
            .await
    }

    pub async fn metrics(
        &self,
    ) -> Result<RaftMetrics<ExampleTypeConfig>, RPCError<ExampleTypeConfig, Infallible>> {
        self.do_send_rpc_to_leader("metrics", None::<&()>).await
    }

    async fn do_send_rpc_to_leader<Req, Resp, Err>(
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

        res.map_err(|e| RPCError::RemoteError(RemoteError::new(leader_id, e)))
    }


    async fn send_rpc_to_leader<Req, Resp, Err>(
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
                self.do_send_rpc_to_leader(uri, req).await;

            let rpc_err = match res {
                Ok(x) => return Ok(x),
                Err(rpc_err) => rpc_err,
            };

            if let RPCError::RemoteError(remote_err) = &rpc_err {
                let forward_err_res = <Err as TryInto<ForwardToLeader<NodeId>>>::try_into(
                    remote_err.source.clone(),
                );

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
}
