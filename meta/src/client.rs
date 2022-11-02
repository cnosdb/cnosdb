use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::Mutex;

use models::meta_data::NodeInfo;
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
use serde::de::DeserializeOwned;
use serde::Deserialize;
use serde::Serialize;

use crate::store::KvResp;
use crate::ExampleTypeConfig;
use crate::KvReq;
use crate::NodeId;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Empty {}

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

    pub fn write(
        &self,
        req: &KvReq,
    ) -> Result<KvResp, RPCError<ExampleTypeConfig, ClientWriteError<NodeId>>> {
        self.send_rpc_to_leader("write", Some(req))
    }

    pub fn read_tenant_meta(
        &self,
        req: &(String, String),
    ) -> Result<KvResp, RPCError<ExampleTypeConfig, Infallible>> {
        self.do_send_rpc_to_leader("read", Some(req))
    }

    pub fn read_data_nodes(
        &self,
        req: &String,
    ) -> Result<Vec<NodeInfo>, RPCError<ExampleTypeConfig, Infallible>> {
        self.do_send_rpc_to_leader("data_nodes", Some(req))
    }

    //////////////////////////////////////////////////
    pub fn consistent_read(
        &self,
        req: &String,
    ) -> Result<String, RPCError<ExampleTypeConfig, CheckIsLeaderError<NodeId>>> {
        self.do_send_rpc_to_leader("consistent_read", Some(req))
    }

    pub fn init(&self) -> Result<(), RPCError<ExampleTypeConfig, InitializeError<NodeId>>> {
        self.do_send_rpc_to_leader("init", Some(&Empty {}))
    }

    pub fn add_learner(
        &self,
        req: (NodeId, String),
    ) -> Result<AddLearnerResponse<NodeId>, RPCError<ExampleTypeConfig, AddLearnerError<NodeId>>>
    {
        self.send_rpc_to_leader("add-learner", Some(&req))
    }

    pub fn change_membership(
        &self,
        req: &BTreeSet<NodeId>,
    ) -> Result<
        ClientWriteResponse<ExampleTypeConfig>,
        RPCError<ExampleTypeConfig, ClientWriteError<NodeId>>,
    > {
        self.send_rpc_to_leader("change-membership", Some(req))
    }

    pub fn metrics(
        &self,
    ) -> Result<RaftMetrics<ExampleTypeConfig>, RPCError<ExampleTypeConfig, Infallible>> {
        self.do_send_rpc_to_leader("metrics", None::<&()>)
    }

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
