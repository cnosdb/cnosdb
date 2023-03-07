use async_trait::async_trait;
use openraft::error::{
    AppendEntriesError, InstallSnapshotError, NetworkError, RPCError, RemoteError, VoteError,
};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use openraft::{RaftNetwork, RaftNetworkFactory};
use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::{ClusterNode, ClusterNodeId, TypeConfig};

pub struct Connections {
    // pub inner: Arc<HashMap<String,Channel>>,
    inner: reqwest::Client,
}
// impl Connections {
//     pub async fn add_conn(&mut self, url: &String) -> MetaResult<Channel>{
//         let channel = Channel::from_static(url.parse().unwrap())
//              .connect()
//              .await
//              .context(MetaError::RaftConnectSnafu)?;
//         self.inner.insert(url.clone(), channel.clone());
//         Ok(channel)
//     }
//     pub async fn get_conn(&mut self, url: String) -> MetaResult<Channel> {
//         match self.inner.get(&url){
//             None => self.add_conn(&url).await,
//             Some(c) => Ok(c.clone()),
//         }
//     }
// }

impl Default for Connections {
    fn default() -> Self {
        Self::new()
    }
}

impl Connections {
    pub fn new() -> Self {
        Self {
            inner: reqwest::Client::new(),
        }
    }

    pub async fn send_req<Req, Resp, Err>(
        &mut self,
        target: ClusterNodeId,
        node: ClusterNode,
        uri: &str,
        req: Req,
    ) -> Result<Resp, RPCError<ClusterNodeId, ClusterNode, Err>>
    where
        Req: Serialize,
        Err: std::error::Error + DeserializeOwned,
        Resp: DeserializeOwned,
    {
        let url = format!("http://{}/{}", node.rpc_addr, uri);
        let resp = self
            .inner
            .post(url)
            .json(&req)
            .send()
            .await
            .map_err(|e| RPCError::Network(NetworkError::new(&e)))?;

        let res: Result<Resp, Err> = resp
            .json()
            .await
            .map_err(|e| RPCError::Network(NetworkError::new(&e)))?;

        res.map_err(|e| RPCError::RemoteError(RemoteError::new(target, e)))
    }
}

#[async_trait]
impl RaftNetworkFactory<TypeConfig> for Connections {
    type Network = ConnManager;
    type ConnectionError = NetworkError;

    async fn new_client(
        &mut self,
        target: ClusterNodeId,
        node: &ClusterNode,
    ) -> Result<Self::Network, Self::ConnectionError> {
        Ok(ConnManager {
            //todo: use grpc
            owner: Connections::new(),
            target,
            target_node: node.clone(),
        })
    }
}

pub struct ConnManager {
    owner: Connections,
    target: ClusterNodeId,
    target_node: ClusterNode,
}

#[async_trait]
impl RaftNetwork<TypeConfig> for ConnManager {
    async fn send_append_entries(
        &mut self,
        req: AppendEntriesRequest<TypeConfig>,
    ) -> Result<
        AppendEntriesResponse<ClusterNodeId>,
        RPCError<ClusterNodeId, ClusterNode, AppendEntriesError<ClusterNodeId>>,
    > {
        // tracing::info!("send_append_entries: req {:?}", req);
        self.owner
            .send_req(self.target, self.target_node.clone(), "raft-append", req)
            .await
    }

    async fn send_install_snapshot(
        &mut self,
        req: InstallSnapshotRequest<TypeConfig>,
    ) -> Result<
        InstallSnapshotResponse<ClusterNodeId>,
        RPCError<ClusterNodeId, ClusterNode, InstallSnapshotError<ClusterNodeId>>,
    > {
        // tracing::info!("send_install_snapshot: req {:?}", req);
        self.owner
            .send_req(self.target, self.target_node.clone(), "raft_snapshot", req)
            .await
    }

    async fn send_vote(
        &mut self,
        req: VoteRequest<ClusterNodeId>,
    ) -> Result<
        VoteResponse<ClusterNodeId>,
        RPCError<ClusterNodeId, ClusterNode, VoteError<ClusterNodeId>>,
    > {
        self.owner
            .send_req(self.target, self.target_node.clone(), "raft-vote", req)
            .await
    }
}
