use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use openraft::error::AppendEntriesError;
use openraft::error::InstallSnapshotError;
use openraft::error::NetworkError;
use openraft::error::RPCError;
use openraft::error::RemoteError;
use openraft::error::VoteError;
use openraft::raft::AppendEntriesRequest;
use openraft::raft::AppendEntriesResponse;
use openraft::raft::InstallSnapshotRequest;
use openraft::raft::InstallSnapshotResponse;
use openraft::raft::VoteRequest;
use openraft::raft::VoteResponse;
use openraft::Node;
use openraft::RaftNetwork;
use openraft::RaftNetworkFactory;
use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::ExampleTypeConfig;
use crate::NodeId;

pub struct Connections {
    pub clients: Arc<HashMap<String, reqwest::Client>>,
}

impl Default for Connections {
    fn default() -> Self {
        Self::new()
    }
}

impl Connections {
    pub fn new() -> Self {
        Self {
            clients: Arc::new(HashMap::new()),
        }
    }

    pub async fn send_rpc<Req, Resp, Err>(
        &mut self,
        target: NodeId,
        target_node: Option<&Node>,
        uri: &str,
        req: Req,
    ) -> Result<Resp, RPCError<ExampleTypeConfig, Err>>
    where
        Req: Serialize,
        Err: std::error::Error + DeserializeOwned,
        Resp: DeserializeOwned,
    {
        let addr = target_node.map(|x| &x.addr).unwrap();
        let url = format!("http://{}/{}", addr, uri);
        let clients = Arc::get_mut(&mut self.clients).unwrap();
        let client = clients.entry(url.clone()).or_insert(reqwest::Client::new());

        let resp = client
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

// NOTE: This could be implemented also on `Arc<ExampleNetwork>`, but since it's empty, implemented directly.
#[async_trait]
impl RaftNetworkFactory<ExampleTypeConfig> for Connections {
    type Network = ExampleNetworkConnection;

    async fn connect(&mut self, target: NodeId, node: Option<&Node>) -> Self::Network {
        ExampleNetworkConnection {
            owner: Connections::new(),
            target,
            target_node: node.cloned(),
        }
    }
}

pub struct ExampleNetworkConnection {
    owner: Connections,
    target: NodeId,
    target_node: Option<Node>,
}

#[async_trait]
impl RaftNetwork<ExampleTypeConfig> for ExampleNetworkConnection {
    async fn send_append_entries(
        &mut self,
        req: AppendEntriesRequest<ExampleTypeConfig>,
    ) -> Result<
        AppendEntriesResponse<NodeId>,
        RPCError<ExampleTypeConfig, AppendEntriesError<NodeId>>,
    > {
        self.owner
            .send_rpc(self.target, self.target_node.as_ref(), "raft-append", req)
            .await
    }

    async fn send_install_snapshot(
        &mut self,
        req: InstallSnapshotRequest<ExampleTypeConfig>,
    ) -> Result<
        InstallSnapshotResponse<NodeId>,
        RPCError<ExampleTypeConfig, InstallSnapshotError<NodeId>>,
    > {
        self.owner
            .send_rpc(self.target, self.target_node.as_ref(), "raft-snapshot", req)
            .await
    }

    async fn send_vote(
        &mut self,
        req: VoteRequest<NodeId>,
    ) -> Result<VoteResponse<NodeId>, RPCError<ExampleTypeConfig, VoteError<NodeId>>> {
        self.owner
            .send_rpc(self.target, self.target_node.as_ref(), "raft-vote", req)
            .await
    }
}
