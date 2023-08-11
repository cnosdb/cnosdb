use async_trait::async_trait;
use openraft::error::{InstallSnapshotError, NetworkError, RemoteError};
use openraft::network::{RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use serde::de::DeserializeOwned;
use serde::Serialize;
use tracing::info;

use crate::{RaftNodeId, RaftNodeInfo, TypeConfig};

pub struct NetworkClient {}

impl NetworkClient {
    pub async fn send_rpc<Req, Resp, Err>(
        &self,
        target: RaftNodeId,
        target_node: &RaftNodeInfo,
        uri: &str,
        req: Req,
    ) -> Result<Resp, openraft::error::RPCError<RaftNodeId, RaftNodeInfo, Err>>
    where
        Req: Serialize,
        Err: std::error::Error + DeserializeOwned,
        Resp: DeserializeOwned,
    {
        let addr = &target_node.address;

        let url = format!("http://{}/{}", addr, uri);
        tracing::debug!("send_rpc to url: {}", url);

        let client = reqwest::Client::new();
        tracing::debug!("client is created for: {}", url);

        let resp = client
            .post(url)
            .json(&req)
            .send()
            .await
            .map_err(|e| openraft::error::RPCError::Network(NetworkError::new(&e)))?;

        tracing::debug!("client.post() is sent");

        let res: Result<Resp, Err> = resp
            .json()
            .await
            .map_err(|e| openraft::error::RPCError::Network(NetworkError::new(&e)))?;

        res.map_err(|e| openraft::error::RPCError::RemoteError(RemoteError::new(target, e)))
    }
}

// NOTE: This could be implemented also on `Arc<ExampleNetwork>`, but since it's empty, implemented
// directly.
#[async_trait]
impl RaftNetworkFactory<TypeConfig> for NetworkClient {
    type Network = NetworkConnection;

    async fn new_client(&mut self, target: RaftNodeId, node: &RaftNodeInfo) -> Self::Network {
        NetworkConnection {
            owner: NetworkClient {},
            target,
            target_node: node.clone(),
        }
    }
}

pub struct NetworkConnection {
    owner: NetworkClient,
    target: RaftNodeId,
    target_node: RaftNodeInfo,
}

type RaftError<E = openraft::error::Infallible> = openraft::error::RaftError<RaftNodeId, E>;
type RPCError<E = openraft::error::Infallible> =
    openraft::error::RPCError<RaftNodeId, RaftNodeInfo, RaftError<E>>;

#[async_trait]
impl RaftNetwork<TypeConfig> for NetworkConnection {
    async fn send_vote(
        &mut self,
        req: VoteRequest<RaftNodeId>,
    ) -> Result<VoteResponse<RaftNodeId>, RPCError> {
        info!(
            "Network callback send_vote target:{}, req: {:?}",
            self.target, req
        );

        self.owner
            .send_rpc(self.target, &self.target_node, "raft-vote", req)
            .await
    }

    async fn send_append_entries(
        &mut self,
        req: AppendEntriesRequest<TypeConfig>,
    ) -> Result<AppendEntriesResponse<RaftNodeId>, RPCError> {
        info!(
            "Network callback send_append_entries target:{}, req: {:?}",
            self.target, req
        );

        self.owner
            .send_rpc(self.target, &self.target_node, "raft-append", req)
            .await
    }

    async fn send_install_snapshot(
        &mut self,
        req: InstallSnapshotRequest<TypeConfig>,
    ) -> Result<InstallSnapshotResponse<RaftNodeId>, RPCError<InstallSnapshotError>> {
        info!(
            "Network callback send_install_snapshot target:{}, req: {:?}",
            self.target, req
        );

        self.owner
            .send_rpc(self.target, &self.target_node, "raft-snapshot", req)
            .await
    }
}
