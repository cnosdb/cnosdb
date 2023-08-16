use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use openraft::error::{InstallSnapshotError, NetworkError, RemoteError};
use openraft::network::{RaftNetwork, RaftNetworkFactory};
use openraft::raft::*;
use parking_lot::RwLock;
use protos::raft_service::raft_service_client::RaftServiceClient;
use protos::raft_service::*;
use tonic::transport::{Channel, Endpoint};
use tower::timeout::Timeout;
use tracing::info;

use crate::errors::{ReplicationError, ReplicationResult};
use crate::{RaftNodeId, RaftNodeInfo, TypeConfig};

// ------------------------------------------------------------------------- //
#[derive(Clone)]
pub struct NetworkConn {
    conn_map: Arc<RwLock<HashMap<String, Channel>>>,
}

impl Default for NetworkConn {
    fn default() -> Self {
        Self::new()
    }
}

impl NetworkConn {
    pub fn new() -> Self {
        Self {
            conn_map: Arc::new(RwLock::new(HashMap::new())),
        }
    }
    async fn get_conn(&self, addr: &str) -> ReplicationResult<Channel> {
        if let Some(val) = self.conn_map.read().get(addr) {
            return Ok(val.clone());
        }

        let connector = Endpoint::from_shared(format!("http://{}", addr)).map_err(|err| {
            ReplicationError::GRPCRequest {
                msg: err.to_string(),
            }
        })?;

        let channel = connector
            .connect()
            .await
            .map_err(|err| ReplicationError::GRPCRequest {
                msg: err.to_string(),
            })?;

        self.conn_map
            .write()
            .insert(addr.to_string(), channel.clone());

        Ok(channel)
    }
}

#[async_trait]
impl RaftNetworkFactory<TypeConfig> for NetworkConn {
    type Network = TargetClient;

    async fn new_client(&mut self, target: RaftNodeId, node: &RaftNodeInfo) -> Self::Network {
        TargetClient {
            target,
            conn: self.clone(),
            target_node: node.clone(),
        }
    }
}

// ------------------------------------------------------------------------- //
type RaftError<E = openraft::error::Infallible> = openraft::error::RaftError<RaftNodeId, E>;
type RPCError<E = openraft::error::Infallible> =
    openraft::error::RPCError<RaftNodeId, RaftNodeInfo, RaftError<E>>;

pub struct TargetClient {
    conn: NetworkConn,
    target: RaftNodeId,
    target_node: RaftNodeInfo,
}

#[async_trait]
impl RaftNetwork<TypeConfig> for TargetClient {
    async fn send_vote(
        &mut self,
        req: VoteRequest<RaftNodeId>,
    ) -> Result<VoteResponse<RaftNodeId>, RPCError> {
        info!(
            "Network callback send_vote target:{}, req: {:?}",
            self.target, req
        );

        let channel = self
            .conn
            .get_conn(&self.target_node.address)
            .await
            .map_err(|e| openraft::error::RPCError::Network(NetworkError::new(&e)))?;

        let timeout_channel = Timeout::new(channel, Duration::from_millis(3 * 1000));
        let mut client = RaftServiceClient::<Timeout<Channel>>::new(timeout_channel);

        let data = serde_json::to_string(&req)
            .map_err(|e| openraft::error::RPCError::Network(NetworkError::new(&e)))?;
        let cmd = tonic::Request::new(RaftVoteReq {
            data,
            group_id: self.target_node.group_id,
        });

        let rsp = client
            .raft_vote(cmd)
            .await
            .map_err(|e| openraft::error::RPCError::Network(NetworkError::new(&e)))?
            .into_inner();

        let res: Result<VoteResponse<u64>, RaftError> = serde_json::from_str(&rsp.data)
            .map_err(|e| openraft::error::RPCError::Network(NetworkError::new(&e)))?;

        res.map_err(|e| openraft::error::RPCError::RemoteError(RemoteError::new(self.target, e)))
    }

    async fn send_append_entries(
        &mut self,
        req: AppendEntriesRequest<TypeConfig>,
    ) -> Result<AppendEntriesResponse<RaftNodeId>, RPCError> {
        // info!(
        //     "Network callback send_append_entries target:{}, req: {:?}",
        //     self.target, req
        // );

        let channel = self
            .conn
            .get_conn(&self.target_node.address)
            .await
            .map_err(|e| openraft::error::RPCError::Network(NetworkError::new(&e)))?;

        let timeout_channel = Timeout::new(channel, Duration::from_millis(3 * 1000));
        let mut client = RaftServiceClient::<Timeout<Channel>>::new(timeout_channel);

        let data = bincode::serialize(&req)
            .map_err(|e| openraft::error::RPCError::Network(NetworkError::new(&e)))?;
        let cmd = tonic::Request::new(RaftAppendEntriesReq {
            data,
            group_id: self.target_node.group_id,
        });

        let rsp = client
            .raft_append_entries(cmd)
            .await
            .map_err(|e| openraft::error::RPCError::Network(NetworkError::new(&e)))?
            .into_inner();

        let res: Result<AppendEntriesResponse<u64>, RaftError> = serde_json::from_str(&rsp.data)
            .map_err(|e| openraft::error::RPCError::Network(NetworkError::new(&e)))?;

        res.map_err(|e| openraft::error::RPCError::RemoteError(RemoteError::new(self.target, e)))
    }

    async fn send_install_snapshot(
        &mut self,
        req: InstallSnapshotRequest<TypeConfig>,
    ) -> Result<InstallSnapshotResponse<RaftNodeId>, RPCError<InstallSnapshotError>> {
        info!(
            "Network callback send_install_snapshot target:{}, req: {:?}",
            self.target, req
        );

        let channel = self
            .conn
            .get_conn(&self.target_node.address)
            .await
            .map_err(|e| openraft::error::RPCError::Network(NetworkError::new(&e)))?;

        let timeout_channel = Timeout::new(channel, Duration::from_millis(3 * 1000));
        let mut client = RaftServiceClient::<Timeout<Channel>>::new(timeout_channel);

        let data = bincode::serialize(&req)
            .map_err(|e| openraft::error::RPCError::Network(NetworkError::new(&e)))?;
        let cmd = tonic::Request::new(RaftSnapshotReq {
            data,
            group_id: self.target_node.group_id,
        });

        let rsp = client
            .raft_snapshot(cmd)
            .await
            .map_err(|e| openraft::error::RPCError::Network(NetworkError::new(&e)))?
            .into_inner();

        let res: Result<InstallSnapshotResponse<u64>, RaftError<InstallSnapshotError>> =
            serde_json::from_str(&rsp.data)
                .map_err(|e| openraft::error::RPCError::Network(NetworkError::new(&e)))?;

        res.map_err(|e| openraft::error::RPCError::RemoteError(RemoteError::new(self.target, e)))
    }
}
