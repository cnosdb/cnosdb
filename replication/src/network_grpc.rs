use std::sync::Arc;

use openraft::raft::*;
use protos::raft_service::raft_service_server::RaftService;
use protos::raft_service::*;
use tokio::sync::RwLock;

use crate::multi_raft::MultiRaft;
use crate::raft_node::RaftNode;
use crate::{RaftNodeId, TypeConfig};

#[derive(Clone)]
pub struct RaftCBServer {
    nodes: Arc<RwLock<MultiRaft>>,
}

impl RaftCBServer {
    pub fn new(nodes: Arc<RwLock<MultiRaft>>) -> Self {
        Self { nodes }
    }

    async fn get_node(&self, group_id: u32) -> std::result::Result<Arc<RaftNode>, tonic::Status> {
        let node = self
            .nodes
            .read()
            .await
            .get_node(group_id)
            .ok_or(tonic::Status::new(
                tonic::Code::Internal,
                format!("Not Found Raft Node for Group: {}", group_id),
            ))?;

        Ok(node)
    }
}

#[tonic::async_trait]
impl RaftService for RaftCBServer {
    async fn raft_vote(
        &self,
        request: tonic::Request<RaftVoteReq>,
    ) -> std::result::Result<tonic::Response<RaftResponse>, tonic::Status> {
        let inner = request.into_inner();

        let vote = match serde_json::from_str::<VoteRequest<RaftNodeId>>(&inner.data) {
            Ok(val) => val,
            Err(err) => return Err(tonic::Status::new(tonic::Code::Internal, err.to_string())),
        };

        let node = self.get_node(inner.group_id).await?;
        let res = node.raw_raft().vote(vote).await;
        let data = serde_json::to_string(&res).unwrap_or("encode vote rsp failed".to_string());

        Ok(tonic::Response::new(RaftResponse { code: 0, data }))
    }

    async fn raft_snapshot(
        &self,
        request: tonic::Request<RaftSnapshotReq>,
    ) -> std::result::Result<tonic::Response<RaftResponse>, tonic::Status> {
        let inner = request.into_inner();

        let snapshot = match bincode::deserialize::<InstallSnapshotRequest<TypeConfig>>(&inner.data)
        {
            Ok(val) => val,
            Err(err) => return Err(tonic::Status::new(tonic::Code::Internal, err.to_string())),
        };

        let node = self.get_node(inner.group_id).await?;
        let res = node.raw_raft().install_snapshot(snapshot).await;
        let data = serde_json::to_string(&res).unwrap_or("encode vote rsp failed".to_string());

        Ok(tonic::Response::new(RaftResponse { code: 0, data }))
    }

    async fn raft_append_entries(
        &self,
        request: tonic::Request<RaftAppendEntriesReq>,
    ) -> std::result::Result<tonic::Response<RaftResponse>, tonic::Status> {
        let inner = request.into_inner();

        let entries = match bincode::deserialize::<AppendEntriesRequest<TypeConfig>>(&inner.data) {
            Ok(val) => val,
            Err(err) => return Err(tonic::Status::new(tonic::Code::Internal, err.to_string())),
        };

        let node = self.get_node(inner.group_id).await?;
        let res = node.raw_raft().append_entries(entries).await;
        let data = serde_json::to_string(&res).unwrap_or("encode vote rsp failed".to_string());

        Ok(tonic::Response::new(RaftResponse { code: 0, data }))
    }
}
