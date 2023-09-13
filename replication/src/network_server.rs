use std::collections::BTreeSet;
use std::convert::Infallible;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use openraft::raft::*;
use protos::raft_service::raft_service_server::RaftService;
use protos::raft_service::*;
use warp::{hyper, Filter};

use crate::errors::ReplicationError;
use crate::raft_node::RaftNode;
use crate::{RaftNodeId, RaftNodeInfo, Request, TypeConfig};

// ------------------------------------------------------------------------- //
#[derive(Clone)]
pub struct RaftCBServer {
    node: Arc<RaftNode>,
}

impl RaftCBServer {
    pub fn new(node: Arc<RaftNode>) -> Self {
        Self { node }
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

        let res = self.node.raw_raft().vote(vote).await;
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

        let res = self.node.raw_raft().install_snapshot(snapshot).await;
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

        let res = self.node.raw_raft().append_entries(entries).await;
        let data = serde_json::to_string(&res).unwrap_or("encode vote rsp failed".to_string());

        Ok(tonic::Response::new(RaftResponse { code: 0, data }))
    }
}

// ------------------------------------------------------------------------- //
pub type SyncSendError = Box<dyn std::error::Error + Send + Sync + 'static>;
pub enum EitherBody<A, B> {
    Left(A),
    Right(B),
}

impl<A, B> http_body::Body for EitherBody<A, B>
where
    A: http_body::Body + Send + Unpin,
    B: http_body::Body<Data = A::Data> + Send + Unpin,
    A::Error: Into<SyncSendError>,
    B::Error: Into<SyncSendError>,
{
    type Data = A::Data;
    type Error = Box<dyn std::error::Error + Send + Sync + 'static>;

    fn is_end_stream(&self) -> bool {
        match self {
            EitherBody::Left(b) => b.is_end_stream(),
            EitherBody::Right(b) => b.is_end_stream(),
        }
    }

    fn poll_data(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Self::Data, Self::Error>>> {
        match self.get_mut() {
            EitherBody::Left(b) => Pin::new(b).poll_data(cx).map(map_option_err),
            EitherBody::Right(b) => Pin::new(b).poll_data(cx).map(map_option_err),
        }
    }

    fn poll_trailers(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<Option<http::HeaderMap>, Self::Error>> {
        match self.get_mut() {
            EitherBody::Left(b) => Pin::new(b).poll_trailers(cx).map_err(Into::into),
            EitherBody::Right(b) => Pin::new(b).poll_trailers(cx).map_err(Into::into),
        }
    }
}

fn map_option_err<T, U: Into<SyncSendError>>(
    err: Option<Result<T, U>>,
) -> Option<Result<T, SyncSendError>> {
    err.map(|e| e.map_err(Into::into))
}
