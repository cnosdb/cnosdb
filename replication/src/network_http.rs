use std::collections::{BTreeMap, BTreeSet};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use axum::extract::{Json, State};
use axum::http::StatusCode;
use axum::{routing, Router};
use http_body::Frame;

use crate::raft_node::RaftNode;
use crate::{RaftNodeId, RaftNodeInfo};

pub fn create_router(raft_node: Arc<RaftNode>) -> Router {
    Router::new()
        .route("/init", routing::any(init_raft))
        .route("/add-learner", routing::any(add_learner))
        .route("/change-membership", routing::any(change_membership))
        .route("/metrics", routing::any(metrics))
        .with_state(raft_node)
}

async fn init_raft(State(raft_node): State<Arc<RaftNode>>) -> (StatusCode, String) {
    let ret = match raft_node.raft_init(BTreeMap::new()).await {
        Ok(_) => Ok(()),
        Err(err) => Err(err.to_string()),
    };
    match serde_json::to_string(&ret) {
        Ok(data) => (StatusCode::OK, data),
        Err(e) => {
            trace::error!("Failed to serialize response({ret:?}): {e}");
            (StatusCode::INTERNAL_SERVER_ERROR, String::new())
        }
    }
}

async fn add_learner(
    State(raft_node): State<Arc<RaftNode>>,
    Json((id, address)): Json<(RaftNodeId, String)>,
) -> (StatusCode, String) {
    let node = RaftNodeInfo {
        group_id: 2222, // TODO(zipper): why 2222 ?
        address,
    };

    let ret = raft_node.raw_raft().add_learner(id, node, true).await;
    match serde_json::to_string(&ret) {
        Ok(data) => (StatusCode::OK, data),
        Err(e) => {
            trace::error!("Failed to serialize response({ret:?}): {e}");
            (StatusCode::INTERNAL_SERVER_ERROR, String::new())
        }
    }
}

async fn change_membership(
    State(raft_node): State<Arc<RaftNode>>,
    Json(members): Json<BTreeSet<RaftNodeId>>,
) -> (StatusCode, String) {
    let ret = raft_node.raw_raft().change_membership(members, false).await;
    match serde_json::to_string(&ret) {
        Ok(data) => (StatusCode::OK, data),
        Err(e) => {
            trace::error!("Failed to serialize response({ret:?}): {e}");
            (StatusCode::INTERNAL_SERVER_ERROR, String::new())
        }
    }
}

async fn metrics(State(raft_node): State<Arc<RaftNode>>) -> (StatusCode, String) {
    let raft_metrics = raft_node.raft_metrics();
    match serde_json::to_string(&raft_metrics) {
        Ok(data) => (StatusCode::OK, data),
        Err(e) => {
            trace::error!("Failed to serialize response({raft_metrics:?}): {e}");
            (StatusCode::INTERNAL_SERVER_ERROR, String::new())
        }
    }
}

pub type SyncSendError = Box<dyn std::error::Error + Send + Sync + 'static>;

pub enum EitherBody<A, B> {
    Left(A),
    Right(B),
}

impl<A, B> EitherBody<A, B> {
    fn map_opt_ret<T, U: Into<SyncSendError>>(
        err: Option<Result<T, U>>,
    ) -> Option<Result<T, SyncSendError>> {
        err.map(|e| e.map_err(Into::into))
    }
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

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        match self.get_mut() {
            EitherBody::Left(b) => Pin::new(b).poll_frame(cx).map(Self::map_opt_ret),
            EitherBody::Right(b) => Pin::new(b).poll_frame(cx).map(Self::map_opt_ret),
        }
    }
}
