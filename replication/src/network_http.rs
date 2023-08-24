use std::collections::BTreeSet;
use std::convert::Infallible;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use warp::{hyper, Filter};

use crate::errors::ReplicationError;
use crate::raft_node::RaftNode;
use crate::{RaftNodeId, RaftNodeInfo};

async fn handle_rejection(
    err: warp::Rejection,
) -> Result<impl warp::Reply, std::convert::Infallible> {
    let reason = format!("{:?}", err);

    Ok(warp::Reply::into_response(reason))
}
pub struct RaftHttpAdmin {
    node: Arc<RaftNode>,
}

impl RaftHttpAdmin {
    pub fn new(node: Arc<RaftNode>) -> Self {
        Self { node }
    }

    fn with_raft_node(
        &self,
    ) -> impl Filter<Extract = (Arc<RaftNode>,), Error = Infallible> + Clone {
        let node = self.node.clone();

        warp::any().map(move || node.clone())
    }

    fn init_raft(
        &self,
    ) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
        warp::path!("init")
            .and(self.with_raft_node())
            .and_then(|node: Arc<RaftNode>| async move {
                let rsp = node
                    .raft_init()
                    .await
                    .map_or_else(|err| err.to_string(), |_| "Success".to_string());

                let res: Result<String, warp::Rejection> = Ok(rsp);

                res
            })
    }

    fn add_learner(
        &self,
    ) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
        warp::path!("add-learner")
            .and(warp::body::bytes())
            .and(self.with_raft_node())
            .and_then(|req: hyper::body::Bytes, node: Arc<RaftNode>| async move {
                let req: (RaftNodeId, String) = serde_json::from_slice(&req)
                    .map_err(ReplicationError::from)
                    .map_err(warp::reject::custom)?;

                let id = req.0;
                let addr = req.1;
                let info = RaftNodeInfo {
                    group_id: 2222,
                    address: addr,
                };

                let rsp = node
                    .raft_add_learner(id, info)
                    .await
                    .map_or_else(|err| err.to_string(), |_| "Success".to_string());

                let res: Result<String, warp::Rejection> = Ok(rsp);

                res
            })
    }

    fn change_membership(
        &self,
    ) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
        warp::path!("change-membership")
            .and(warp::body::bytes())
            .and(self.with_raft_node())
            .and_then(|req: hyper::body::Bytes, node: Arc<RaftNode>| async move {
                let req: BTreeSet<RaftNodeId> = serde_json::from_slice(&req)
                    .map_err(ReplicationError::from)
                    .map_err(warp::reject::custom)?;

                let rsp = node
                    .raft_change_membership(req)
                    .await
                    .map_or_else(|err| err.to_string(), |_| "Success".to_string());

                let res: Result<String, warp::Rejection> = Ok(rsp);

                res
            })
    }

    fn metrics(
        &self,
    ) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
        warp::path!("metrics")
            .and(self.with_raft_node())
            .map(|node: Arc<RaftNode>| {
                let status = node.raft_metrics();

                warp::reply::json(&status)
            })
    }

    pub fn routes(
        &self,
    ) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
        self.init_raft()
            .or(self.add_learner())
            .or(self.change_membership())
            .or(self.metrics())
    }

    async fn start(&self, addr: String) {
        tracing::info!("http server start addr: {}", addr);

        let addr: SocketAddr = addr.parse().unwrap();
        warp::serve(self.routes()).run(addr).await;
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
