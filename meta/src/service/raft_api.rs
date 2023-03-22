use std::collections::{BTreeMap, BTreeSet};

use actix_web::web::Data;
use actix_web::{get, post, web, Responder};
use openraft::async_trait::async_trait;
use openraft::error::{
    AddLearnerError, AppendEntriesError, ClientWriteError, Infallible, InitializeError,
    InstallSnapshotError, VoteError,
};
use openraft::raft::{
    AddLearnerResponse, AppendEntriesRequest, AppendEntriesResponse, ClientWriteResponse,
    InstallSnapshotRequest, InstallSnapshotResponse, VoteRequest, VoteResponse,
};
use openraft::RaftMetrics;
use web::Json;

use crate::service::{init_meta, MetaRaftApi};
use crate::{ClusterNode, ClusterNodeId, MetaApp, TypeConfig};

#[async_trait]
impl MetaRaftApi for MetaApp {
    async fn vote(
        &self,
        req: Json<VoteRequest<ClusterNodeId>>,
    ) -> Result<VoteResponse<ClusterNodeId>, VoteError<ClusterNodeId>> {
        self.raft.vote(req.0).await
    }

    async fn append(
        &self,
        req: Json<AppendEntriesRequest<TypeConfig>>,
    ) -> Result<AppendEntriesResponse<ClusterNodeId>, AppendEntriesError<ClusterNodeId>> {
        self.raft.append_entries(req.0).await
    }

    async fn snapshot(
        &self,
        req: Json<InstallSnapshotRequest<TypeConfig>>,
    ) -> Result<InstallSnapshotResponse<ClusterNodeId>, InstallSnapshotError<ClusterNodeId>> {
        self.raft.install_snapshot(req.0).await
    }

    async fn add_learner(
        &self,
        req: Json<(ClusterNodeId, String)>,
    ) -> Result<AddLearnerResponse<ClusterNodeId>, AddLearnerError<ClusterNodeId, ClusterNode>>
    {
        let node_id = req.0 .0;
        let node = ClusterNode {
            api_addr: req.0 .1.clone(),
            rpc_addr: req.0 .1.clone(),
        };
        self.raft.add_learner(node_id, node, true).await
    }

    async fn change_membership(
        &self,
        req: Json<BTreeSet<ClusterNodeId>>,
    ) -> Result<ClientWriteResponse<TypeConfig>, ClientWriteError<ClusterNodeId, ClusterNode>> {
        self.raft.change_membership(req.0, true, false).await
    }

    async fn init(&self) -> Result<(), InitializeError<ClusterNodeId, ClusterNode>> {
        let mut nodes = BTreeMap::new();
        nodes.insert(
            self.id,
            ClusterNode {
                rpc_addr: self.http_addr.clone(),
                api_addr: self.http_addr.clone(),
            },
        );
        let res = self.raft.initialize(nodes).await;
        init_meta(self).await;
        res
    }

    async fn metrics(&self) -> Result<RaftMetrics<ClusterNodeId, ClusterNode>, Infallible> {
        let metric0s = self.raft.metrics().borrow().clone();
        let res: Result<RaftMetrics<ClusterNodeId, ClusterNode>, Infallible> = Ok(metric0s);
        res
    }
}

#[post("/raft-vote")]
pub async fn vote(
    app: Data<MetaApp>,
    req: Json<VoteRequest<ClusterNodeId>>,
) -> actix_web::Result<impl Responder> {
    let app = app.as_ref() as &dyn MetaRaftApi;
    let res = app.vote(req).await;
    Ok(Json(res))
}

#[post("/raft-append")]
pub async fn append(
    app: Data<MetaApp>,
    req: Json<AppendEntriesRequest<TypeConfig>>,
) -> actix_web::Result<impl Responder> {
    let app = app.as_ref() as &dyn MetaRaftApi;
    let res = app.append(req).await;
    Ok(Json(res))
}

#[post("/raft_snapshot")]
pub async fn snapshot(
    app: Data<MetaApp>,
    req: Json<InstallSnapshotRequest<TypeConfig>>,
) -> actix_web::Result<impl Responder> {
    let app = app.as_ref() as &dyn MetaRaftApi;
    let res = app.snapshot(req).await;
    Ok(Json(res))
}

#[post("/add-learner")]
pub async fn add_learner(
    app: Data<MetaApp>,
    req: Json<(ClusterNodeId, String)>,
) -> actix_web::Result<impl Responder> {
    let app = app.as_ref() as &dyn MetaRaftApi;
    let res = app.add_learner(req).await;
    Ok(Json(res))
}

#[post("/change-membership")]
pub async fn change_membership(
    app: Data<MetaApp>,
    req: Json<BTreeSet<ClusterNodeId>>,
) -> actix_web::Result<impl Responder> {
    let app = app.as_ref() as &dyn MetaRaftApi;
    let res = app.change_membership(req).await;
    Ok(Json(res))
}

#[post("/init")]
pub async fn init(app: Data<MetaApp>) -> actix_web::Result<impl Responder> {
    let app = app.as_ref() as &dyn MetaRaftApi;
    let res = app.init().await;
    Ok(Json(res))
}

#[get("/metrics")]
pub async fn metrics(app: Data<MetaApp>) -> actix_web::Result<impl Responder> {
    let app = app.as_ref() as &dyn MetaRaftApi;
    let res = app.metrics().await;
    Ok(Json(res))
}
