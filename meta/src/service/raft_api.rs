use std::collections::{BTreeMap, BTreeSet};

use actix_web::web::Data;
use actix_web::{get, post, web, Responder};
use openraft::error::Infallible;
use openraft::raft::{AppendEntriesRequest, InstallSnapshotRequest, VoteRequest};
use openraft::RaftMetrics;
use web::Json;

use crate::service::init_meta;
use crate::{ClusterNode, ClusterNodeId, MetaApp, TypeConfig};

#[post("/raft-vote")]
pub async fn vote(
    app: Data<MetaApp>,
    req: Json<VoteRequest<ClusterNodeId>>,
) -> actix_web::Result<impl Responder> {
    let res = app.raft.vote(req.0).await;
    // info!("raft-vote-----debug {:?}", &res);
    Ok(Json(res))
}

#[post("/raft-append")]
pub async fn append(
    app: Data<MetaApp>,
    req: Json<AppendEntriesRequest<TypeConfig>>,
) -> actix_web::Result<impl Responder> {
    let res = app.raft.append_entries(req.0).await;
    Ok(Json(res))
}

#[post("/raft_snapshot")]
pub async fn snapshot(
    app: Data<MetaApp>,
    req: Json<InstallSnapshotRequest<TypeConfig>>,
) -> actix_web::Result<impl Responder> {
    let res = app.raft.install_snapshot(req.0).await;
    Ok(Json(res))
}

#[post("/add-learner")]
pub async fn add_learner(
    app: Data<MetaApp>,
    req: Json<(ClusterNodeId, String)>,
) -> actix_web::Result<impl Responder> {
    let node_id = req.0 .0;
    let node = ClusterNode {
        api_addr: req.0 .1.clone(),
        rpc_addr: req.0 .1.clone(),
    };
    let res = app.raft.add_learner(node_id, node, true).await;
    Ok(Json(res))
}

#[post("/change-membership")]
pub async fn change_membership(
    app: Data<MetaApp>,
    req: Json<BTreeSet<ClusterNodeId>>,
) -> actix_web::Result<impl Responder> {
    let res = app.raft.change_membership(req.0, true, false).await;
    Ok(Json(res))
}

#[post("/init")]
pub async fn init(app: Data<MetaApp>) -> actix_web::Result<impl Responder> {
    let mut nodes = BTreeMap::new();
    nodes.insert(
        app.id,
        ClusterNode {
            rpc_addr: app.http_addr.clone(),
            api_addr: app.http_addr.clone(),
        },
    );
    let res = app.raft.initialize(nodes).await;
    init_meta(&app).await;
    Ok(Json(res))
}

#[get("/metrics")]
pub async fn metrics(app: Data<MetaApp>) -> actix_web::Result<impl Responder> {
    let metrics = app.raft.metrics().borrow().clone();

    let res: Result<RaftMetrics<ClusterNodeId, ClusterNode>, Infallible> = Ok(metrics);
    Ok(Json(res))
}
