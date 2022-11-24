use actix_web::get;
use actix_web::post;
use actix_web::web;
use actix_web::web::Data;
use actix_web::Responder;
use openraft::error::Infallible;
use openraft::raft::AppendEntriesRequest;
use openraft::raft::InstallSnapshotRequest;
use openraft::raft::VoteRequest;
use openraft::Node;
use openraft::RaftMetrics;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use web::Json;

use crate::meta_app::MetaApp;
use crate::ExampleTypeConfig;
use crate::NodeId;

#[post("/raft-vote")]
pub async fn vote(
    app: Data<MetaApp>,
    req: Json<VoteRequest<NodeId>>,
) -> actix_web::Result<impl Responder> {
    let res = app.raft.vote(req.0).await;
    Ok(Json(res))
}

#[post("/raft-append")]
pub async fn append(
    app: Data<MetaApp>,
    req: Json<AppendEntriesRequest<ExampleTypeConfig>>,
) -> actix_web::Result<impl Responder> {
    let res = app.raft.append_entries(req.0).await;
    Ok(Json(res))
}

#[post("/raft-snapshot")]
pub async fn snapshot(
    app: Data<MetaApp>,
    req: Json<InstallSnapshotRequest<ExampleTypeConfig>>,
) -> actix_web::Result<impl Responder> {
    let res = app.raft.install_snapshot(req.0).await;
    Ok(Json(res))
}

#[post("/add-learner")]
pub async fn add_learner(
    app: Data<MetaApp>,
    req: Json<(NodeId, String)>,
) -> actix_web::Result<impl Responder> {
    let node_id = req.0 .0;
    let node = Node {
        addr: req.0 .1.clone(),
        ..Default::default()
    };
    let res = app.raft.add_learner(node_id, Some(node), true).await;
    Ok(Json(res))
}

#[post("/change-membership")]
pub async fn change_membership(
    app: Data<MetaApp>,
    req: Json<BTreeSet<NodeId>>,
) -> actix_web::Result<impl Responder> {
    let res = app.raft.change_membership(req.0, true, false).await;
    Ok(Json(res))
}

#[post("/init")]
pub async fn init(app: Data<MetaApp>) -> actix_web::Result<impl Responder> {
    let mut nodes = BTreeMap::new();
    nodes.insert(
        app.id,
        Node {
            addr: app.addr.clone(),
            data: Default::default(),
        },
    );
    let res = app.raft.initialize(nodes).await;
    Ok(Json(res))
}

#[get("/metrics")]
pub async fn metrics(app: Data<MetaApp>) -> actix_web::Result<impl Responder> {
    let metrics = app.raft.metrics().borrow().clone();

    let res: Result<RaftMetrics<ExampleTypeConfig>, Infallible> = Ok(metrics);
    Ok(Json(res))
}
