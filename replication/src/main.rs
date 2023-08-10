use std::collections::BTreeSet;
use std::sync::Arc;

use actix_web::web::{Data, Json};
use actix_web::{get, post, Responder};
use clap::Parser;
use openraft::error::Infallible;
use openraft::raft::{AppendEntriesRequest, InstallSnapshotRequest, VoteRequest};
use replication::apply_store::{ApplyStorageRef, ExampleApplyStorage};
use replication::entry_store::{EntryStorageRef, ExampleEntryStorage};
use replication::errors::ReplicationResult;
use replication::node_store::NodeStorage;
use replication::raft_node::RaftNode;
use replication::state_store::StateStorage;
use replication::{RaftNodeId, RaftNodeInfo, Request, TypeConfig};
use tracing::info;

#[derive(Parser, Clone, Debug)]
#[clap(author, version, about, long_about = None)]
pub struct Opt {
    #[clap(long)]
    pub id: u64,

    #[clap(long)]
    pub http_addr: String,
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let options = Opt::parse();

    let subscriber = tracing_subscriber::FmtSubscriber::new();
    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");

    info!("Service Start Option: {:?}", options);

    start_raft_node(options.id, options.http_addr)
        .await
        .unwrap();

    Ok(())
}

async fn start_raft_node(id: RaftNodeId, http_addr: String) -> ReplicationResult<()> {
    let path = format!("/tmp/cnosdb/{}", id);

    let state = StateStorage::open(format!("{}-state", path))?;
    let entry = ExampleEntryStorage::open(format!("{}-entry", path))?;
    let engine = ExampleApplyStorage::open(format!("{}-engine", path))?;

    let state = Arc::new(state);
    let entry: EntryStorageRef = Arc::new(entry);
    let engine: ApplyStorageRef = Arc::new(engine);

    let info = RaftNodeInfo {
        group_id: 2222,
        address: http_addr.clone(),
    };

    let storage = NodeStorage::open(id, info.clone(), state, engine.clone(), entry)?;
    let storage = Arc::new(storage);

    let node = RaftNode::new(id, info, storage, engine).await.unwrap();
    let app_data = actix_web::web::Data::new(node);

    // Start the actix-web server.
    let server = actix_web::HttpServer::new(move || {
        actix_web::App::new()
            .wrap(actix_web::middleware::Logger::default())
            .wrap(actix_web::middleware::Logger::new("%a %{User-Agent}i"))
            .wrap(actix_web::middleware::Compress::default())
            .app_data(app_data.clone())
            // raft internal RPC
            .service(append)
            .service(snapshot)
            .service(vote)
            // admin API
            .service(init)
            .service(add_learner)
            .service(change_membership)
            .service(metrics)
            // application API
            .service(write)
            .service(read)
    });

    let x = server.bind(http_addr)?;

    x.run().await.unwrap();

    Ok(())
}

// -----------------------------------------------------------------------------//
#[post("/write")]
pub async fn write(app: Data<RaftNode>, req: Json<Request>) -> actix_web::Result<impl Responder> {
    info!("Received request write: {:?}", req);
    let response = app.raw_raft().client_write(req.0).await;

    Ok(Json(response))
}

#[post("/read")]
pub async fn read(app: Data<RaftNode>, req: Json<String>) -> actix_web::Result<impl Responder> {
    info!("Received request read: {:?}", req);
    let rsp = app
        .test_read_data(&req.0)
        .await
        .map_or_else(|err| Some(err.to_string()), |v| v)
        .unwrap_or("not found value by key".to_string());

    Ok(rsp)
    // let res: Result<String, Infallible> = Ok(rsp);
    // Ok(Json(res))
}

// -----------------------------------------------------------------------------//
#[post("/init")]
pub async fn init(app: Data<RaftNode>) -> actix_web::Result<impl Responder> {
    info!("Received request init");
    let rsp = app
        .raft_init()
        .await
        .map_or_else(|err| err.to_string(), |_| "Success".to_string());

    Ok(rsp)
}

#[post("/add-learner")]
pub async fn add_learner(
    app: Data<RaftNode>,
    req: Json<(RaftNodeId, String)>,
) -> actix_web::Result<impl Responder> {
    info!("Received request add-learner: {:?}", req);
    let id = req.0 .0;
    let addr = req.0 .1;
    let info = RaftNodeInfo {
        group_id: 2222,
        address: addr,
    };

    let rsp = app
        .raft_add_learner(id, info)
        .await
        .map_or_else(|err| err.to_string(), |_| "Success".to_string());

    Ok(rsp)
}

/// Changes specified learners to members, or remove members.
#[post("/change-membership")]
pub async fn change_membership(
    app: Data<RaftNode>,
    req: Json<BTreeSet<RaftNodeId>>,
) -> actix_web::Result<impl Responder> {
    info!("Received request change-membership: {:?}", req);
    let rsp = app
        .raft_change_membership(req.0)
        .await
        .map_or_else(|err| err.to_string(), |_| "Success".to_string());

    Ok(rsp)
}

/// Get the latest metrics of the cluster
#[get("/metrics")]
pub async fn metrics(app: Data<RaftNode>) -> actix_web::Result<impl Responder> {
    let metrics = app.raft_metrics();

    let res: Result<openraft::RaftMetrics<RaftNodeId, RaftNodeInfo>, Infallible> = Ok(metrics);
    Ok(Json(res))
}

// -----------------------------------------------------------------------------//
#[post("/raft-vote")]
pub async fn vote(
    app: Data<RaftNode>,
    req: Json<VoteRequest<RaftNodeId>>,
) -> actix_web::Result<impl Responder> {
    info!("Received request raft-vote: {:?}", req);
    let res = app.raw_raft().vote(req.0).await;

    Ok(Json(res))
}

#[post("/raft-append")]
pub async fn append(
    app: Data<RaftNode>,
    req: Json<AppendEntriesRequest<TypeConfig>>,
) -> actix_web::Result<impl Responder> {
    info!("Received request raft-append: {:?}", req);
    let res = app.raw_raft().append_entries(req.0).await;

    Ok(Json(res))
}

#[post("/raft-snapshot")]
pub async fn snapshot(
    app: Data<RaftNode>,
    req: Json<InstallSnapshotRequest<TypeConfig>>,
) -> actix_web::Result<impl Responder> {
    info!("Received request raft-snapshot: {:?}", req);
    let res = app.raw_raft().install_snapshot(req.0).await;

    Ok(Json(res))
}
