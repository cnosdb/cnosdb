use actix_web::get;
use actix_web::post;
use actix_web::web;
use actix_web::web::Data;
use actix_web::Responder;
use models::meta_data::NodeInfo;
use models::meta_data::TenantMetaData;
use openraft::error::CheckIsLeaderError;
use openraft::error::Infallible;
use openraft::raft::ClientWriteRequest;
use openraft::EntryPayload;
use web::Json;

use crate::meta_app::MetaApp;

use crate::NodeId;

use crate::store::state_machine::children_data;
use crate::store::state_machine::KeyPath;
use crate::store::state_machine::{KvReq, KvResp};

#[post("/write")]
pub async fn write(app: Data<MetaApp>, req: Json<KvReq>) -> actix_web::Result<impl Responder> {
    let request = ClientWriteRequest::new(EntryPayload::Normal(req.0));
    let response = match app.raft.client_write(request).await {
        Ok(val) => val.data,
        Err(err) => KvResp {
            err_code: -1,
            meta_data: TenantMetaData::new(),
            err_msg: format!("raft write error: {}", err),
        },
    };

    let res: Result<KvResp, Infallible> = Ok(response);
    Ok(Json(res))
}

#[post("/read")]
pub async fn read(
    app: Data<MetaApp>,
    req: Json<(String, String)>,
) -> actix_web::Result<impl Responder> {
    let (cluster, tenant) = req.0;

    let sm = app.store.state_machine.read().await;

    let response = KvResp {
        err_code: 0,
        err_msg: "".to_string(),
        meta_data: sm.to_tenant_meta_data(&cluster, &tenant),
    };

    let res: Result<KvResp, Infallible> = Ok(response);
    Ok(Json(res))
}

#[post("/data_nodes")]
pub async fn data_nodes(
    app: Data<MetaApp>,
    req: Json<String>,
) -> actix_web::Result<impl Responder> {
    let cluster = req.0;

    let sm = app.store.state_machine.read().await;

    let response = children_data::<NodeInfo>(&KeyPath::data_nodes(&cluster), &sm.data)
        .into_values()
        .collect();

    let res: Result<Vec<NodeInfo>, Infallible> = Ok(response);

    Ok(Json(res))
}

#[get("/read_all")]
pub async fn read_all(app: Data<MetaApp>) -> actix_web::Result<impl Responder> {
    let sm = app.store.state_machine.read().await;

    let mut response = "*---------------------------------------------------------\n".to_string();
    for (k, v) in sm.data.iter() {
        response = response + &format!("* {}: {}\n", k, v);
    }
    response += "*-------------------------------------------------------\n";

    Ok(response)
}

#[post("/consistent_read")]
pub async fn consistent_read(
    app: Data<MetaApp>,
    req: Json<String>,
) -> actix_web::Result<impl Responder> {
    let ret = app.raft.is_leader().await;
    match ret {
        Ok(_) => {
            let state_machine = app.store.state_machine.read().await;
            let key = req.0;
            let value = state_machine.data.get(&key).cloned();
            let res: Result<String, CheckIsLeaderError<NodeId>> = Ok(value.unwrap_or_default());
            Ok(Json(res))
        }
        Err(e) => Ok(Json(Err(e))),
    }
}
