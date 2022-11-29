use actix_web::get;
use actix_web::post;
use actix_web::web;
use actix_web::web::Data;
use actix_web::Responder;
use openraft::error::CheckIsLeaderError;
use openraft::error::Infallible;
use openraft::raft::ClientWriteRequest;
use openraft::EntryPayload;
use web::Json;

use crate::store::command::*;
use crate::store::state_machine::CommandResp;
use crate::MetaApp;
use crate::NodeId;

#[post("/read")]
pub async fn read(app: Data<MetaApp>, req: Json<ReadCommand>) -> actix_web::Result<impl Responder> {
    let sm = app.store.state_machine.read().await;

    let res = sm.process_read_command(&req.0);

    let response: Result<CommandResp, Infallible> = Ok(res);
    Ok(Json(response))
}

#[post("/write")]
pub async fn write(
    app: Data<MetaApp>,
    req: Json<WriteCommand>,
) -> actix_web::Result<impl Responder> {
    let request = ClientWriteRequest::new(EntryPayload::Normal(req.0));
    let res = match app.raft.client_write(request).await {
        Ok(val) => val.data,
        Err(err) => {
            TenaneMetaDataResp::new(META_REQUEST_FAILED, format!("raft write error: {}", err))
                .to_string()
        }
    };

    let response: Result<CommandResp, Infallible> = Ok(res);
    Ok(Json(response))
}

#[get("/read_all")]
pub async fn read_all(app: Data<MetaApp>) -> actix_web::Result<impl Responder> {
    let sm = app.store.state_machine.read().await;

    let mut response = "******--------------------------------------------******\n".to_string();
    for (k, v) in sm.data.iter() {
        response = response + &format!("* {}: {}\n", k, v);
    }
    response += "******----------------------------------------------******\n";

    Ok(response)
}

//*************************************************************************//
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
