use actix_web::get;
use actix_web::post;
use actix_web::web;
use actix_web::web::Data;
use actix_web::Responder;
use openraft::error::Infallible;
use web::Json;

use crate::store::command::*;
use crate::store::state_machine::CommandResp;
use crate::MetaApp;

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
    println!("--- hello this is write ---");
    let res = match app.raft.client_write(req.0).await {
        Ok(val) => val.data,
        Err(err) => {
            TenaneMetaDataResp::new(META_REQUEST_FAILED, format!("raft write error: {}", err))
                .to_string()
        }
    };

    let response: Result<CommandResp, Infallible> = Ok(res);
    Ok(Json(response))
}

#[get("/debug")]
pub async fn debug(app: Data<MetaApp>) -> actix_web::Result<impl Responder> {
    println!("--- hello this is debug ---");
    let sm = app.store.state_machine.read().await;
    let mut response = "******---------------------------******\n".to_string();
    for res in sm.db.iter() {
        let (k, v) = res.unwrap();
        let k = String::from_utf8((*k).to_owned()).unwrap();
        let v = String::from_utf8((*v).to_owned()).unwrap();
        response = response + &format!("* {}: {}\n", k, v);
    }
    response += "******----------------------------------------------******\n";

    Ok(response)
}
