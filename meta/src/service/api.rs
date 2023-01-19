use actix_web::get;
use actix_web::post;
use actix_web::web;
use actix_web::web::Data;
use actix_web::Responder;
use openraft::error::Infallible;
use trace::info;
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
    let response = app.raft.client_write(req.0).await;
    Ok(Json(response))
}

#[post("/watch")]
pub async fn watch(
    app: Data<MetaApp>,
    req: Json<(String, String, String, u64)>, //client id, cluster,version
) -> actix_web::Result<impl Responder> {
    info!("watch all  args: {:?}", req);
    let client = req.0 .0;
    let cluster = req.0 .1;
    let tenant = req.0 .2;
    let base_ver = req.0 .3;
    let mut follow_ver = base_ver;

    let mut chan = {
        let sm = app.store.state_machine.read().await;
        let watch_data = sm.read_change_logs(&cluster, &tenant, follow_ver);
        info!(
            "{} {}.{}: change logs: {:?} ",
            client, base_ver, follow_ver, watch_data
        );
        if watch_data.need_return(base_ver) {
            let data = serde_json::to_string(&watch_data).unwrap();
            let response: Result<CommandResp, Infallible> = Ok(data);
            return Ok(Json(response));
        }

        sm.watch.subscribe()
    };

    while (chan.recv().await).is_ok() {
        let sm = app.store.state_machine.read().await;
        let watch_data = sm.read_change_logs(&cluster, &tenant, follow_ver);
        info!(
            "{} {}.{}: change logs: {:?} ",
            client, base_ver, follow_ver, watch_data
        );
        if watch_data.need_return(base_ver) {
            let data = serde_json::to_string(&watch_data).unwrap();
            let response: Result<CommandResp, Infallible> = Ok(data);
            return Ok(Json(response));
        }

        if follow_ver < watch_data.max_ver {
            follow_ver = watch_data.max_ver;
        }
    }

    let response: Result<CommandResp, Infallible> = Ok("notify channel closed".to_string());
    Ok(Json(response))
}

#[get("/debug")]
pub async fn debug(app: Data<MetaApp>) -> actix_web::Result<impl Responder> {
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
