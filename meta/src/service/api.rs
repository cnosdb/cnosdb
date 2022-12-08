#![allow(clippy::field_reassign_with_default)]

use actix_web::get;
use actix_web::post;
use actix_web::web;
use actix_web::web::Data;
use actix_web::Responder;
use openraft::error::Infallible;
use openraft::raft::ClientWriteRequest;
use openraft::EntryPayload;
use tokio::sync::mpsc;
use web::Json;

use crate::store::command::*;
use crate::store::state_machine::CommandResp;
use crate::store::state_machine::WatchTenantMetaData;
use crate::MetaApp;

#[post("/read")]
pub async fn read(app: Data<MetaApp>, req: Json<ReadCommand>) -> actix_web::Result<impl Responder> {
    let sm = app.store.state_machine.read().await;

    println!("====={:#?}, {}", app.id, &app.addr);

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

#[post("/watch_tenant")]
pub async fn watch_tenant(
    app: Data<MetaApp>,
    req: Json<(String, String, String, u64)>, //client_id, cluster, tenant,version
) -> actix_web::Result<impl Responder> {
    let client_id = req.0 .0;
    let cluster = req.0 .1;
    let tenant = req.0 .2;
    let version = req.0 .3;

    let (sender, mut receiver) = mpsc::channel(1);
    {
        let mut watch_data = WatchTenantMetaData {
            sender,
            cluster: cluster.clone(),
            tenant: tenant.clone(),
            delta: TenantMetaDataDelta::default(),
        };

        let mut watch = app.store.watch.write().await;
        let sm = app.store.state_machine.write().await;
        if let Some(item) = watch.get_mut(&client_id) {
            item.sender.closed().await;
            watch_data.delta = item.delta.clone();
        }

        let delta_min_ver = watch_data.delta.ver_range.0;
        if sm.version() > version && (version < delta_min_ver || delta_min_ver == 0) {
            watch_data.delta = TenantMetaDataDelta::default();
            watch_data.delta.update_version(sm.version());
            watch.insert(client_id.clone(), watch_data);

            let mut data = TenantMetaDataDelta::default();
            data.full_load = true;
            data.update = sm.to_tenant_meta_data(&cluster, &tenant);

            let res = serde_json::to_string(&data).unwrap();
            let response: Result<CommandResp, Infallible> = Ok(res);
            return Ok(Json(response));
        }

        let delta_max_ver = watch_data.delta.ver_range.1;
        if version < delta_max_ver {
            let _ = watch_data.sender.try_send(true);
        }

        watch.insert(client_id.clone(), watch_data);
    }

    if receiver.recv().await.is_none() {
        let response: Result<CommandResp, Infallible> = Ok("watch channel closed!".to_string());
        return Ok(Json(response));
    }

    let mut watch = app.store.watch.write().await;
    if let Some(item) = watch.get_mut(&client_id) {
        let res = serde_json::to_string(&item.delta).unwrap();

        let delta_max_ver = item.delta.ver_range.0;
        item.delta = TenantMetaDataDelta::default();
        item.delta.update_version(delta_max_ver);

        let response: Result<CommandResp, Infallible> = Ok(res);
        return Ok(Json(response));
    }

    let response: Result<CommandResp, Infallible> = Ok("can't found watch info".to_string());
    Ok(Json(response))
}

#[get("/debug")]
pub async fn debug(app: Data<MetaApp>) -> actix_web::Result<impl Responder> {
    let sm = app.store.state_machine.read().await;

    let mut response = format!("******----------version: {}-------******\n", sm.version());
    for (k, v) in sm.data.iter() {
        response = response + &format!("* {}: {}\n", k, v);
    }
    response += "******--------------------------------------******\n";

    Ok(response)
}
