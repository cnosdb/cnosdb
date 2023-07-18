use std::collections::HashSet;
use std::time::Duration;

use actix_web::web::Data;
use actix_web::{get, post, web, Responder};
use openraft::error::Infallible;
use trace::info;
use web::Json;

use crate::store::command::*;
use crate::store::state_machine::{response_encode, CommandResp};
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

// curl -XPOST http://127.0.0.1:8901/dump --o ./meta_dump.data
// curl -XPOST http://127.0.0.1:8901/restore --data-binary "@./meta_dump.data"
#[get("/dump")]
pub async fn dump(app: Data<MetaApp>) -> actix_web::Result<impl Responder> {
    let sm = app.store.state_machine.read().await;
    let mut response = "".to_string();
    for res in sm.db.iter() {
        let (key, val) = res.map_or(("".to_string(), "".to_string()), |(k, v)| {
            (
                String::from_utf8((*k).to_owned()).map_or("".to_string(), |d| d),
                String::from_utf8((*v).to_owned()).map_or("".to_string(), |d| d),
            )
        });
        response = response + &format!("{}: {}\n", key, val);
    }

    Ok(response)
}

#[post("/restore")]
pub async fn restore(app: Data<MetaApp>, data: String) -> actix_web::Result<impl Responder> {
    info!("restore data length:{}", data.len());

    let mut count = 0;
    let lines: Vec<&str> = data.split('\n').collect();
    for line in lines.iter() {
        let strs: Vec<&str> = line.splitn(2, ": ").collect();
        if strs.len() != 2 {
            continue;
        }

        let command = WriteCommand::Set {
            key: strs[0].to_string(),
            value: strs[1].to_string(),
        };
        if let Err(err) = app.raft.client_write(command).await {
            return Ok(err.to_string());
        }

        count += 1;
    }

    Ok(format!("Restore Data Success, Total: {} ", count))
}

#[post("/watch")]
pub async fn watch(
    app: Data<MetaApp>,
    req: Json<(String, String, HashSet<String>, u64)>, //client id, cluster,version
) -> actix_web::Result<impl Responder> {
    info!("watch all  args: {:?}", req);
    let client = req.0 .0;
    let cluster = req.0 .1;
    let tenants = req.0 .2;
    let base_ver = req.0 .3;
    let mut follow_ver = base_ver;

    let mut notify = {
        let sm = app.store.state_machine.read().await;
        let watch_data = sm.read_change_logs(&cluster, &tenants, follow_ver);
        info!(
            "{} {}.{}: change logs: {:?} ",
            client, base_ver, follow_ver, watch_data
        );

        if watch_data.need_return(base_ver) {
            let data = response_encode(Ok(watch_data));
            let response: Result<CommandResp, Infallible> = Ok(data);
            return Ok(Json(response));
        }

        sm.watch.subscribe()
    };

    let now = std::time::Instant::now();
    loop {
        let _ = tokio::time::timeout(tokio::time::Duration::from_secs(20), notify.recv()).await;

        let sm = app.store.state_machine.read().await;
        let watch_data = sm.read_change_logs(&cluster, &tenants, follow_ver);
        info!(
            "{} {}.{}: change logs: {:?} ",
            client, base_ver, follow_ver, watch_data
        );
        if watch_data.need_return(base_ver) || now.elapsed() > Duration::from_secs(30) {
            let data = response_encode(Ok(watch_data));
            let response: Result<CommandResp, Infallible> = Ok(data);
            return Ok(Json(response));
        }

        if follow_ver < watch_data.max_ver {
            follow_ver = watch_data.max_ver;
        }
    }
}

#[get("/debug")]
pub async fn debug(app: Data<MetaApp>) -> actix_web::Result<impl Responder> {
    let sm = app.store.state_machine.read().await;
    let mut response = "******---------------------------******\n".to_string();
    for res in sm.db.iter() {
        let (key, val) = res.map_or(("".to_string(), "".to_string()), |(k, v)| {
            (
                String::from_utf8((*k).to_owned()).map_or("".to_string(), |d| d),
                String::from_utf8((*v).to_owned()).map_or("".to_string(), |d| d),
            )
        });
        response = response + &format!("* {}: {}\n", key, val);
    }
    response += "******----------------------------------------------******\n";

    Ok(response)
}

#[get("/debug/pprof")]
pub async fn cpu_pprof(_app: Data<MetaApp>) -> actix_web::Result<impl Responder> {
    #[cfg(unix)]
    {
        match utils::pprof_tools::gernate_pprof().await {
            Ok(v) => Ok(v),
            Err(v) => Ok(v),
        }
    }
    #[cfg(not(unix))]
    {
        actix_web::Result::<String>::Err(actix_web::error::ErrorNotFound(
            "/debug/pprof only supported on *unix systems.",
        ))
    }
}

#[get("/debug/backtrace")]
pub async fn backtrace(_app: Data<MetaApp>) -> actix_web::Result<impl Responder> {
    Ok(utils::backtrace::backtrace())
}
