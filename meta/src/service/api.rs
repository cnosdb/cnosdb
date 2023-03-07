use std::collections::HashSet;
use std::fs::File;
use std::io::Write;
use std::time::Duration;

use actix_web::web::Data;
use actix_web::{get, post, web, Responder};
use openraft::error::Infallible;
use pprof::protos::Message;
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
            let data = serde_json::to_string(&watch_data).unwrap();
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
            let data = serde_json::to_string(&watch_data).unwrap();
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
        let (k, v) = res.unwrap();
        let k = String::from_utf8((*k).to_owned()).unwrap();
        let v = String::from_utf8((*v).to_owned()).unwrap();
        response = response + &format!("* {}: {}\n", k, v);
    }
    response += "******----------------------------------------------******\n";

    Ok(response)
}

#[get("/pprof")]
pub async fn pprof_test(_app: Data<MetaApp>) -> actix_web::Result<impl Responder> {
    let guard = pprof::ProfilerGuardBuilder::default()
        .frequency(1000)
        .blocklist(&["libc", "libgcc", "pthread", "vdso"])
        .build()
        .unwrap();

    tokio::time::sleep(tokio::time::Duration::from_secs(10)).await;

    if let Ok(report) = guard.report().build() {
        info!("====== write pprof file");
        let mut file = File::create("/tmp/cnosdb/profile.pb").unwrap();
        let profile = report.pprof().unwrap();
        let mut content = Vec::new();
        profile.write_to_vec(&mut content).unwrap();
        file.write_all(&content).unwrap();

        let file = File::create("/tmp/cnosdb/flamegraph.svg").unwrap();
        report.flamegraph(file).unwrap();
    };

    Ok("".to_string())
}
