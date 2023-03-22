use std::collections::HashSet;
use std::time::Duration;

use actix_web::web::Data;
use actix_web::{get, post, web, Responder};
use openraft::async_trait::async_trait;
use openraft::error::{ClientWriteError, Infallible};
use openraft::raft::ClientWriteResponse;
use trace::info;
use web::Json;

use crate::service::MetaApi;
use crate::store::command::*;
use crate::store::state_machine::CommandResp;
use crate::{ClusterNode, ClusterNodeId, MetaApp, TypeConfig};

#[async_trait]
impl MetaApi for MetaApp {
    async fn read(&self, req: Json<ReadCommand>) -> Result<CommandResp, Infallible> {
        let sm = self.store.state_machine.read().await;

        let res = sm.process_read_command(&req.0);

        let response: Result<CommandResp, Infallible> = Ok(res);
        response
    }

    async fn write(
        &self,
        req: Json<WriteCommand>,
    ) -> Result<ClientWriteResponse<TypeConfig>, ClientWriteError<ClusterNodeId, ClusterNode>> {
        self.raft.client_write(req.0).await
    }

    async fn dump(&self) -> String {
        let sm = self.store.state_machine.read().await;
        let mut response = "".to_string();
        for res in sm.db.iter() {
            let (k, v) = res.unwrap();
            let k = String::from_utf8((*k).to_owned()).unwrap();
            let v = String::from_utf8((*v).to_owned()).unwrap();
            response = response + &format!("{}: {}\n", k, v);
        }
        response
    }

    async fn restore(&self, data: String) -> String {
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
            if let Err(err) = self.raft.client_write(command).await {
                return err.to_string();
            }

            count += 1;
        }
        format!("Restore Data Success, Total: {} ", count)
    }

    async fn watch(
        &self,
        req: Json<(String, String, HashSet<String>, u64)>,
    ) -> Result<CommandResp, Infallible> {
        info!("watch all  args: {:?}", req);
        let client = req.0 .0;
        let cluster = req.0 .1;
        let tenants = req.0 .2;
        let base_ver = req.0 .3;
        let mut follow_ver = base_ver;

        let mut notify = {
            let sm = self.store.state_machine.read().await;
            let watch_data = sm.read_change_logs(&cluster, &tenants, follow_ver);
            info!(
                "{} {}.{}: change logs: {:?} ",
                client, base_ver, follow_ver, watch_data
            );
            if watch_data.need_return(base_ver) {
                let data = serde_json::to_string(&watch_data).unwrap();
                let response: Result<CommandResp, Infallible> = Ok(data);
                return response;
            }

            sm.watch.subscribe()
        };

        let now = std::time::Instant::now();
        loop {
            let _ = tokio::time::timeout(tokio::time::Duration::from_secs(20), notify.recv()).await;

            let sm = self.store.state_machine.read().await;
            let watch_data = sm.read_change_logs(&cluster, &tenants, follow_ver);
            info!(
                "{} {}.{}: change logs: {:?} ",
                client, base_ver, follow_ver, watch_data
            );
            if watch_data.need_return(base_ver) || now.elapsed() > Duration::from_secs(30) {
                let data = serde_json::to_string(&watch_data).unwrap();
                let response: Result<CommandResp, Infallible> = Ok(data);
                return response;
            }

            if follow_ver < watch_data.max_ver {
                follow_ver = watch_data.max_ver;
            }
        }
    }

    async fn debug(&self) -> String {
        let sm = self.store.state_machine.read().await;
        let mut response = "******---------------------------******\n".to_string();
        for res in sm.db.iter() {
            let (k, v) = res.unwrap();
            let k = String::from_utf8((*k).to_owned()).unwrap();
            let v = String::from_utf8((*v).to_owned()).unwrap();
            response = response + &format!("* {}: {}\n", k, v);
        }
        response += "******----------------------------------------------******\n";
        response
    }

    async fn cpu_pprof(&self) -> String {
        match utils::pprof_tools::gernate_pprof().await {
            Ok(v) => v,
            Err(v) => v,
        }
    }

    async fn backtrace(&self) -> String {
        utils::backtrace::backtrace()
    }
}

#[post("/read")]
pub async fn read(app: Data<MetaApp>, req: Json<ReadCommand>) -> actix_web::Result<impl Responder> {
    let app = app.as_ref() as &dyn MetaApi;
    let res = app.read(req).await;
    Ok(Json(res))
}

#[post("/write")]
pub async fn write(
    app: Data<MetaApp>,
    req: Json<WriteCommand>,
) -> actix_web::Result<impl Responder> {
    let app = app.as_ref() as &dyn MetaApi;
    let response = app.write(req).await;
    Ok(Json(response))
}

// curl -XPOST http://127.0.0.1:21001/dump --o ./meta_dump.data
// curl -XPOST http://127.0.0.1:21001/restore --data-binary "@./meta_dump.data"
#[get("/dump")]
pub async fn dump(app: Data<MetaApp>) -> actix_web::Result<impl Responder> {
    let app = app.as_ref() as &dyn MetaApi;
    let response = app.dump().await;
    Ok(response)
}

#[post("/restore")]
pub async fn restore(app: Data<MetaApp>, data: String) -> actix_web::Result<impl Responder> {
    let app = app.as_ref() as &dyn MetaApi;
    let res = app.restore(data).await;
    Ok(res)
}

#[post("/watch")]
pub async fn watch(
    app: Data<MetaApp>,
    req: Json<(String, String, HashSet<String>, u64)>, //client id, cluster,version
) -> actix_web::Result<impl Responder> {
    let app = app.as_ref() as &dyn MetaApi;
    let res = app.watch(req).await;
    Ok(Json(res))
}

#[get("/debug")]
pub async fn debug(app: Data<MetaApp>) -> actix_web::Result<impl Responder> {
    let app = app.as_ref() as &dyn MetaApi;
    let resp = app.debug().await;
    Ok(resp)
}

#[get("/debug/pprof")]
pub async fn cpu_pprof(app: Data<MetaApp>) -> actix_web::Result<impl Responder> {
    let app = app.as_ref() as &dyn MetaApi;
    let res = app.cpu_pprof().await;
    Ok(res)
}

#[get("/debug/backtrace")]
pub async fn backtrace(app: Data<MetaApp>) -> actix_web::Result<impl Responder> {
    let app = app.as_ref() as &dyn MetaApi;
    let res = app.backtrace().await;
    Ok(res)
}
