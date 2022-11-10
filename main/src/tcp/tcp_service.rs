use futures::future::ok;
use futures::Future;
use snafu::ResultExt;
use spi::server::dbms::DBMSRef;
use std::net::{self, SocketAddr};
use tokio::io::BufReader;

use std::{collections::HashMap, sync::Arc};
use tokio::net::{TcpListener, TcpStream};
// use std::net::{TcpListener, TcpStream};
use tokio::sync::oneshot::Receiver;

use tokio::sync::oneshot;
use tokio::time::{self, Duration};
use tskv::engine::EngineRef;

use coordinator::command::*;
use coordinator::errors::*;
use protos::kv_service::WritePointsRpcRequest;

use models::meta_data;

use crate::server;

use crate::server::{Service, ServiceHandle};

use trace::{debug, error, info};

pub struct TcpService {
    addr: SocketAddr,
    dbms: DBMSRef,
    kv_inst: EngineRef,
    handle: Option<ServiceHandle<()>>,
}

impl TcpService {
    pub fn new(dbms: DBMSRef, kv_inst: EngineRef, addr: SocketAddr) -> Self {
        Self {
            addr,
            dbms,
            kv_inst,
            handle: None,
        }
    }
}

#[async_trait::async_trait]
impl Service for TcpService {
    fn start(&mut self) -> Result<(), server::Error> {
        let (shutdown, rx) = oneshot::channel();

        let addr = self.addr.clone();
        let dbms = self.dbms.clone();
        let kv_inst = self.kv_inst.clone();

        let join_handle = tokio::spawn(service_run(addr, dbms, kv_inst));
        self.handle = Some(ServiceHandle::new(
            "tcp service".to_string(),
            join_handle,
            shutdown,
        ));

        Ok(())
    }

    async fn stop(&mut self, force: bool) {
        if let Some(stop) = self.handle.take() {
            stop.shutdown(force).await
        };
    }
}

async fn service_run(addr: SocketAddr, dbms: DBMSRef, kv_inst: EngineRef) {
    let listener = TcpListener::bind(addr.to_string()).await.unwrap();
    info!("tcp server start addr: {}", addr);

    loop {
        match listener.accept().await {
            Ok((client, address)) => {
                debug!("tcp client address: {}", address);

                let dbms_ = dbms.clone();
                let kv_inst_ = kv_inst.clone();
                let processor_fn = async move {
                    info!("process client begin");
                    let result = process_client(client, dbms_, kv_inst_).await;
                    info!("process client result: {:#?}", result);
                };

                let _handler = tokio::spawn(processor_fn);
                _handler.await.unwrap(); //todo
            }

            Err(err) => {
                info!("tcp server accetp error: {}", err.to_string());
                time::sleep(Duration::from_secs(1)).await;
            }
        }
    }
}

async fn process_client(
    mut client: TcpStream,
    dbms: DBMSRef,
    kv_inst: EngineRef,
) -> CoordinatorResult<()> {
    //let mut client = client.into_std()?;
    //client.set_nonblocking(false)?;

    //let reader = BufReader::new(client);

    loop {
        let recv_cmd = recv_command(&mut client).await?;
        match recv_cmd {
            CoordinatorTcpCmd::CommonResponseCmd(cmd) => {}
            CoordinatorTcpCmd::WriteVnodePointCmd(cmd) => {
                process_vnode_write_command(&mut client, cmd, kv_inst.clone()).await?;
            }
        }
    }
}

async fn process_vnode_write_command(
    client: &mut TcpStream,
    cmd: WriteVnodeRequest,
    kv_inst: EngineRef,
) -> CoordinatorResult<()> {
    let req = WritePointsRpcRequest {
        version: 1,
        points: cmd.data,
    };

    let mut resp = CommonResponse {
        code: SUCCESS_RESPONSE_CODE,
        data: "".to_string(),
    };
    if let Err(err) = kv_inst.write(cmd.vnode_id, req).await {
        resp.code = -1;
        resp.data = err.to_string();
        send_command(client, &CoordinatorTcpCmd::CommonResponseCmd(resp)).await?;
        return Err(err.into());
    } else {
        info!("success write data to vnode: {}", cmd.vnode_id);
        send_command(client, &CoordinatorTcpCmd::CommonResponseCmd(resp)).await?;
        return Ok(());
    }
}
