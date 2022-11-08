use futures::future::ok;
use snafu::ResultExt;
use spi::server::dbms::DBMSRef;
use std::io::{BufReader, Error, Read};
use std::net::SocketAddr;
use std::net::{TcpListener, TcpStream};
use std::{collections::HashMap, sync::Arc};
//use tokio::net::{TcpListener, TcpStream};

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
        let acceptor_fn = async move {
            let listener = TcpListener::bind(addr.to_string()).unwrap();
            info!("tcp server start addr: {}", addr);

            tokio::select! {
                res = service_run(listener,dbms,kv_inst) => {
                    if let Err(err) = res {
                        error!(cause = %err, "failed to accept");
                    }
                }

                _ = rx => {
                    info!("tcp server shutting down");
                }
            }
        };

        let join_handle = tokio::spawn(acceptor_fn);

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

async fn service_run(
    listener: TcpListener,
    dbms: DBMSRef,
    kv_inst: EngineRef,
) -> Result<(), Error> {
    let mut backoff = 1;

    loop {
        match listener.accept() {
            Ok((client, address)) => {
                backoff = 1;

                debug!("client address: {}", address);

                let dbms_ = dbms.clone();
                let kv_inst_ = kv_inst.clone();
                let processor_fn = async move {
                    info!("=====process_client begin");
                    let result = process_client(client, dbms_, kv_inst_).await;
                    info!("=====process_client {:#?}", result);
                };

                let handler = tokio::spawn(processor_fn);
                handler.await.unwrap(); //todo
            }

            Err(err) => {
                info!("=== {}", err.to_string());
                if backoff > 64 {
                    // Accept has failed too many times. Return the error.
                    return Err(err.into());
                }

                time::sleep(Duration::from_secs(backoff)).await;
                // Double the back off
                backoff *= 2;
            }
        }
    }
}

async fn process_client(
    mut client: TcpStream,
    dbms: DBMSRef,
    kv_inst: EngineRef,
) -> CoordinatorResult<()> {
    loop {
        let recv_cmd = recv_command(&mut client)?;
        match recv_cmd {
            CoordinatorCmd::CommonResponseCmd(cmd) => {}
            CoordinatorCmd::WriteVnodePointCmd(cmd) => {
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
        send_command(client, &CoordinatorCmd::CommonResponseCmd(resp))?;
        return Err(err.into());
    } else {
        info!("success write data to vnode: {}", cmd.vnode_id);
        send_command(client, &CoordinatorCmd::CommonResponseCmd(resp))?;
        return Ok(());
    }
}
