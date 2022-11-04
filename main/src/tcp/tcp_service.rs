use futures::future::ok;
use snafu::ResultExt;
use spi::server::dbms::DBMSRef;
use std::io::{BufReader, Error, Read};
use std::net::SocketAddr;
use std::net::{TcpListener, TcpStream};
use std::{collections::HashMap, sync::Arc};
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

                let handler =
                    tokio::spawn(warp_process_client(client, dbms.clone(), kv_inst.clone()));
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

async fn warp_process_client(client: TcpStream, dbms: DBMSRef, kv_inst: EngineRef) {
    print!("=====process_client begin");

    info!("=====process_client begin");
    let result = process_client(client, dbms, kv_inst).await;

    info!("=====process_client {:#?}", result);
}

async fn process_client(
    mut client: TcpStream,
    dbms: DBMSRef,
    kv_inst: EngineRef,
) -> CoordinatorResult<()> {
    loop {
        let mut tmp_buf: [u8; 4] = [0; 4];

        client.read_exact(&mut tmp_buf)?;
        let cmd_type = u32::from_be_bytes(tmp_buf);

        client.read_exact(&mut tmp_buf)?;
        let data_len = u32::from_be_bytes(tmp_buf);

        let mut cmd_buf = vec![0; data_len as usize];
        client.read_exact(&mut cmd_buf)?;

        if cmd_type == WRITE_VNODE_POINT_COMMAND {
            process_vnode_write_command(&mut client, cmd_buf, kv_inst.clone()).await?;
        }
    }
}

async fn process_vnode_write_command(
    client: &mut TcpStream,
    cmd_buf: Vec<u8>,
    kv_inst: EngineRef,
) -> CoordinatorResult<()> {
    let cmd = WriteVnodeRequest::decode(&cmd_buf)?;

    let mut points_data = vec![0; cmd.data_len as usize];
    client.read_exact(&mut points_data)?;

    let req = WritePointsRpcRequest {
        version: 1,
        points: points_data,
    };

    if let Err(err) = kv_inst.write(cmd.vnode_id, req).await {
        CommonResponse::send(client, -1, err.to_string()).await?;
        return Err(err.into());
    } else {
        CommonResponse::send(client, 0, "".to_owned()).await?;
        return Ok(());
    }
}
