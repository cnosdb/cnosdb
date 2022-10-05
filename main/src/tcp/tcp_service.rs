use std::{collections::HashMap, sync::Arc};
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};
use tokio::runtime::Runtime;
use tokio::time::{self, Duration};

use spi::server::dbms::DBMSRef;
use std::io::Error;
use std::net::SocketAddr;
use tokio::sync::oneshot;
use tskv::engine::EngineRef;

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
            let listener = TcpListener::bind(addr.to_string()).await.unwrap();
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
        match listener.accept().await {
            Ok((client, address)) => {
                backoff = 1;

                debug!("client address: {}", address);
                tokio::spawn(process_client(client, dbms.clone(), kv_inst.clone()));
            }

            Err(err) => {
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
) -> Result<(), Error> {
    loop {
        let cmd_type = client.read_u32().await?;
        let data_len = client.read_u32().await?;

        let mut data_buf = vec![0; data_len as usize];
        client.read_exact(&mut data_buf).await?;
        if cmd_type == meta_data::WRITE_VNODE_POINT_COMMAND {
            process_vnode_write_command();
        }
    }
}

fn process_vnode_write_command() {}

#[cfg(test)]
mod test {
    use std::io::Error;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};
    use tokio::time;
    #[tokio::test]
    async fn tcp_client() {
        let mut buffer = vec![0; 4096];

        let mut client = TcpStream::connect("127.0.0.1:31005").await.unwrap();

        client.write_u32(100).await.unwrap();
        client.write(b"hello my server").await.unwrap();

        let len = client.read_u32().await.unwrap();
        let size = client.read(&mut buffer).await.unwrap();

        let str = std::str::from_utf8(&buffer[..size]).unwrap();
        println!("收到数据：{}|{}", len, str);
    }
}
