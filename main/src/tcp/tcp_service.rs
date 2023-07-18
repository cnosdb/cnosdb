use async_trait::async_trait;
use coordinator::service::CoordinatorRef;
use models::consistency_level::ConsistencyLevel::Any;
use models::schema::{Precision, DEFAULT_CATALOG, DEFAULT_DATABASE};
use models::utils::now_timestamp_millis;
use protocol_parser::lines_convert::parse_lines_to_points;
use protocol_parser::open_tsdb::parser::Parser;
use protos::kv_service::WritePointsRequest;
use tokio::io::AsyncReadExt;
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use trace::info;

use crate::server;
use crate::server::{Error, ServiceHandle};
use crate::spi::service::Service;

const MILLISECOND_TIMESTAMP: i64 = 1_000_000_000_000;

pub struct TcpService {
    handle: Option<ServiceHandle<server::Result<()>>>,
    coord: CoordinatorRef,
    addr: String,
}

impl TcpService {
    pub fn new(coord: CoordinatorRef, addr: String) -> Self {
        Self {
            handle: None,
            coord,
            addr,
        }
    }
}

#[async_trait]
impl Service for TcpService {
    fn start(&mut self) -> server::Result<()> {
        let (shutdown, _rx) = oneshot::channel();
        let coord = self.coord.clone();
        let addr = self.addr.clone();
        let join_handle = tokio::spawn(async move {
            let listener = TcpListener::bind(&addr).await.unwrap();
            loop {
                let (mut stream, _) = listener.accept().await.map_err(|e| Error::Common {
                    reason: format!("{:?}", e),
                })?;
                let coord = coord.clone();
                tokio::spawn(async move {
                    let parser = Parser::new(now_timestamp_millis());
                    let mut buffer = Vec::with_capacity(1024);
                    'inner: loop {
                        if stream
                            .read_buf(&mut buffer)
                            .await
                            .map_err(|e| Error::Common {
                                reason: format!("{:?}", e),
                            })?
                            == 0
                        {
                            break 'inner;
                        }
                        if let Ok((mut lines, pos)) = parser.parse_tcp(&mut buffer) {
                            if lines.is_empty() {
                                continue;
                            }

                            lines.iter_mut().for_each(|line| {
                                let mut bit = line.timestamp / MILLISECOND_TIMESTAMP;
                                while bit > 10 {
                                    line.timestamp /= 10;
                                    bit = line.timestamp / MILLISECOND_TIMESTAMP;
                                }
                                while bit == 0 {
                                    line.timestamp *= 10;
                                    bit = line.timestamp / MILLISECOND_TIMESTAMP;
                                }
                            });
                            let points = parse_lines_to_points(DEFAULT_DATABASE, &lines);
                            let req = WritePointsRequest {
                                version: 1,
                                meta: None,
                                points,
                            };
                            coord
                                .write_points(
                                    DEFAULT_CATALOG.to_string(),
                                    Any,
                                    Precision::MS,
                                    req,
                                    None,
                                )
                                .await
                                .map_err(|e| Error::Common {
                                    reason: format!("open opentsdb write point failed: {:?}", e),
                                })?;
                            buffer.drain(..pos);
                        }
                    }
                    Ok::<(), Error>(())
                });
            }
        });
        self.handle = Some(ServiceHandle::new(
            "tcp service".to_string(),
            join_handle,
            shutdown,
        ));

        info!("tcp server start addr: {}", self.addr);

        Ok(())
    }

    async fn stop(&mut self, force: bool) {
        if let Some(stop) = self.handle.take() {
            stop.shutdown(force).await
        };
    }
}

#[cfg(test)]
pub mod test {

    #[tokio::test]
    async fn test() {
        println!("{}", 10_u64.pow(10));
    }
}
