use coordinator::reader::{QueryExecutor, ReaderIterator};
use coordinator::service::CoordinatorRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use futures::future::ok;
use futures::{executor, Future};
use meta::meta_client::MetaRef;
use models::predicate::domain::Predicate;
use snafu::ResultExt;
use spi::query::DEFAULT_CATALOG;
use spi::server::dbms::DBMSRef;
use std::net::{self, SocketAddr};
use tokio::io::BufReader;
use tokio::sync::mpsc::Sender;
use tskv::iterator::{QueryOption, TableScanMetrics};

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

use models::meta_data::{self, VnodeInfo};

use crate::server;

use crate::server::{Service, ServiceHandle};

use trace::{debug, error, info};

pub struct TcpService {
    addr: SocketAddr,
    dbms: DBMSRef,
    coord: CoordinatorRef,
    handle: Option<ServiceHandle<()>>,
}

impl TcpService {
    pub fn new(dbms: DBMSRef, coord: CoordinatorRef, addr: SocketAddr) -> Self {
        Self {
            addr,
            dbms,
            coord,
            handle: None,
        }
    }
}

#[async_trait::async_trait]
impl Service for TcpService {
    fn start(&mut self) -> Result<(), server::Error> {
        let (shutdown, rx) = oneshot::channel();

        let addr = self.addr;
        let dbms = self.dbms.clone();
        let coord = self.coord.clone();

        let join_handle = tokio::spawn(service_run(addr, dbms, coord));
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

async fn service_run(addr: SocketAddr, dbms: DBMSRef, coord: CoordinatorRef) {
    let listener = TcpListener::bind(addr.to_string()).await.unwrap();
    info!("tcp server start addr: {}", addr);

    loop {
        match listener.accept().await {
            Ok((client, address)) => {
                debug!("tcp client address: {}", address);

                let dbms = dbms.clone();
                let coord = coord.clone();
                let processor_fn = async move {
                    let result = process_client(client, dbms, coord).await;
                    info!("process client {} result: {:#?}", address, result);
                };

                let _ = tokio::spawn(processor_fn);
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
    coord: CoordinatorRef,
) -> CoordinatorResult<()> {
    loop {
        let recv_cmd = recv_command(&mut client).await?;

        match recv_cmd {
            CoordinatorTcpCmd::WriteVnodePointCmd(cmd) => {
                process_vnode_write_command(&mut client, cmd, coord.store_engine()).await?;
            }

            CoordinatorTcpCmd::AdminStatementCmd(cmd) => {
                process_admin_statement_command(&mut client, cmd, coord.store_engine()).await?;
            }

            CoordinatorTcpCmd::QueryRecordBatchCmd(cmd) => {
                process_query_record_batch_command(
                    &mut client,
                    cmd,
                    coord.store_engine(),
                    coord.meta_manager(),
                )
                .await?;
            }

            _ => {}
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

    let mut resp = StatusResponse {
        code: SUCCESS_RESPONSE_CODE,
        data: "".to_string(),
    };
    // todo: we should get tenant from token
    let mock_tenant = DEFAULT_CATALOG;
    if let Err(err) = kv_inst.write(cmd.vnode_id, mock_tenant, req).await {
        resp.code = -1;
        resp.data = err.to_string();
        send_command(client, &CoordinatorTcpCmd::StatusResponseCmd(resp)).await?;
        Err(err.into())
    } else {
        info!("success write data to vnode: {}", cmd.vnode_id);
        send_command(client, &CoordinatorTcpCmd::StatusResponseCmd(resp)).await?;
        Ok(())
    }
}

async fn process_admin_statement_command(
    client: &mut TcpStream,
    cmd: AdminStatementRequest,
    engine: EngineRef,
) -> CoordinatorResult<()> {
    match cmd.stmt {
        AdminStatementType::DropDB(db) => {
            let _ = engine.drop_database(&cmd.tenant, &db);
        }

        AdminStatementType::DropTable(db, table) => {
            let _ = engine.drop_table(&cmd.tenant, &db, &table);
        }
    }

    let resp = StatusResponse {
        code: SUCCESS_RESPONSE_CODE,
        data: "".to_string(),
    };
    send_command(client, &CoordinatorTcpCmd::StatusResponseCmd(resp)).await?;

    Ok(())
}

async fn process_query_record_batch_command(
    client: &mut TcpStream,
    cmd: QueryRecordBatchRequest,
    kv_inst: EngineRef,
    meta: MetaRef,
) -> CoordinatorResult<()> {
    let (mut iterator, sender) = ReaderIterator::new();
    let _ = tokio::spawn(query_record_batch(cmd, kv_inst, meta, sender));

    loop {
        if let Some(item) = iterator.next().await {
            match item {
                Ok(record) => {
                    let resp = RecordBatchResponse { record };
                    send_command(client, &CoordinatorTcpCmd::RecordBatchResponseCmd(resp)).await?;
                }

                Err(err) => {
                    let resp = StatusResponse {
                        code: FAILED_RESPONSE_CODE,
                        data: err.to_string(),
                    };

                    send_command(client, &CoordinatorTcpCmd::StatusResponseCmd(resp)).await?;
                    break;
                }
            }
        } else {
            let resp = StatusResponse {
                code: FINISH_RESPONSE_CODE,
                data: "".to_string(),
            };

            send_command(client, &CoordinatorTcpCmd::StatusResponseCmd(resp)).await?;
            break;
        }
    }

    Ok(())
}

async fn query_record_batch(
    cmd: QueryRecordBatchRequest,
    kv_inst: EngineRef,
    meta: MetaRef,
    sender: Sender<CoordinatorResult<RecordBatch>>,
) {
    let filter = Arc::new(
        Predicate::default()
            .set_limit(cmd.args.limit)
            .push_down_filter(&cmd.expr.filters, &cmd.expr.table_schema),
    );

    let plan_metrics = ExecutionPlanMetricsSet::new();
    let scan_metrics = TableScanMetrics::new(&plan_metrics, 0);
    let option = QueryOption::new(
        cmd.args.batch_size,
        cmd.args.tenant.clone(),
        filter,
        cmd.expr.df_schema,
        cmd.expr.table_schema,
        scan_metrics.tskv_metrics(),
    );

    let node_id = meta.node_id();
    let mut vnodes = Vec::with_capacity(cmd.args.vnode_ids.len());
    for id in cmd.args.vnode_ids.iter() {
        vnodes.push(VnodeInfo { id: *id, node_id })
    }

    let executor = QueryExecutor::new(option, kv_inst, meta, sender.clone());
    if let Err(err) = executor.local_node_executor(vnodes).await {
        info!("select statement execute failed: {}", err.to_string());
        let _ = sender.send(Err(err)).await;
    } else {
        info!("select statement execute success");
    }
}
