use std::collections::HashMap;
use std::fmt::format;
use std::net::{self, SocketAddr};
use std::sync::Arc;

use clap::ErrorKind::NoEquals;
use coordinator::command::*;
use coordinator::errors::*;
use coordinator::file_info::get_files_meta;
use coordinator::reader::{QueryExecutor, ReaderIterator};
use coordinator::service::CoordinatorRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use futures::future::ok;
use futures::{executor, Future};
use meta::MetaRef;
use models::auth::user::{ROOT, ROOT_PWD};
use models::meta_data::{self, VnodeInfo};
use models::predicate::domain::Predicate;
use protos::kv_service::{Meta, WritePointsRequest};
use snafu::ResultExt;
use spi::server::dbms::DBMSRef;
use tokio::fs::File;
use tokio::io::{AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc::Sender;
use tokio::sync::oneshot;
// use std::net::{TcpListener, TcpStream};
use tokio::sync::oneshot::Receiver;
use tokio::time::{self, Duration};
use trace::{debug, error, info};
use tskv::engine::EngineRef;
use tskv::iterator::{QueryOption, TableScanMetrics};
use tskv::VersionEdit;

use super::vnode_manager::VnodeManager;
use crate::server;
use crate::server::{Service, ServiceHandle};

pub struct TcpService {
    addr: SocketAddr,
    coord: CoordinatorRef,
    handle: Option<ServiceHandle<()>>,
}

impl TcpService {
    pub fn new(coord: CoordinatorRef, addr: SocketAddr) -> Self {
        Self {
            addr,
            coord,
            handle: None,
        }
    }
}

#[async_trait::async_trait]
impl Service for TcpService {
    fn start(&mut self) -> Result<(), server::Error> {
        info!("tcp server start: {}", self.addr.to_string());
        let (shutdown, rx) = oneshot::channel();

        let addr = self.addr;
        let coord = self.coord.clone();

        let join_handle = tokio::spawn(service_run(addr, coord));
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

async fn service_run(addr: SocketAddr, coord: CoordinatorRef) {
    let listener = TcpListener::bind(addr.to_string()).await.unwrap();
    info!("tcp server start addr: {}", addr);

    loop {
        match listener.accept().await {
            Ok((client, address)) => {
                debug!("tcp client address: {}", address);

                let coord = coord.clone();
                let processor_fn = async move {
                    let result = process_client(client, coord).await;
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

async fn process_client(mut client: TcpStream, coord: CoordinatorRef) -> CoordinatorResult<()> {
    if let Some(kv_inst) = coord.store_engine() {
        loop {
            let recv_cmd = recv_command(&mut client).await?;
            match recv_cmd {
                CoordinatorTcpCmd::WriteVnodePointCmd(cmd) => {
                    process_vnode_write_command(&mut client, cmd, kv_inst.clone()).await?;
                }

                CoordinatorTcpCmd::AdminStatementCmd(cmd) => {
                    process_admin_statement_command(&mut client, cmd, coord.clone()).await?;
                }

                CoordinatorTcpCmd::QueryRecordBatchCmd(cmd) => {
                    process_query_record_batch_command(
                        &mut client,
                        cmd,
                        kv_inst.clone(),
                        coord.meta_manager(),
                    )
                    .await?;
                }

                CoordinatorTcpCmd::FetchVnodeSummaryCmd(cmd) => {
                    process_fetch_vnode_summary_command(
                        &mut client,
                        cmd,
                        kv_inst.clone(),
                        coord.meta_manager(),
                    )
                    .await?;
                }

                CoordinatorTcpCmd::ApplyVnodeSummaryCmd(cmd) => {
                    process_apply_vnode_summary_command(
                        &mut client,
                        cmd,
                        kv_inst.clone(),
                        coord.meta_manager(),
                    )
                    .await?
                }

                _ => {}
            }
        }
    } else {
        Err(CoordinatorError::KvInstanceNotFound {
            vnode_id: 0,
            node_id: coord.node_id(),
        })
    }
}

async fn process_vnode_write_command(
    client: &mut TcpStream,
    cmd: WriteVnodeRequest,
    kv_inst: EngineRef,
) -> CoordinatorResult<()> {
    let req = WritePointsRequest {
        version: 1,
        meta: Some(Meta {
            tenant: cmd.tenant.clone(),
            user: None,
            password: None,
        }),
        points: cmd.data,
    };

    let mut resp = StatusResponse {
        code: SUCCESS_RESPONSE_CODE,
        data: "".to_string(),
    };
    if let Err(err) = kv_inst.write(cmd.vnode_id, req).await {
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
    coord: CoordinatorRef,
) -> CoordinatorResult<()> {
    let mut rsp_data = "".to_string();
    let mut rsp_code = SUCCESS_RESPONSE_CODE;

    let meta = coord.meta_manager();
    // coord.store_engine() is not none
    let engine = coord.store_engine().unwrap();
    match cmd.stmt {
        AdminStatementType::DropDB { db } => {
            let _ = engine.drop_database(&cmd.tenant, &db).await;
        }

        AdminStatementType::DropTable { db, table } => {
            let _ = engine.drop_table(&cmd.tenant, &db, &table).await;
        }

        AdminStatementType::DropColumn { db, table, column } => {
            let _ = engine
                .drop_table_column(&cmd.tenant, &db, &table, &column)
                .await;
        }
        AdminStatementType::AddColumn { db, table, column } => {
            let _ = engine
                .add_table_column(&cmd.tenant, &db, &table, column)
                .await;
        }
        AdminStatementType::AlterColumn {
            db,
            table,
            column_name,
            new_column,
        } => {
            let _ = engine
                .change_table_column(&cmd.tenant, &db, &table, &column_name, new_column)
                .await;
        }

        AdminStatementType::DeleteVnode { db, vnode_id } => {
            let manager = VnodeManager::new(meta, engine, coord.node_id());
            if let Err(err) = manager.drop_vnode(&cmd.tenant, vnode_id).await {
                rsp_code = FAILED_RESPONSE_CODE;
                rsp_data = err.to_string();
            }
        }

        AdminStatementType::CopyVnode { vnode_id } => {
            let manager = VnodeManager::new(meta, engine, coord.node_id());
            if let Err(err) = manager.copy_vnode(&cmd.tenant, vnode_id).await {
                rsp_code = FAILED_RESPONSE_CODE;
                rsp_data = err.to_string();
            }
        }

        AdminStatementType::MoveVnode { vnode_id } => {
            let manager = VnodeManager::new(meta, engine, coord.node_id());
            if let Err(err) = manager.move_vnode(&cmd.tenant, vnode_id).await {
                rsp_code = FAILED_RESPONSE_CODE;
                rsp_data = err.to_string();
            }
        }

        AdminStatementType::CompactVnode { vnode_ids } => {
            if let Err(err) = engine.compact(vnode_ids).await {
                rsp_code = FAILED_RESPONSE_CODE;
                rsp_data = err.to_string();
            }
        }

        AdminStatementType::GetVnodeFilesMeta { db, vnode_id } => {
            let owner = models::schema::make_owner(&cmd.tenant, &db);
            let storage_opt = engine.get_storage_options();

            engine.flush_tsfamily(&cmd.tenant, &db, vnode_id).await?;

            let path = storage_opt.ts_family_dir(&owner, vnode_id);
            info!("get files meta: {:?}", path);
            let meta = get_files_meta(&path.as_path().to_string_lossy()).await?;
            rsp_data = serde_json::to_string(&meta)
                .map_err(|e| CoordinatorError::CommonError { msg: e.to_string() })?;

            info!("files meta: {:?}", meta);
        }

        AdminStatementType::DownloadFile {
            db,
            vnode_id,
            filename,
        } => {
            let owner = models::schema::make_owner(&cmd.tenant, &db);
            let storage_opt = engine.get_storage_options();
            let data_dir = storage_opt.ts_family_dir(&owner, vnode_id);
            let path = data_dir.join(filename);
            info!("download file: {}", path.display());

            let mut file = File::open(path).await?;
            let size = file.metadata().await?.len();

            client.write_u64(size).await?;
            tokio::io::copy(&mut file, client).await?;
            return Ok(());
        }
    }

    let resp = StatusResponse {
        code: rsp_code,
        data: rsp_data,
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

    let executor = QueryExecutor::new(option, Some(kv_inst), meta, sender.clone());
    if let Err(err) = executor.local_node_executor(vnodes).await {
        info!("select statement execute failed: {}", err.to_string());
        let _ = sender.send(Err(err)).await;
    } else {
        info!("select statement execute success");
    }
}

async fn process_fetch_vnode_summary_command(
    client: &mut TcpStream,
    cmd: FetchVnodeSummaryRequest,
    engine: EngineRef,
    meta: MetaRef,
) -> CoordinatorResult<()> {
    let version_edit = match engine
        .get_vnode_summary(&cmd.tenant, &cmd.database, cmd.vnode_id)
        .await
    {
        Ok(version_edit) => version_edit,
        Err(e) => {
            let resp = CoordinatorTcpCmd::StatusResponseCmd(StatusResponse {
                code: FAILED_RESPONSE_CODE,
                data: format!("failed to get vnode summary: {:?}", e),
            });
            return send_command(client, &resp).await;
        }
    };

    let resp = if let Some(ve) = version_edit {
        match ve.encode() {
            Ok(ve_bytes) => {
                CoordinatorTcpCmd::FetchVnodeSummaryResponseCmd(FetchVnodeSummaryResponse {
                    version_edit: ve_bytes,
                })
            }
            Err(e) => CoordinatorTcpCmd::StatusResponseCmd(StatusResponse {
                code: FAILED_RESPONSE_CODE,
                data: format!("failed to encode vnode summary: {:?}", e),
            }),
        }
    } else {
        CoordinatorTcpCmd::FetchVnodeSummaryResponseCmd(FetchVnodeSummaryResponse {
            version_edit: vec![],
        })
    };

    send_command(client, &resp).await?;

    Ok(())
}

async fn process_apply_vnode_summary_command(
    client: &mut TcpStream,
    cmd: ApplyVnodeSummaryRequest,
    engine: EngineRef,
    meta: MetaRef,
) -> CoordinatorResult<()> {
    let version_edit = match VersionEdit::decode(&cmd.version_edit) {
        Ok(ve) => ve,
        Err(e) => {
            let resp = CoordinatorTcpCmd::StatusResponseCmd(StatusResponse {
                code: FAILED_RESPONSE_CODE,
                data: format!("failed to decode vnode summary: {:?}", e),
            });
            return send_command(client, &resp).await;
        }
    };

    let resp = match engine
        .apply_vnode_summary(&cmd.tenant, &cmd.database, cmd.vnode_id, version_edit)
        .await
    {
        Ok(_) => CoordinatorTcpCmd::StatusResponseCmd(StatusResponse {
            code: SUCCESS_RESPONSE_CODE,
            data: "".to_string(),
        }),
        Err(e) => CoordinatorTcpCmd::StatusResponseCmd(StatusResponse {
            code: FAILED_RESPONSE_CODE,
            data: format!("failed to apply vnode summary: {:?}", e),
        }),
    };

    send_command(client, &resp).await?;

    Ok(())
}
