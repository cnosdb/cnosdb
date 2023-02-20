#![allow(clippy::large_enum_variant)]
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::ipc::reader::StreamReader;
use datafusion::arrow::ipc::writer::StreamWriter;
use datafusion::arrow::record_batch::RecordBatch;
// use std::net::{TcpListener, TcpStream};
use models::meta_data::VnodeId;
use models::predicate::domain::{PredicateRef, QueryArgs, QueryExpr};
use models::schema::{TableColumn, TskvTableSchema};
use protos::kv_service::WritePointsRequest;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc::Sender as MpscSender;
use tokio::sync::oneshot::Sender as OneShotSender;
use trace::info;
use tskv::byte_utils;
use tskv::iterator::QueryOption;

use crate::errors::CoordinatorError::{self, *};
use crate::errors::CoordinatorResult;

/* **************************************************************************************** */
/* ************************* tcp service command ****************************************** */
/* **************************************************************************************** */
pub const WRITE_VNODE_POINT_COMMAND: u32 = 1;
pub const ADMIN_STATEMENT_COMMAND: u32 = 2;
pub const QUERY_RECORD_BATCH_COMMAND: u32 = 3;
pub const FETCH_VNODE_SUMMARY_COMMAND: u32 = 4;
pub const APPLY_VNODE_SUMMARY_COMMAND: u32 = 5;

pub const STATUS_RESPONSE_COMMAND: u32 = 100;
pub const RECORD_BATCH_RESPONSE_COMMAND: u32 = 101;
pub const FETCH_VNODE_SUMMARY_RESPONSE_COMMAND: u32 = 102;

pub const FAILED_RESPONSE_CODE: i32 = -1;
pub const FINISH_RESPONSE_CODE: i32 = 0;
pub const SUCCESS_RESPONSE_CODE: i32 = 1;

#[derive(Debug, Clone)]
pub enum CoordinatorTcpCmd {
    WriteVnodePointCmd(WriteVnodeRequest),
    AdminStatementCmd(AdminStatementRequest),
    QueryRecordBatchCmd(QueryRecordBatchRequest),
    FetchVnodeSummaryCmd(FetchVnodeSummaryRequest),
    ApplyVnodeSummaryCmd(ApplyVnodeSummaryRequest),

    StatusResponseCmd(StatusResponse),
    RecordBatchResponseCmd(RecordBatchResponse),
    FetchVnodeSummaryResponseCmd(FetchVnodeSummaryResponse),
}

impl CoordinatorTcpCmd {
    pub fn command_type(&self) -> u32 {
        match self {
            CoordinatorTcpCmd::WriteVnodePointCmd(_) => WRITE_VNODE_POINT_COMMAND,
            CoordinatorTcpCmd::AdminStatementCmd(_) => ADMIN_STATEMENT_COMMAND,
            CoordinatorTcpCmd::QueryRecordBatchCmd(_) => QUERY_RECORD_BATCH_COMMAND,
            CoordinatorTcpCmd::FetchVnodeSummaryCmd(_) => FETCH_VNODE_SUMMARY_COMMAND,
            CoordinatorTcpCmd::ApplyVnodeSummaryCmd(_) => APPLY_VNODE_SUMMARY_COMMAND,
            CoordinatorTcpCmd::StatusResponseCmd(_) => STATUS_RESPONSE_COMMAND,
            CoordinatorTcpCmd::RecordBatchResponseCmd(_) => RECORD_BATCH_RESPONSE_COMMAND,
            CoordinatorTcpCmd::FetchVnodeSummaryResponseCmd(_) => {
                FETCH_VNODE_SUMMARY_RESPONSE_COMMAND
            }
        }
    }
}

pub async fn send_command(conn: &mut TcpStream, cmd: &CoordinatorTcpCmd) -> CoordinatorResult<()> {
    let ttl = tokio::time::Duration::from_secs(5 * 60);

    match tokio::time::timeout(ttl, warp_send_command(conn, cmd)).await {
        Ok(res) => res,

        Err(timeout) => Err(CoordinatorError::RequestTimeout {
            id: cmd.command_type(),
            elapsed: timeout.to_string(),
        }),
    }
}

pub async fn recv_command(conn: &mut TcpStream) -> CoordinatorResult<CoordinatorTcpCmd> {
    let ttl = tokio::time::Duration::from_secs(5 * 60);

    match tokio::time::timeout(ttl, warp_recv_command(conn)).await {
        Ok(res) => res,

        Err(timeout) => Err(CoordinatorError::RequestTimeout {
            id: 0,
            elapsed: timeout.to_string(),
        }),
    }
}

async fn warp_send_command(conn: &mut TcpStream, cmd: &CoordinatorTcpCmd) -> CoordinatorResult<()> {
    conn.write_all(&cmd.command_type().to_be_bytes()).await?;
    match cmd {
        CoordinatorTcpCmd::StatusResponseCmd(val) => val.send_cmd(conn).await,

        CoordinatorTcpCmd::WriteVnodePointCmd(val) => val.send_cmd(conn).await,

        CoordinatorTcpCmd::QueryRecordBatchCmd(val) => val.send_cmd(conn).await,

        CoordinatorTcpCmd::FetchVnodeSummaryCmd(val) => val.send_cmd(conn).await,

        CoordinatorTcpCmd::ApplyVnodeSummaryCmd(val) => val.send_cmd(conn).await,

        CoordinatorTcpCmd::RecordBatchResponseCmd(val) => val.send_cmd(conn).await,

        CoordinatorTcpCmd::FetchVnodeSummaryResponseCmd(val) => val.send_cmd(conn).await,

        CoordinatorTcpCmd::AdminStatementCmd(val) => val.send_cmd(conn).await,
    }
}

async fn warp_recv_command(conn: &mut TcpStream) -> CoordinatorResult<CoordinatorTcpCmd> {
    let mut tmp_buf: [u8; 4] = [0; 4];
    conn.read_exact(&mut tmp_buf).await?;
    let cmd_type = u32::from_be_bytes(tmp_buf);

    match cmd_type {
        STATUS_RESPONSE_COMMAND => {
            let cmd = StatusResponse::recv_data(conn).await?;
            Ok(CoordinatorTcpCmd::StatusResponseCmd(cmd))
        }

        WRITE_VNODE_POINT_COMMAND => {
            let cmd = WriteVnodeRequest::recv_data(conn).await?;
            Ok(CoordinatorTcpCmd::WriteVnodePointCmd(cmd))
        }

        QUERY_RECORD_BATCH_COMMAND => {
            let cmd = QueryRecordBatchRequest::recv_data(conn).await?;
            Ok(CoordinatorTcpCmd::QueryRecordBatchCmd(cmd))
        }

        FETCH_VNODE_SUMMARY_COMMAND => {
            let cmd = FetchVnodeSummaryRequest::recv_data(conn).await?;
            Ok(CoordinatorTcpCmd::FetchVnodeSummaryCmd(cmd))
        }

        RECORD_BATCH_RESPONSE_COMMAND => {
            let cmd = RecordBatchResponse::recv_data(conn).await?;
            Ok(CoordinatorTcpCmd::RecordBatchResponseCmd(cmd))
        }

        FETCH_VNODE_SUMMARY_RESPONSE_COMMAND => {
            let cmd = FetchVnodeSummaryResponse::recv_data(conn).await?;
            Ok(CoordinatorTcpCmd::FetchVnodeSummaryResponseCmd(cmd))
        }

        ADMIN_STATEMENT_COMMAND => {
            let cmd = AdminStatementRequest::recv_data(conn).await?;
            Ok(CoordinatorTcpCmd::AdminStatementCmd(cmd))
        }

        _ => Err(UnKnownCoordCmd { cmd: cmd_type }),
    }
}

async fn read_data_len_val(conn: &mut TcpStream) -> CoordinatorResult<Vec<u8>> {
    let len = conn.read_u32().await?;

    let mut data_buf = vec![0; len as usize];
    conn.read_exact(&mut data_buf).await?;

    Ok(data_buf)
}

#[derive(Serialize, Deserialize, Default, Debug, Clone)]
pub struct StatusResponse {
    pub code: i32,
    pub data: String,
}

impl StatusResponse {
    pub async fn send_cmd(&self, conn: &mut TcpStream) -> CoordinatorResult<()> {
        conn.write_all(&self.code.to_be_bytes()).await?;

        conn.write_all(&(self.data.len() as u32).to_be_bytes())
            .await?;
        conn.write_all(self.data.as_bytes()).await?;

        Ok(())
    }

    pub async fn recv_data(conn: &mut TcpStream) -> CoordinatorResult<StatusResponse> {
        let code = conn.read_i32().await?;

        let data_buf = read_data_len_val(conn).await?;
        let data = String::from_utf8(data_buf).map_err(|_| CoordCommandParseErr)?;

        Ok(StatusResponse { code, data })
    }
}

#[derive(Serialize, Deserialize, Default, Debug, Clone)]
pub struct WriteVnodeRequest {
    pub vnode_id: u32,
    pub tenant: String,
    pub data: Vec<u8>,
}

impl WriteVnodeRequest {
    pub async fn send_cmd(&self, conn: &mut TcpStream) -> CoordinatorResult<()> {
        conn.write_all(&self.vnode_id.to_be_bytes()).await?;
        conn.write_all(&(self.tenant.len() as u32).to_be_bytes())
            .await?;
        conn.write_all(self.tenant.as_bytes()).await?;
        conn.write_all(&(self.data.len() as u32).to_be_bytes())
            .await?;
        conn.write_all(&self.data).await?;

        Ok(())
    }

    pub async fn recv_data(conn: &mut TcpStream) -> CoordinatorResult<WriteVnodeRequest> {
        let vnode_id = conn.read_u32().await?;
        let tenant = unsafe { String::from_utf8_unchecked(read_data_len_val(conn).await?) };
        let data = read_data_len_val(conn).await?;

        Ok(WriteVnodeRequest {
            vnode_id,
            tenant,
            data,
        })
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum AdminStatementType {
    DropDB {
        db: String,
    },
    DropTable {
        db: String,
        table: String,
    },

    DeleteVnode {
        db: String,
        vnode_id: u32,
    },

    CopyVnode {
        vnode_id: u32,
    },

    MoveVnode {
        vnode_id: u32,
    },

    CompactVnode {
        vnode_ids: Vec<u32>,
    },

    GetVnodeFilesMeta {
        db: String,
        vnode_id: u32,
    },

    DownloadFile {
        db: String,
        vnode_id: u32,
        filename: String,
    },

    DropColumn {
        db: String,
        table: String,
        column: String,
    },

    AddColumn {
        db: String,
        table: String,
        column: TableColumn,
    },
    AlterColumn {
        db: String,
        table: String,
        column_name: String,
        new_column: TableColumn,
    },
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct AdminStatementRequest {
    pub tenant: String,
    pub stmt: AdminStatementType,
}

impl AdminStatementRequest {
    pub async fn send_cmd(&self, conn: &mut TcpStream) -> CoordinatorResult<()> {
        let d = bincode::serialize(&self).map_err(|e| CoordinatorError::CommonError {
            msg: "e".to_string(),
        })?;
        conn.write_all(&(d.len() as u32).to_be_bytes()).await?;
        conn.write_all(&d).await?;

        Ok(())
    }

    pub async fn recv_data(conn: &mut TcpStream) -> CoordinatorResult<AdminStatementRequest> {
        let data_buf = read_data_len_val(conn).await?;
        let req = bincode::deserialize::<AdminStatementRequest>(&data_buf)
            .map_err(|e| tskv::Error::Decode { source: (e) })?;

        Ok(req)
    }
}

#[derive(Debug, Clone)]
pub struct QueryRecordBatchRequest {
    pub args: QueryArgs,
    pub expr: QueryExpr,
}

impl QueryRecordBatchRequest {
    pub async fn send_cmd(&self, conn: &mut TcpStream) -> CoordinatorResult<()> {
        let d = bincode::serialize(&self.args).map_err(|e| CoordinatorError::CommonError {
            msg: format!("{}", e),
        })?;
        conn.write_all(&(d.len() as u32).to_be_bytes()).await?;
        conn.write_all(&d).await?;

        let data = QueryExpr::encode(&self.expr)?;
        conn.write_all(&(data.len() as u32).to_be_bytes()).await?;
        conn.write_all(&data).await?;

        Ok(())
    }

    pub async fn recv_data(conn: &mut TcpStream) -> CoordinatorResult<QueryRecordBatchRequest> {
        let data_buf = read_data_len_val(conn).await?;
        let args = bincode::deserialize::<QueryArgs>(&data_buf)
            .map_err(|e| tskv::Error::Decode { source: (e) })?;

        let data_buf = read_data_len_val(conn).await?;
        let expr = QueryExpr::decode(data_buf)?;

        Ok(QueryRecordBatchRequest { args, expr })
    }
}

#[derive(Debug, Clone)]
pub struct FetchVnodeSummaryRequest {
    pub tenant: String,
    pub database: String,
    pub vnode_id: u32,
}

impl FetchVnodeSummaryRequest {
    pub async fn send_cmd(&self, conn: &mut TcpStream) -> CoordinatorResult<()> {
        let tenant_buf = self.tenant.as_bytes();
        conn.write_all(&(tenant_buf.len() as u32).to_be_bytes())
            .await?;
        conn.write_all(tenant_buf).await?;

        let database_buf = self.database.as_bytes();
        conn.write_all(&(database_buf.len() as u32).to_be_bytes())
            .await?;
        conn.write_all(database_buf).await?;

        conn.write_all(&4_u32.to_be_bytes()).await?;
        conn.write_all(&self.vnode_id.to_be_bytes()).await?;

        Ok(())
    }

    pub async fn recv_data(conn: &mut TcpStream) -> CoordinatorResult<FetchVnodeSummaryRequest> {
        let tenant_buf = read_data_len_val(conn).await?;
        let tenant = String::from_utf8(tenant_buf).map_err(|_| CoordCommandParseErr)?;

        let database_buf = read_data_len_val(conn).await?;
        let database = String::from_utf8(database_buf).map_err(|_| CoordCommandParseErr)?;

        let vnode_id_buf = read_data_len_val(conn).await?;
        if vnode_id_buf.len() < 4 {
            return Err(CoordCommandParseErr);
        }
        let vnode_id = byte_utils::decode_be_u32(&vnode_id_buf);

        Ok(FetchVnodeSummaryRequest {
            tenant,
            database,
            vnode_id,
        })
    }
}

#[derive(Debug, Clone)]
pub struct ApplyVnodeSummaryRequest {
    pub tenant: String,
    pub database: String,
    pub vnode_id: u32,
    pub version_edit: Vec<u8>,
}

impl ApplyVnodeSummaryRequest {
    pub async fn send_cmd(&self, conn: &mut TcpStream) -> CoordinatorResult<()> {
        let tenant_buf = self.database.as_bytes();
        conn.write_all(&(tenant_buf.len() as u32).to_be_bytes())
            .await?;
        conn.write_all(tenant_buf).await?;

        let database_buf = self.database.as_bytes();
        conn.write_all(&(database_buf.len() as u32).to_be_bytes())
            .await?;
        conn.write_all(database_buf).await?;

        conn.write_all(&4_u32.to_be_bytes()).await?;
        conn.write_all(&self.vnode_id.to_be_bytes()).await?;

        conn.write_all(&(self.version_edit.len() as u32).to_be_bytes())
            .await?;
        conn.write_all(&self.version_edit).await?;

        Ok(())
    }

    pub async fn recv_data(conn: &mut TcpStream) -> CoordinatorResult<ApplyVnodeSummaryRequest> {
        let tenant_buf = read_data_len_val(conn).await?;
        let tenant = String::from_utf8(tenant_buf).map_err(|_| CoordCommandParseErr)?;

        let database_buf = read_data_len_val(conn).await?;
        let database = String::from_utf8(database_buf).map_err(|_| CoordCommandParseErr)?;

        let vnode_id_buf = read_data_len_val(conn).await?;
        if vnode_id_buf.len() < 4 {
            return Err(CoordCommandParseErr);
        }
        let vnode_id = byte_utils::decode_be_u32(&vnode_id_buf);

        let version_edit = read_data_len_val(conn).await?;

        Ok(ApplyVnodeSummaryRequest {
            tenant,
            database,
            vnode_id,
            version_edit,
        })
    }
}

#[derive(Debug, Clone)]
pub struct RecordBatchResponse {
    pub record: RecordBatch,
}

impl RecordBatchResponse {
    pub async fn send_cmd(&self, conn: &mut TcpStream) -> CoordinatorResult<()> {
        let buffer: Vec<u8> = Vec::new();
        let mut stream_writer = StreamWriter::try_new(buffer, &self.record.schema())?;
        stream_writer.write(&self.record)?;
        stream_writer.finish()?;
        let data = stream_writer.into_inner()?;

        conn.write_all(&(data.len() as u32).to_be_bytes()).await?;
        conn.write_all(&data).await?;

        Ok(())
    }

    pub async fn recv_data(conn: &mut TcpStream) -> CoordinatorResult<RecordBatchResponse> {
        let data_buf = read_data_len_val(conn).await?;

        let mut stream_reader = StreamReader::try_new(std::io::Cursor::new(data_buf), None)?;
        let record = stream_reader.next().ok_or(CoordinatorError::CommonError {
            msg: "record batch is None".to_string(),
        })??;

        Ok(RecordBatchResponse { record })
    }
}

#[derive(Debug, Clone)]
pub struct FetchVnodeSummaryResponse {
    pub version_edit: Vec<u8>,
}

impl FetchVnodeSummaryResponse {
    pub async fn send_cmd(&self, conn: &mut TcpStream) -> CoordinatorResult<()> {
        conn.write_all(&(self.version_edit.len() as u32).to_be_bytes())
            .await?;
        conn.write_all(&self.version_edit).await?;

        Ok(())
    }

    pub async fn recv_data(conn: &mut TcpStream) -> CoordinatorResult<FetchVnodeSummaryResponse> {
        let version_edit = read_data_len_val(conn).await?;

        Ok(FetchVnodeSummaryResponse { version_edit })
    }
}

/* ********************************************************************************************** */
/* ************************** internal service command ****************************************** */
/* ********************************************************************************************** */

#[derive(Debug)]
pub struct WriteRequest {
    pub tenant: String,
    pub level: models::consistency_level::ConsistencyLevel,
    pub request: WritePointsRequest,
}

#[derive(Debug, Clone)]
pub enum VnodeManagerCmdType {
    Copy(u64), // dst node id
    Move(u64), // dst node id
    Drop,      //
    Compact,   // dst node id
}
