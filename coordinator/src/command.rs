#![allow(clippy::large_enum_variant)]
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::ipc::{reader::StreamReader, writer::StreamWriter};
use models::predicate::domain::{PredicateRef, QueryArgs, QueryExpr};
use models::schema::{TableColumn, TskvTableSchema};
use protos::kv_service::WritePointsRpcRequest;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::mpsc::Sender as MpscSender;
use tokio::sync::oneshot::Sender as OneShotSender;

use datafusion::arrow::record_batch::RecordBatch;

// use std::net::{TcpListener, TcpStream};
use models::meta_data::VnodeId;
use tokio::net::{TcpListener, TcpStream};
use trace::info;
use tskv::iterator::QueryOption;

use crate::errors::{
    CoordinatorError::{self, *},
    CoordinatorResult,
};

/* **************************************************************************************** */
/* ************************* tcp service command ****************************************** */
/* **************************************************************************************** */
pub const WRITE_VNODE_POINT_COMMAND: u32 = 1;
pub const ADMIN_STATEMENT_COMMAND: u32 = 2;
pub const QUERY_RECORD_BATCH_COMMAND: u32 = 3;

pub const STATUS_RESPONSE_COMMAND: u32 = 100;
pub const RECORD_BATCH_RESPONSE_COMMAND: u32 = 101;

pub const FAILED_RESPONSE_CODE: i32 = -1;
pub const FINISH_RESPONSE_CODE: i32 = 0;
pub const SUCCESS_RESPONSE_CODE: i32 = 1;

#[derive(Debug, Clone)]
pub enum CoordinatorTcpCmd {
    WriteVnodePointCmd(WriteVnodeRequest),
    AdminStatementCmd(AdminStatementRequest),
    QueryRecordBatchCmd(QueryRecordBatchRequest),

    StatusResponseCmd(StatusResponse),
    RecordBatchResponseCmd(RecordBatchResponse),
}

pub async fn send_command(conn: &mut TcpStream, cmd: &CoordinatorTcpCmd) -> CoordinatorResult<()> {
    match cmd {
        CoordinatorTcpCmd::StatusResponseCmd(val) => {
            conn.write_all(&STATUS_RESPONSE_COMMAND.to_be_bytes())
                .await?;
            val.send_cmd(conn).await
        }

        CoordinatorTcpCmd::WriteVnodePointCmd(val) => {
            conn.write_all(&WRITE_VNODE_POINT_COMMAND.to_be_bytes())
                .await?;
            val.send_cmd(conn).await
        }

        CoordinatorTcpCmd::QueryRecordBatchCmd(val) => {
            conn.write_all(&QUERY_RECORD_BATCH_COMMAND.to_be_bytes())
                .await?;
            val.send_cmd(conn).await
        }

        CoordinatorTcpCmd::RecordBatchResponseCmd(val) => {
            conn.write_all(&RECORD_BATCH_RESPONSE_COMMAND.to_be_bytes())
                .await?;
            val.send_cmd(conn).await
        }
        CoordinatorTcpCmd::AdminStatementCmd(val) => {
            conn.write_all(&ADMIN_STATEMENT_COMMAND.to_be_bytes())
                .await?;
            val.send_cmd(conn).await
        }
    }
}

pub async fn recv_command(conn: &mut TcpStream) -> CoordinatorResult<CoordinatorTcpCmd> {
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
        RECORD_BATCH_RESPONSE_COMMAND => {
            let cmd = RecordBatchResponse::recv_data(conn).await?;
            Ok(CoordinatorTcpCmd::RecordBatchResponseCmd(cmd))
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

/* ********************************************************************************************** */
/* ************************** internal service command ****************************************** */
/* ********************************************************************************************** */
#[derive(Debug)]
pub enum CoordinatorIntCmd {
    WritePointsCmd(WritePointsRequest),
    SelectStatementCmd(SelectStatementRequest),
    AdminStatementCmd(AdminStatementRequest, OneShotSender<CoordinatorResult<()>>),
}

#[derive(Debug)]
pub struct WritePointsRequest {
    pub tenant: String,
    pub level: models::consistency_level::ConsistencyLevel,
    pub request: WritePointsRpcRequest,

    pub sender: OneShotSender<CoordinatorResult<()>>,
}

#[derive(Debug)]
pub struct SelectStatementRequest {
    pub option: QueryOption,
    pub sender: MpscSender<CoordinatorResult<RecordBatch>>,
}
