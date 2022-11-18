use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::ipc::{reader::StreamReader, writer::StreamWriter};
use models::predicate::domain::{PredicateRef, QueryExpr};
use models::schema::TskvTableSchema;
use protos::kv_service::WritePointsRpcRequest;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::mpsc::Sender as MpscSender;
use tokio::sync::oneshot::Sender as OneShotSender;

use datafusion::arrow::record_batch::RecordBatch;

// use std::net::{TcpListener, TcpStream};
use tokio::net::{TcpListener, TcpStream};
use tskv::iterator::QueryOption;

use crate::errors::{
    CoordinatorError::{self, *},
    CoordinatorResult,
};
use crate::reader::ReaderIteratorRef;

/* ************************* tcp service command ********************************* */
pub const STATUS_RESPONSE_COMMAND: u32 = 1;
pub const WRITE_VNODE_POINT_COMMAND: u32 = 2;
pub const EXECUTE_STATEMENT_COMMAND: u32 = 3;
pub const QUERY_RECORD_BATCH_COMMAND: u32 = 4;
pub const RECORD_BATCH_RESPONSE_COMMAND: u32 = 5;

pub const FAILED_RESPONSE_CODE: i32 = -1;
pub const FINISH_RESPONSE_CODE: i32 = 0;
pub const SUCCESS_RESPONSE_CODE: i32 = 1;

pub enum CoordinatorTcpCmd {
    StatusResponseCmd(StatusResponse),
    WriteVnodePointCmd(WriteVnodeRequest),
    QueryRecordBatchCmd(QueryRecordBatchRequest),
    RecordBatchResponseCmd(RecordBatchResponse),
}

pub async fn send_command(conn: &mut TcpStream, cmd: &CoordinatorTcpCmd) -> CoordinatorResult<()> {
    match cmd {
        CoordinatorTcpCmd::StatusResponseCmd(val) => val.send_cmd(conn).await,
        CoordinatorTcpCmd::WriteVnodePointCmd(val) => val.send_cmd(conn).await,
        CoordinatorTcpCmd::QueryRecordBatchCmd(val) => val.send_cmd(conn).await,
        CoordinatorTcpCmd::RecordBatchResponseCmd(val) => val.send_cmd(conn).await,
    }
}

pub async fn recv_command(conn: &mut TcpStream) -> CoordinatorResult<CoordinatorTcpCmd> {
    let mut tmp_buf: [u8; 4] = [0; 4];
    conn.read_exact(&mut tmp_buf).await?;

    let cmd_type = u32::from_be_bytes(tmp_buf);
    if cmd_type == STATUS_RESPONSE_COMMAND {
        let mut cmd = StatusResponse::default();
        cmd.recv_data(conn).await?;
        return Ok(CoordinatorTcpCmd::StatusResponseCmd(cmd));
    } else if cmd_type == WRITE_VNODE_POINT_COMMAND {
        let mut cmd = WriteVnodeRequest::default();
        cmd.recv_data(conn).await?;
        return Ok(CoordinatorTcpCmd::WriteVnodePointCmd(cmd));
    } else if cmd_type == QUERY_RECORD_BATCH_COMMAND {
        let cmd = QueryRecordBatchRequest::recv_data(conn).await?;
        return Ok(CoordinatorTcpCmd::QueryRecordBatchCmd(cmd));
    } else if cmd_type == RECORD_BATCH_RESPONSE_COMMAND {
        let cmd = RecordBatchResponse::recv_data(conn).await?;
        return Ok(CoordinatorTcpCmd::RecordBatchResponseCmd(cmd));
    } else {
        return Err(UnKnownCoordCmd { cmd: cmd_type });
    }
}

#[derive(Serialize, Deserialize, Default, Debug, Clone)]
pub struct StatusResponse {
    pub code: i32,
    pub data: String,
}

impl StatusResponse {
    pub async fn send_cmd(&self, conn: &mut TcpStream) -> CoordinatorResult<()> {
        conn.write(&STATUS_RESPONSE_COMMAND.to_be_bytes()).await?;

        conn.write(&self.code.to_be_bytes()).await?;
        conn.write(&(self.data.len() as u32).to_be_bytes()).await?;
        conn.write_all(self.data.as_bytes()).await?;

        Ok(())
    }

    pub async fn recv_data(&mut self, conn: &mut TcpStream) -> CoordinatorResult<()> {
        let mut tmp_buf: [u8; 4] = [0; 4];

        conn.read_exact(&mut tmp_buf).await?;
        self.code = i32::from_be_bytes(tmp_buf);

        conn.read_exact(&mut tmp_buf).await?;
        let len = u32::from_be_bytes(tmp_buf);

        let mut data_buf = vec![0; len as usize];
        conn.read_exact(&mut data_buf).await?;

        self.data = String::from_utf8(data_buf).map_err(|_| CoordCommandParseErr)?;

        Ok(())
    }
}

#[derive(Serialize, Deserialize, Default, Debug, Clone)]
pub struct WriteVnodeRequest {
    pub vnode_id: u32,
    pub data: Vec<u8>,
}

impl WriteVnodeRequest {
    pub async fn send_cmd(&self, conn: &mut TcpStream) -> CoordinatorResult<()> {
        conn.write(&WRITE_VNODE_POINT_COMMAND.to_be_bytes()).await?;

        conn.write(&self.vnode_id.to_be_bytes()).await?;
        conn.write(&(self.data.len() as u32).to_be_bytes()).await?;
        conn.write_all(&self.data).await?;

        Ok(())
    }

    pub async fn recv_data(&mut self, conn: &mut TcpStream) -> CoordinatorResult<()> {
        let mut tmp_buf: [u8; 4] = [0; 4];

        conn.read_exact(&mut tmp_buf).await?;
        self.vnode_id = u32::from_be_bytes(tmp_buf);

        conn.read_exact(&mut tmp_buf).await?;
        let len = u32::from_be_bytes(tmp_buf);

        self.data.resize(len as usize, 0);
        conn.read_exact(&mut self.data).await?;

        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct QueryRecordBatchRequest {
    pub expr: QueryExpr,
}

impl QueryRecordBatchRequest {
    pub async fn send_cmd(&self, conn: &mut TcpStream) -> CoordinatorResult<()> {
        conn.write(&QUERY_RECORD_BATCH_COMMAND.to_be_bytes())
            .await?;

        let data = QueryExpr::encode(&self.expr)?;
        conn.write(&(data.len() as u32).to_be_bytes()).await?;
        conn.write_all(&data).await?;

        Ok(())
    }

    pub async fn recv_data(conn: &mut TcpStream) -> CoordinatorResult<QueryRecordBatchRequest> {
        let mut tmp_buf: [u8; 4] = [0; 4];

        conn.read_exact(&mut tmp_buf).await?;
        let len = u32::from_be_bytes(tmp_buf);

        let mut data = Vec::with_capacity(len as usize);
        conn.read_exact(&mut data).await?;
        let expr = QueryExpr::decode(data)?;

        Ok(QueryRecordBatchRequest { expr })
    }
}

#[derive(Debug, Clone)]
pub struct RecordBatchResponse {
    pub record: RecordBatch,
}

impl RecordBatchResponse {
    pub async fn send_cmd(&self, conn: &mut TcpStream) -> CoordinatorResult<()> {
        conn.write(&RECORD_BATCH_RESPONSE_COMMAND.to_be_bytes())
            .await?;

        let buffer: Vec<u8> = Vec::new();
        let mut stream_writer = StreamWriter::try_new(buffer, &self.record.schema())?;
        stream_writer.write(&self.record)?;
        stream_writer.finish()?;
        let data = stream_writer.into_inner()?;

        conn.write(&(data.len() as u32).to_be_bytes()).await?;
        conn.write_all(&data).await?;

        Ok(())
    }

    pub async fn recv_data(conn: &mut TcpStream) -> CoordinatorResult<RecordBatchResponse> {
        let mut tmp_buf: [u8; 4] = [0; 4];

        conn.read_exact(&mut tmp_buf).await?;
        let len = u32::from_be_bytes(tmp_buf);
        let mut data_buf = vec![0; len as usize];
        conn.read_exact(&mut data_buf).await?;

        let mut stream_reader = StreamReader::try_new(std::io::Cursor::new(data_buf), None)?;
        let record = stream_reader.next().ok_or(CoordinatorError::CommonError {
            msg: "record batch is None".to_string(),
        })??;

        Ok(RecordBatchResponse { record })
    }
}

/* ************************** internal service command ********************************* */

#[derive(Debug)]
pub enum CoordinatorIntCmd {
    WritePointsCmd(WritePointsRequest),
    SelectStatementCmd(SelectStatementRequest),
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
