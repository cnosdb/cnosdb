use protos::kv_service::WritePointsRpcRequest;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::oneshot::Sender;

// use std::net::{TcpListener, TcpStream};
use tokio::net::{TcpListener, TcpStream};

use crate::errors::{CoordinatorError::*, CoordinatorResult};

/* ************************* tcp service command ********************************* */
pub const COMMON_RESPONSE_COMMAND: u32 = 1;
pub const WRITE_VNODE_POINT_COMMAND: u32 = 2;
pub const EXECUTE_STATEMENT_REQUEST_COMMAND: u32 = 3;

pub const SUCCESS_RESPONSE_CODE: i32 = 0;

pub enum CoordinatorTcpCmd {
    CommonResponseCmd(CommonResponse),
    WriteVnodePointCmd(WriteVnodeRequest),
}

pub async fn send_command(conn: &mut TcpStream, cmd: &CoordinatorTcpCmd) -> CoordinatorResult<()> {
    match cmd {
        CoordinatorTcpCmd::CommonResponseCmd(val) => val.send_cmd(conn).await,
        CoordinatorTcpCmd::WriteVnodePointCmd(val) => val.send_cmd(conn).await,
    }
}

pub async fn recv_command(conn: &mut TcpStream) -> CoordinatorResult<CoordinatorTcpCmd> {
    let mut tmp_buf: [u8; 4] = [0; 4];
    conn.read_exact(&mut tmp_buf).await?;

    let cmd_type = u32::from_be_bytes(tmp_buf);
    if cmd_type == COMMON_RESPONSE_COMMAND {
        let mut cmd = CommonResponse::default();
        cmd.recv_data(conn).await?;
        return Ok(CoordinatorTcpCmd::CommonResponseCmd(cmd));
    } else if cmd_type == WRITE_VNODE_POINT_COMMAND {
        let mut cmd = WriteVnodeRequest::default();
        cmd.recv_data(conn).await?;
        return Ok(CoordinatorTcpCmd::WriteVnodePointCmd(cmd));
    } else {
        return Err(UnKnownCoordCmd { cmd: cmd_type });
    }
}

#[derive(Serialize, Deserialize, Default, Debug, Clone)]
pub struct CommonResponse {
    pub code: i32,
    pub data: String,
}

impl CommonResponse {
    pub async fn send_cmd(&self, conn: &mut TcpStream) -> CoordinatorResult<()> {
        conn.write(&COMMON_RESPONSE_COMMAND.to_be_bytes()).await?;

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

/* ************************** internal service command ********************************* */

#[derive(Debug)]
pub enum CoordinatorIntCmd {
    WritePointsCmd(WritePointsRequest),
}

#[derive(Debug)]
pub struct WritePointsRequest {
    pub tenant: String,
    pub level: models::consistency_level::ConsistencyLevel,
    pub request: WritePointsRpcRequest,

    pub sender: Sender<CoordinatorResult<()>>,
}
