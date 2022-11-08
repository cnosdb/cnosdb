use serde::{Deserialize, Serialize};
use std::{
    io::{BufReader, BufWriter, Read, Write},
    net::{TcpListener, TcpStream},
};

use crate::errors::{CoordinatorError::*, CoordinatorResult};

pub const COMMON_RESPONSE_COMMAND: u32 = 1;
pub const WRITE_VNODE_POINT_COMMAND: u32 = 2;
pub const EXECUTE_STATEMENT_REQUEST_COMMAND: u32 = 3;

pub const SUCCESS_RESPONSE_CODE: i32 = 0;

pub enum CoordinatorCmd {
    CommonResponseCmd(CommonResponse),
    WriteVnodePointCmd(WriteVnodeRequest),
}

pub fn send_command(conn: &mut TcpStream, cmd: &CoordinatorCmd) -> CoordinatorResult<()> {
    match cmd {
        CoordinatorCmd::CommonResponseCmd(val) => val.send_cmd(conn),
        CoordinatorCmd::WriteVnodePointCmd(val) => val.send_cmd(conn),
    }
}

pub fn recv_command(conn: &mut TcpStream) -> CoordinatorResult<CoordinatorCmd> {
    let mut tmp_buf: [u8; 4] = [0; 4];
    conn.read_exact(&mut tmp_buf)?;

    let cmd_type = u32::from_be_bytes(tmp_buf);
    if cmd_type == COMMON_RESPONSE_COMMAND {
        let mut cmd = CommonResponse::default();
        cmd.recv_data(conn)?;
        return Ok(CoordinatorCmd::CommonResponseCmd(cmd));
    } else if cmd_type == WRITE_VNODE_POINT_COMMAND {
        let mut cmd = WriteVnodeRequest::default();
        cmd.recv_data(conn)?;
        return Ok(CoordinatorCmd::WriteVnodePointCmd(cmd));
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
    pub fn send_cmd(&self, conn: &mut TcpStream) -> CoordinatorResult<()> {
        conn.write(&COMMON_RESPONSE_COMMAND.to_be_bytes())?;

        conn.write(&self.code.to_be_bytes())?;
        conn.write(&(self.data.len() as u32).to_be_bytes())?;
        conn.write_all(self.data.as_bytes())?;

        Ok(())
    }

    pub fn recv_data(&mut self, conn: &mut TcpStream) -> CoordinatorResult<()> {
        let mut tmp_buf: [u8; 4] = [0; 4];

        conn.read_exact(&mut tmp_buf)?;
        self.code = i32::from_be_bytes(tmp_buf);

        conn.read_exact(&mut tmp_buf)?;
        let len = u32::from_be_bytes(tmp_buf);

        let mut data_buf = vec![0; len as usize];
        conn.read_exact(&mut data_buf)?;

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
    pub fn send_cmd(&self, conn: &mut TcpStream) -> CoordinatorResult<()> {
        conn.write(&WRITE_VNODE_POINT_COMMAND.to_be_bytes())?;

        conn.write(&self.vnode_id.to_be_bytes())?;
        conn.write(&(self.data.len() as u32).to_be_bytes())?;
        conn.write_all(&self.data)?;

        Ok(())
    }

    pub fn recv_data(&mut self, conn: &mut TcpStream) -> CoordinatorResult<()> {
        let mut tmp_buf: [u8; 4] = [0; 4];

        conn.read_exact(&mut tmp_buf)?;
        self.vnode_id = u32::from_be_bytes(tmp_buf);

        conn.read_exact(&mut tmp_buf)?;
        let len = u32::from_be_bytes(tmp_buf);

        self.data.resize(len as usize, 0);
        conn.read_exact(&mut self.data)?;

        Ok(())
    }
}
