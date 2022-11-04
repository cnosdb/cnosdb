use serde::{Deserialize, Serialize};
use std::{
    io::{BufReader, BufWriter, Read, Write},
    net::{TcpListener, TcpStream},
};

use crate::errors::*;

pub const COMMON_RESPONSE_COMMAND: u32 = 1;
pub const WRITE_VNODE_POINT_COMMAND: u32 = 1;
pub const EXECUTE_STATEMENT_REQUEST_COMMAND: u32 = 2;

pub enum CoordinatorCmd {
    CommonResponseCmd(CommonResponse),
    WriteVnodePointCmd(WriteVnodeRequest),
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct CommandHeader {
    pub typ: u32,
    pub len: u32,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct CommonResponse {
    pub code: i32,
    pub data: String,
}

impl CommonResponse {
    pub fn new(code: i32, data: String) -> Self {
        Self { code, data }
    }

    pub async fn send(conn: &mut TcpStream, code: i32, data: String) -> CoordinatorResult<()> {
        let cmd = CommonResponse::new(code, data);
        let cmd_data = cmd.encode();

        conn.write(&COMMON_RESPONSE_COMMAND.to_be_bytes())?;
        conn.write(&(cmd_data.len() as u32).to_be_bytes())?;
        conn.write_all(&cmd_data)?;

        Ok(())
    }

    pub async fn recv(conn: &mut TcpStream) -> CoordinatorResult<CommonResponse> {
        let mut tmp_buf: [u8; 4] = [0; 4];

        conn.read_exact(&mut tmp_buf)?;
        let cmd_type = u32::from_be_bytes(tmp_buf);

        conn.read_exact(&mut tmp_buf)?;
        let data_len = u32::from_be_bytes(tmp_buf);

        let mut data_buf = vec![0; data_len as usize];
        conn.read_exact(&mut data_buf)?;

        CommonResponse::decode(&data_buf)
    }

    pub fn encode(&self) -> Vec<u8> {
        bincode::serialize(self).unwrap()
    }

    pub fn decode(data: &[u8]) -> CoordinatorResult<CommonResponse> {
        let key = bincode::deserialize(data);
        match key {
            Ok(key) => Ok(key),
            Err(err) => Err(CoordinatorError::InvalidSerdeMsg {
                err: format!("Invalid CommonResponse serde message: {}", err),
            }),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct WriteVnodeRequest {
    pub vnode_id: u32,
    pub data_len: u32,
}

impl WriteVnodeRequest {
    pub fn new(vnode_id: u32, data_len: u32) -> Self {
        Self { vnode_id, data_len }
    }

    pub fn encode(&self) -> Vec<u8> {
        bincode::serialize(self).unwrap()
    }

    pub fn decode(data: &[u8]) -> CoordinatorResult<WriteVnodeRequest> {
        let key = bincode::deserialize(data);
        match key {
            Ok(key) => Ok(key),
            Err(err) => Err(CoordinatorError::InvalidSerdeMsg {
                err: format!("Invalid WriteVnodeRequest serde message: {}", err),
            }),
        }
    }
}
