use serde::{Deserialize, Serialize};
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};

use crate::errors::*;

pub const COMMON_RESPONSE_COMMAND: u32 = 1;
pub const WRITE_VNODE_REQUEST_COMMAND: u32 = 1;
pub const EXECUTE_STATEMENT_REQUEST_COMMAND: u32 = 2;

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
        conn.write_u32(COMMON_RESPONSE_COMMAND).await?;
        conn.write_u32(cmd_data.len().try_into().unwrap()).await?;

        conn.write_all(&cmd_data).await?;

        Ok(())
    }

    pub async fn recv(conn: &mut TcpStream) -> CoordinatorResult<CommonResponse> {
        let cmd_type = (*conn).read_u32().await?;
        let data_len = (*conn).read_u32().await?;

        let mut data_buf = vec![0; data_len as usize];
        (*conn).read_exact(&mut data_buf).await?;

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
    pub db_name: String,
    pub data_len: u32,
}

impl WriteVnodeRequest {
    pub fn new(vnode_id: u32, db_name: String, data_len: u32) -> Self {
        Self {
            vnode_id,
            db_name,
            data_len,
        }
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
