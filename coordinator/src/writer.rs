use async_channel as channel;
use flatbuffers::FlatBufferBuilder;
use models::meta_data::*;
use models::RwLockRef;
use parking_lot::{RwLock, RwLockReadGuard};
use protos::kv_service::{WritePointsRpcRequest, WritePointsRpcResponse};
use protos::models::{FieldBuilder, Point, PointArgs, Points, PointsArgs, TagBuilder};
use snafu::ResultExt;
use std::collections::HashMap;
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};
use tskv::engine::EngineRef;

use crate::command::*;
use crate::errors::*;
use crate::meta_client::MetaClientRef;

pub struct WritePointsRequest {
    db: String,
    level: models::consistency_level::ConsistencyLevel,
    request: WritePointsRpcRequest,
}

pub struct VnodePoints<'a> {
    db: String,
    fbb: FlatBufferBuilder<'a>,
    offset: Vec<flatbuffers::WIPOffset<Point<'a>>>,

    pub data: Vec<u8>,
    pub repl_set: ReplcationSet,
}

impl VnodePoints<'_> {
    pub fn new(db: String, repl_set: ReplcationSet) -> Self {
        Self {
            db,
            repl_set,
            fbb: FlatBufferBuilder::new(),
            offset: Vec::new(),
            data: vec![],
        }
    }

    pub fn add_point(&mut self, args: &PointArgs) {
        self.offset.push(Point::create(&mut self.fbb, args));
    }

    pub fn finish(&mut self) {
        let fbb_db = self.fbb.create_vector(self.db.as_bytes());
        let points_raw = self.fbb.create_vector(&self.offset);

        let points = Points::create(
            &mut self.fbb,
            &PointsArgs {
                database: Some(fbb_db),
                points: Some(points_raw),
            },
        );
        self.fbb.finish(points, None);
        self.data = self.fbb.finished_data().to_vec();
    }
}

pub struct VnodeMapping<'a> {
    pub points: HashMap<u64, VnodePoints<'a>>,
    pub sets: HashMap<u64, ReplcationSet>,
}

impl<'a> VnodeMapping<'a> {
    pub fn new() -> Self {
        Self {
            points: HashMap::new(),
            sets: HashMap::new(),
        }
    }

    pub fn map_point(&mut self, meta_client: MetaClientRef, db: &String, point: &PointArgs) {
        if let Ok(info) = meta_client.locate_db_ts_for_write(db, point.timestamp) {
            let full_name = format!("{}.{}", meta_client.tenant_name(), db);
            self.sets.insert(info.id, info.clone());
            let entry = self
                .points
                .entry(info.id)
                .or_insert(VnodePoints::new(full_name, info));

            entry.add_point(point);
        }
    }
}

pub struct PointWriter {
    self_id: u64,
    kv_inst: EngineRef,
    conn_map: RwLock<HashMap<u64, Vec<TcpStream>>>,

    pub meta_client: MetaClientRef,
}

impl PointWriter {
    pub fn new(node_id: u64, kv_inst: EngineRef, client: MetaClientRef) -> Self {
        Self {
            self_id: node_id,
            kv_inst,
            meta_client: client,
            conn_map: RwLock::new(HashMap::new()),
        }
    }

    pub async fn write_points(&self, mapping: &mut VnodeMapping<'_>) -> CoordinatorResult<()> {
        let mut requests = vec![];
        for (id, points) in mapping.points.iter_mut() {
            points.finish();

            for vnode in points.repl_set.vnodes.iter() {
                let request = self.write_to_node(vnode.id, vnode.node_id, points);
                requests.push(request);
            }
        }

        futures::future::try_join_all(requests).await?;

        Ok(())
    }

    async fn write_to_node(
        &self,
        vnode_id: u32,
        node_id: u64,
        points: &VnodePoints<'_>,
    ) -> CoordinatorResult<()> {
        if node_id == self.self_id {
            let req = WritePointsRpcRequest {
                version: 1,
                points: points.data.clone(),
            };

            if let Err(err) = self.kv_inst.write(req).await {
                return Err(CoordinatorError::TskvWrite { source: err });
            } else {
                return Ok(());
            }
        }

        let mut conn = self.get_node_conn(node_id).await?;

        let req_cmd = WriteVnodeRequest::new(vnode_id, points.db.clone(), points.data.len() as u32);
        let cmd_data = req_cmd.encode();
        conn.write_u32(WRITE_VNODE_REQUEST_COMMAND).await?;
        conn.write_u32(cmd_data.len().try_into().unwrap()).await?;

        conn.write_all(&cmd_data).await?;

        conn.write_all(&points.data).await?;

        match CommonResponse::recv(&mut conn).await {
            Ok(msg) => {
                self.put_node_conn(node_id, conn);
                if msg.code == 0 {
                    return Ok(());
                } else {
                    return Err(CoordinatorError::WriteVnode {
                        msg: format!("code: {}, msg: {}", msg.code, msg.data),
                    });
                }
            }
            Err(err) => Err(err),
        }
    }

    async fn get_node_conn(&self, node_id: u64) -> CoordinatorResult<TcpStream> {
        {
            let mut write = self.conn_map.write();
            let entry = write.entry(node_id).or_insert(Vec::with_capacity(32));
            if let Some(val) = entry.pop() {
                return Ok(val);
            }
        }

        let info = self.meta_client.node_info_by_id(node_id)?;
        let client = TcpStream::connect(info.tcp_addr).await?;

        return Ok(client);
    }

    fn put_node_conn(&self, node_id: u64, conn: TcpStream) {
        let mut write = self.conn_map.write();
        let entry = write.entry(node_id).or_insert(Vec::with_capacity(32));
        entry.push(conn);
    }
}
