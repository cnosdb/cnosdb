use async_channel as channel;
use flatbuffers::FlatBufferBuilder;
use models::meta_data::*;
use models::utils::now_timestamp;
use models::RwLockRef;
use parking_lot::{RwLock, RwLockReadGuard};
use protos::kv_service::{WritePointsRpcRequest, WritePointsRpcResponse};
use protos::models::{FieldBuilder, Point, PointArgs, Points, PointsArgs, TagBuilder};
use snafu::ResultExt;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::io::{BufWriter, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::Arc;
use tokio::sync::mpsc::Sender;
use tokio::sync::oneshot;
use tskv::engine::EngineRef;

use protos::models as fb_models;

use crate::command::*;
use crate::errors::*;
use crate::hh_queue::HintedOffManager;
use crate::hh_queue::{HintedOffBlock, HintedOffWriteReq};
use crate::meta_client::{MetaClientRef, MetaRef};
use trace::debug;
use trace::info;

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

    pub fn add_point(&mut self, point: models::Point) {
        let mut tags = Vec::with_capacity(point.tags.len());
        for item in point.tags.iter() {
            let fbk = self.fbb.create_vector(&item.key);
            let fbv = self.fbb.create_vector(&item.value);
            let mut tag_builder = TagBuilder::new(&mut self.fbb);
            tag_builder.add_key(fbk);
            tag_builder.add_value(fbv);
            tags.push(tag_builder.finish());
        }

        let mut fields = Vec::with_capacity(point.fields.len());
        for item in point.fields.iter() {
            let fbk = self.fbb.create_vector(&item.name);
            let fbv = self.fbb.create_vector(&item.value);

            //fb_models::FieldType::Boolean,

            let vtype = item.value_type.to_fb_type();
            let mut field_builder = FieldBuilder::new(&mut self.fbb);
            field_builder.add_name(fbk);
            field_builder.add_type_(vtype);
            field_builder.add_value(fbv);
            fields.push(field_builder.finish());
        }

        let point_args = PointArgs {
            db: Some(self.fbb.create_vector(point.db.as_bytes())),
            table: Some(self.fbb.create_vector(point.table.as_bytes())),
            tags: Some(self.fbb.create_vector(&tags)),
            fields: Some(self.fbb.create_vector(&fields)),
            timestamp: point.timestamp,
        };

        self.offset.push(Point::create(&mut self.fbb, &point_args));
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
    pub points: HashMap<u32, VnodePoints<'a>>,
    pub sets: HashMap<u32, ReplcationSet>,
}

impl<'a> VnodeMapping<'a> {
    pub fn new() -> Self {
        Self {
            points: HashMap::new(),
            sets: HashMap::new(),
        }
    }

    pub fn map_point(&mut self, meta_client: MetaClientRef, point: models::Point) {
        if let Ok(info) =
            meta_client.locate_replcation_set_for_write(&point.db, point.hash_id, point.timestamp)
        {
            //let full_name = format!("{}.{}", meta_client.tenant_name(), db);
            self.sets.entry(info.id).or_insert(info.clone());
            let entry = self
                .points
                .entry(info.id)
                .or_insert(VnodePoints::new(point.db.clone(), info));

            entry.add_point(point);
        }
    }
}

pub struct PointWriter {
    node_id: u64,
    kv_inst: EngineRef,
    meta_manager: MetaRef,
    hh_sender: Sender<HintedOffWriteReq>,

    conn_map: RwLock<HashMap<u64, VecDeque<TcpStream>>>,
}

impl PointWriter {
    pub fn new(
        node_id: u64,
        kv_inst: EngineRef,
        meta_manager: MetaRef,
        hh_sender: Sender<HintedOffWriteReq>,
    ) -> Self {
        Self {
            node_id,
            kv_inst,
            meta_manager,
            hh_sender,
            conn_map: RwLock::new(HashMap::new()),
        }
    }

    pub async fn write_points(
        &self,
        mapping: &mut VnodeMapping<'_>,
        hh_manager: Arc<HintedOffManager>,
    ) -> CoordinatorResult<()> {
        let mut requests = vec![];
        for (id, points) in mapping.points.iter_mut() {
            points.finish();

            for vnode in points.repl_set.vnodes.iter() {
                let request = self.warp_write_to_node(
                    vnode.id,
                    vnode.node_id,
                    points.data.clone(),
                    hh_manager.clone(),
                );
                requests.push(request);
            }
        }

        futures::future::try_join_all(requests).await?;

        Ok(())
    }

    async fn warp_write_to_node(
        &self,
        vnode_id: u32,
        node_id: u64,
        data: Vec<u8>,
        hh_manager: Arc<HintedOffManager>,
    ) -> CoordinatorResult<()> {
        match self.write_to_node(vnode_id, node_id, data.clone()).await {
            Ok(_) => {
                debug!(
                    "write data to node: {}[vnode: {}] success!",
                    node_id, vnode_id
                );
                return Ok(());
            }
            Err(err) => {
                debug!(
                    "write data to node: {} [vnode: {}] failed; {}!",
                    node_id,
                    vnode_id,
                    err.to_string()
                );

                let (sender, receiver) = oneshot::channel();
                let request = HintedOffWriteReq {
                    node_id,
                    sender,
                    block: HintedOffBlock::new(now_timestamp(), vnode_id, data),
                };

                self.hh_sender.send(request).await.map_err(|err| {
                    CoordinatorError::ChannelSend {
                        msg: err.to_string(),
                    }
                })?;

                let result = receiver
                    .await
                    .map_err(|err| CoordinatorError::ChannelSend {
                        msg: err.to_string(),
                    })?;

                return result;
            }
        }
    }

    pub async fn write_to_node(
        &self,
        vnode_id: u32,
        node_id: u64,
        data: Vec<u8>,
    ) -> CoordinatorResult<()> {
        if node_id == self.node_id {
            let req = WritePointsRpcRequest {
                version: 1,
                points: data.clone(),
            };

            if let Err(err) = self.kv_inst.write(vnode_id, req).await {
                return Err(err.into());
            } else {
                return Ok(());
            }
        }

        let mut conn = self.get_node_conn(node_id)?;
        let req_cmd = WriteVnodeRequest { vnode_id, data };
        send_command(&mut conn, &CoordinatorCmd::WriteVnodePointCmd(req_cmd))?;
        let rsp_cmd = recv_command(&mut conn)?;
        if let CoordinatorCmd::CommonResponseCmd(msg) = rsp_cmd {
            self.put_node_conn(node_id, conn);
            if msg.code == 0 {
                return Ok(());
            } else {
                return Err(CoordinatorError::WriteVnode {
                    msg: format!("code: {}, msg: {}", msg.code, msg.data),
                });
            }
        } else {
            return Err(CoordinatorError::UnExpectResponse);
        }
    }

    fn get_node_conn(&self, node_id: u64) -> CoordinatorResult<TcpStream> {
        {
            let mut write = self.conn_map.write();
            let entry = write.entry(node_id).or_insert(VecDeque::with_capacity(32));
            if let Some(val) = entry.pop_front() {
                return Ok(val);
            }
        }

        let info = self.meta_manager.admin_meta().node_info_by_id(node_id)?;
        let client = TcpStream::connect(info.tcp_addr)?;

        return Ok(client);
    }

    fn put_node_conn(&self, node_id: u64, conn: TcpStream) {
        let mut write = self.conn_map.write();
        let entry = write.entry(node_id).or_insert(VecDeque::with_capacity(32));

        // close too more idle connection
        if entry.len() < 32 {
            entry.push_back(conn);
        }
    }
}
