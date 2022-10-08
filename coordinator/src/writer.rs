use flatbuffers::FlatBufferBuilder;
use models::meta_data::VnodeInfo;
use protos::kv_service::{WritePointsRpcRequest, WritePointsRpcResponse};
use protos::models::{FieldBuilder, Point, PointArgs, Points, PointsArgs, TagBuilder};
use std::collections::HashMap;
use tskv::engine::EngineRef;

use crate::meta_client::MetaClientRef;

pub struct WritePointsRequest {
    db: String,
    level: models::consistency_level::ConsistencyLevel,
    request: WritePointsRpcRequest,
}

pub struct VnodePoints<'a> {
    pub db: String,
    pub fbb: FlatBufferBuilder<'a>,
    pub offset: Vec<flatbuffers::WIPOffset<Point<'a>>>,

    pub data: Vec<u8>,
    pub vnode: VnodeInfo,
}

impl VnodePoints<'_> {
    pub fn new(db: String, vnode: VnodeInfo) -> Self {
        Self {
            db,
            vnode,
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
    pub db: String,
    pub points: HashMap<u64, VnodePoints<'a>>,
    pub vnodes: HashMap<u64, VnodeInfo>,
}

impl<'a> VnodeMapping<'a> {
    pub fn new(db: String) -> Self {
        Self {
            db,
            points: HashMap::new(),
            vnodes: HashMap::new(),
        }
    }

    pub fn map_point(&mut self, meta_client: MetaClientRef, db: &String, point: &PointArgs) {
        if let Ok(info) = meta_client.locate_db_ts_for_write(db, point.timestamp) {
            self.vnodes.insert(info.id, info.clone());
            let entry = self
                .points
                .entry(info.id)
                .or_insert(VnodePoints::new(db.clone(), info));

            entry.add_point(point);
        }
    }
}

pub struct PointWriter {
    pub node_id: u64,
    pub kv_inst: EngineRef,
    pub meta_client: MetaClientRef,
}

impl PointWriter {
    pub fn new(node_id: u64, kv_inst: EngineRef, meta_client: MetaClientRef) -> Self {
        Self {
            node_id,
            kv_inst,
            meta_client,
        }
    }

    pub fn write_points(&self, mapping: &mut VnodeMapping) {
        for (id, points) in mapping.points.iter_mut() {
            points.finish();
            self.write_to_vnode(points)
        }
    }

    pub fn write_to_vnode(&self, points: &VnodePoints) {
        for owner in points.vnode.owners.iter() {
            self.write_to_node(points.vnode.id, *owner, points);
        }
    }

    pub fn write_to_node(&self, vnode_id: u64, node_id: u64, points: &VnodePoints) {
        let info = self.meta_client.node_info_by_id(node_id).unwrap();
    }
}
