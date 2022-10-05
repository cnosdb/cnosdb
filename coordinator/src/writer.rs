use flatbuffers::FlatBufferBuilder;
use models::meta_data::VnodeInfo;
use protos::kv_service::{WritePointsRpcRequest, WritePointsRpcResponse};
use protos::models::{FieldBuilder, Point, PointArgs, Points, PointsArgs, TagBuilder};

use std::collections::HashMap;

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
    pub points: HashMap<u64, Points<'a>>,
    pub vnodes: HashMap<u64, VnodeInfo>,
}

pub struct PointWriter {}

impl PointWriter {
    pub fn map_vnodes(request: WritePointsRequest) -> VnodeMapping<'static> {
        todo!()
    }

    pub fn write_points(request: WritePointsRequest) {}
}
