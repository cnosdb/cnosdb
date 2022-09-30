use protos::kv_service::{WritePointsRpcRequest, WritePointsRpcResponse};
use protos::models::{Point, Points};

use std::collections::HashMap;

pub struct WritePointsRequest {
    db: String,
    level: models::consistency_level::ConsistencyLevel,
    request: WritePointsRpcRequest,
}

pub struct VnodeMapping<'a> {
    pub points: HashMap<u64, Points<'a>>,
    pub vnodes: HashMap<u64, models::meta_data::VnodeInfo>,
}

pub struct PointWriter {}

impl PointWriter {
    pub fn map_vnodes(request: WritePointsRequest) -> VnodeMapping<'static> {
        todo!()
    }

    pub fn write_points(request: WritePointsRequest) {}
}
