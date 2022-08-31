use crate::Timestamp;

#[allow(dead_code)]
pub struct Vnode {
    ///  same with time series family name
    name: String,
    node_id: String,
    ip: String,
}

#[allow(dead_code)]
pub struct Node {
    node_id: String,
    ip: String,
    flavor: Flavor,
    status: NodeStatus,
    located: Location,
    last_updated: Timestamp,
}

#[allow(dead_code)]
pub enum NodeStatus {
    Healthy,
    Broken,
    Unreachable,
}

#[allow(dead_code)]
pub struct Location {
    ///  aws / huawei / google / local
    provider: String,
    region: String,
    az: String,
}

#[allow(dead_code)]
pub struct Flavor {
    memory: f64, // M
    cpu: f64,    // core
    disk: f64,   // G
}
