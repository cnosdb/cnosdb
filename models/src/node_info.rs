use crate::Timestamp;

pub struct Vnode {
    ///  same with time series family name
    name: String,
    node_id: String,
    ip: String,
}

pub struct Node {
    node_id: String,
    ip: String,
    flavor: Flavor,
    status: NodeStatus,
    located: Location,
    last_updated: Timestamp,
}

pub enum NodeStatus {
    Healthy,
    Broken,
    Unreachable,
}

pub struct Location {
    ///  aws / huawei / google / local
    provider: String,
    region: String,
    az: String,
}

pub struct Flavor {
    memory: f64, // M
    cpu: f64,    // core
    disk: f64,   // G
}
