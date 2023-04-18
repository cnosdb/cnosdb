use serde::{Deserialize, Serialize};

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

#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]

pub enum NodeStatus {
    #[default]
    Healthy,
    Broken,
    Unreachable,
    NoDiskSpace,
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
