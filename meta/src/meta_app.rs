use std::sync::Arc;

use openraft::Config;

use crate::NodeId;
use crate::ExampleRaft;
use crate::Store;

pub struct MetaApp {
    pub id: NodeId,
    pub addr: String,
    pub raft: ExampleRaft,
    pub store: Arc<Store>,
    pub config: Arc<Config>,
}
