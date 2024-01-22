#![allow(dead_code)]
#![allow(unused)]
use std::any::Any;
use std::fmt::Debug;
use std::io::Cursor;
use std::sync::Arc;

use async_trait::async_trait;
use errors::ReplicationResult;
use network_client::NetworkConn;
use node_store::NodeStorage;
use openraft::storage::Adaptor;
use openraft::{Entry, TokioRuntime};
use tokio::sync::RwLock;

pub mod apply_store;
pub mod entry_store;
pub mod errors;

pub mod multi_raft;
pub mod network_client;
pub mod network_grpc;
pub mod network_http;
pub mod node_store;
pub mod raft_node;
pub mod state_store;

pub type RaftNodeId = u64;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq, Default)]
pub struct RaftNodeInfo {
    pub group_id: u32,   // raft group id
    pub address: String, // server address
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq, Default)]
pub struct ReplicationConfig {
    pub cluster_name: String,
    pub lmdb_max_map_size: usize,
    pub grpc_enable_gzip: bool,
    pub heartbeat_interval: u64,
    pub raft_logs_to_keep: u64,
    pub send_append_entries_timeout: u64, //ms
    pub install_snapshot_timeout: u64,    //ms
}

// #[derive(Debug, Clone, Copy, Default, Eq, PartialEq, Ord, PartialOrd)]
// #[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
// pub struct TypeConfig {}
// impl openraft::RaftTypeConfig for TypeConfig {
//     type D = Request;
//     type R = Response;
//     type NodeId = RaftNodeId;
//     type Node = RaftNodeInfo;
//     type Entry = openraft::Entry<TypeConfig>;
//     type SnapshotData = Cursor<Vec<u8>>;
//     type AsyncRuntime = TokioRuntime;
// }
openraft::declare_raft_types!(
    /// Declare the type configuration.
    pub TypeConfig:
        D = Request,
        R = Response,
        NodeId = RaftNodeId,
        Node = RaftNodeInfo,
        Entry = openraft::Entry<TypeConfig>,
        SnapshotData = Cursor<Vec<u8>>,
        AsyncRuntime = TokioRuntime
);

type LocalLogStore = Adaptor<TypeConfig, Arc<NodeStorage>>;
type LocalStateMachineStore = Adaptor<TypeConfig, Arc<NodeStorage>>;
pub type OpenRaftNode =
    openraft::Raft<TypeConfig, NetworkConn, LocalLogStore, LocalStateMachineStore>;

pub type Request = Vec<u8>;
pub type Response = Vec<u8>;
//-----------------------------------------------------------------//

pub const APPLY_TYPE_WAL: u32 = 1;
pub const APPLY_TYPE_WRITE: u32 = 2;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq, Default)]
pub struct ApplyContext {
    pub index: u64,
    pub apply_type: u32,
    pub raft_id: RaftNodeId,
}

#[async_trait]
pub trait ApplyStorage: Send + Sync + Any {
    async fn apply(&mut self, ctx: &ApplyContext, req: &Request) -> ReplicationResult<Response>;
    async fn snapshot(&mut self) -> ReplicationResult<Vec<u8>>;
    async fn restore(&mut self, snapshot: &[u8]) -> ReplicationResult<()>;
    async fn destory(&mut self) -> ReplicationResult<()>;
}
pub type ApplyStorageRef = Arc<RwLock<dyn ApplyStorage + Send + Sync>>;

#[async_trait]
pub trait EntryStorage: Send + Sync {
    // Get the entry by index
    async fn entry(&mut self, index: u64) -> ReplicationResult<Option<Entry<TypeConfig>>>;

    // Delete entries: from begin to index
    async fn del_before(&mut self, index: u64) -> ReplicationResult<()>; // [0, index)

    // Delete entries: from index to end
    async fn del_after(&mut self, index: u64) -> ReplicationResult<()>; // [index, ...)

    // Write entries
    async fn append(&mut self, ents: &[Entry<TypeConfig>]) -> ReplicationResult<()>;

    // Get the last entry
    async fn last_entry(&mut self) -> ReplicationResult<Option<Entry<TypeConfig>>>;

    // Get entries from begin to end
    async fn entries(&mut self, begin: u64, end: u64) -> ReplicationResult<Vec<Entry<TypeConfig>>>; // [begin, end)
}
pub type EntryStorageRef = Arc<RwLock<dyn EntryStorage>>;
