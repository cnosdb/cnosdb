#![allow(dead_code)]
#![allow(unused)]
use std::fmt::Debug;
use std::io::Cursor;
use std::sync::Arc;

use network_client::NetworkConn;
use node_store::NodeStorage;
use openraft::storage::Adaptor;
use openraft::TokioRuntime;

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
    pub TypeConfig: D = Request, R = Response, NodeId = RaftNodeId, Node = RaftNodeInfo,
    Entry = openraft::Entry<TypeConfig>, SnapshotData = Cursor<Vec<u8>>, AsyncRuntime = TokioRuntime
);

type LocalLogStore = Adaptor<TypeConfig, Arc<NodeStorage>>;
type LocalStateMachineStore = Adaptor<TypeConfig, Arc<NodeStorage>>;
pub type OpenRaftNode =
    openraft::Raft<TypeConfig, NetworkConn, LocalLogStore, LocalStateMachineStore>;

pub type Request = Vec<u8>;
pub type Response = Vec<u8>;
//-----------------------------------------------------------------//

// #[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
// pub enum Request {
//     Set { key: String, value: String },
// }

// #[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
// pub struct Response {
//     pub value: Option<String>,
// }
