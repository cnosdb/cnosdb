#![allow(dead_code)]
#![allow(unused)]
use std::fmt::Display;
use std::io::Cursor;
use std::sync::Arc;

use node_store::NodeStorage;
use openraft::storage::Adaptor;
use openraft::{Config, TokioRuntime};
use raft_network::Network;
use serde::{Deserialize, Serialize};

pub mod apply_store;
pub mod entry_store;
pub mod errors;

pub mod node_store;
pub mod raft_network;
pub mod raft_node;
pub mod state_store;

pub type RaftNodeId = u64;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq, Default)]
pub struct RaftNodeInfo {
    pub group_id: u32,   // raft group id
    pub address: String, // server address
}

// pub struct TypeConfig {}
// impl ::openraft::RaftTypeConfig for TypeConfig {
//     type D = Request;
//     type R = Response;
//     type NodeId = NodeId;
//     type Node = BasicNode;
//     type Entry = openraft::Entry<TypeConfig>;
//     type SnapshotData = Cursor<Vec<u8>>;
//     type AsyncRuntime = TokioRuntime;
// }
openraft::declare_raft_types!(
    /// Declare the type configuration for example K/V store.
    pub TypeConfig: D = Request, R = Response, NodeId = RaftNodeId, Node = RaftNodeInfo,
    Entry = openraft::Entry<TypeConfig>, SnapshotData = Cursor<Vec<u8>>, AsyncRuntime = TokioRuntime
);

type LocalLogStore = Adaptor<TypeConfig, Arc<NodeStorage>>;
type LocalStateMachineStore = Adaptor<TypeConfig, Arc<NodeStorage>>;
pub type OpenRaftNode = openraft::Raft<TypeConfig, Network, LocalLogStore, LocalStateMachineStore>;

//-----------------------------------------------------------------//
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum Request {
    Set { key: String, value: String },
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Response {
    pub value: Option<String>,
}
