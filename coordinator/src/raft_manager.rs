use std::collections::BTreeSet;
use std::sync::Arc;

use meta::model::MetaRef;
use models::meta_data::*;
use models::schema::Precision;
use protos::kv_service::WritePointsRequest;
use replication::apply_store::{ApplyStorageRef, HeedApplyStorage};
use replication::entry_store::{EntryStorageRef, HeedEntryStorage};
use replication::multi_raft::MultiRaft;
use replication::node_store::NodeStorage;
use replication::raft_node::RaftNode;
use replication::state_store::StateStorage;
use replication::{RaftNodeId, RaftNodeInfo};
use tokio::sync::RwLock;

use crate::errors::*;

pub struct RaftWriteRequest {
    pub points: WritePointsRequest,
    pub precision: Precision,
}

pub struct RaftNodesManager {
    node_id: u64,
    meta: MetaRef,
    grpc_addr: String,
    raft_state: Arc<StateStorage>,
    raft_nodes: Arc<RwLock<MultiRaft>>,
}

impl RaftNodesManager {
    pub fn new(node_id: u64, grpc_addr: String, meta: MetaRef) -> Self {
        let state = StateStorage::open(format!("/tmp/cnosdb/{}-state", node_id)).unwrap();

        Self {
            meta,
            node_id,
            grpc_addr,
            raft_state: Arc::new(state),
            raft_nodes: Arc::new(RwLock::new(MultiRaft::new())),
        }
    }

    pub async fn raft_node_by_id(&self, id: ReplicationSetId) -> Option<Arc<RaftNode>> {
        self.raft_nodes.read().await.get_node(id).map(|v| v.clone())
    }

    pub async fn build_replica_group(&self, replica: &ReplicationSet) -> CoordinatorResult<()> {
        if replica.leader_node_id() != self.node_id {
            return Err(CoordinatorError::LeaderIsWrong {
                msg: format!(
                    "build replica group node_id: {}, replica:{:?}",
                    self.node_id, replica
                ),
            });
        }

        let mut nodes = self.raft_nodes.write().await;

        if nodes.exist(replica.id) {
            return Ok(());
        }

        let is_init = self.raft_state.is_already_init(replica.id)?;
        let leader_vid = replica.leader_vnode_id();
        let raft_node = self.open_raft_node(leader_vid, replica.id).await?;
        let raft_node = Arc::new(raft_node);
        if is_init {
            nodes.add_node(raft_node.clone());
            return Ok(());
        }

        raft_node.raft_init().await?;

        let mut followers = BTreeSet::new();
        for vnode in &replica.vnodes {
            let raft_id = vnode.id as RaftNodeId;
            followers.insert(raft_id);
            if vnode.id == leader_vid {
                continue;
            }

            let info = RaftNodeInfo {
                group_id: replica.id,
                address: self.meta.node_info_by_id(vnode.node_id).await?.grpc_addr,
            };

            raft_node.raft_add_learner(raft_id, info).await?;
        }

        if followers.len() > 1 {
            raft_node.raft_change_membership(followers).await?;
        }

        nodes.add_node(raft_node.clone());

        Ok(())
    }

    pub async fn open_raft_node(
        &self,
        id: VnodeId,
        group_id: ReplicationSetId,
    ) -> CoordinatorResult<RaftNode> {
        let id = id as u64;
        let path = format!("/tmp/cnosdb/{}/{}", self.node_id, id);

        let entry = HeedEntryStorage::open(format!("{}-entry", path))?;
        let engine = HeedApplyStorage::open(format!("{}-engine", path))?;

        let entry: EntryStorageRef = Arc::new(entry);
        let engine: ApplyStorageRef = Arc::new(engine);

        let info = RaftNodeInfo {
            group_id,
            address: self.grpc_addr.clone(),
        };

        let storage = NodeStorage::open(
            id,
            info.clone(),
            self.raft_state.clone(),
            engine.clone(),
            entry,
        )?;
        let storage = Arc::new(storage);

        let config = openraft::Config {
            heartbeat_interval: 500,
            election_timeout_min: 1500,
            election_timeout_max: 3000,
            ..Default::default()
        };

        let node = RaftNode::new(id, info, config, storage, engine)
            .await
            .unwrap();

        Ok(node)
    }
}
