use std::default;
use std::fmt::Debug;
use std::io::Cursor;
use std::ops::RangeBounds;
use std::sync::Arc;

use openraft::storage::{LogState, Snapshot};
use openraft::{
    Entry, EntryPayload, LogId, LogIdOptionExt, MessageSummary, RaftLogReader, RaftSnapshotBuilder,
    RaftStorage, RaftTypeConfig, SnapshotMeta, StorageError, StorageIOError, StoredMembership,
    Vote,
};
use serde::{Deserialize, Serialize};
use trace::info;
use tracing::debug;

use crate::errors::{ReplicationError, ReplicationResult};
use crate::state_store::StateStorage;
use crate::{
    ApplyContext, ApplyStorageRef, EntryStorageRef, RaftNodeId, RaftNodeInfo, Response,
    SnapshotMode, TypeConfig,
};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct StoredSnapshot {
    pub data: Vec<u8>,
    pub meta: SnapshotMeta<RaftNodeId, RaftNodeInfo>,
}

// #[derive(Clone)]
pub struct NodeStorage {
    id: RaftNodeId,
    info: RaftNodeInfo,
    state: Arc<StateStorage>,
    engine: ApplyStorageRef,
    raft_logs: EntryStorageRef,
}

impl NodeStorage {
    pub fn open(
        id: RaftNodeId,
        info: RaftNodeInfo,
        state: Arc<StateStorage>,
        engine: ApplyStorageRef,
        raft_logs: EntryStorageRef,
    ) -> ReplicationResult<Self> {
        Ok(Self {
            id,
            info,
            state,
            engine,
            raft_logs,
        })
    }

    fn group_id(&self) -> u32 {
        self.info.group_id
    }

    pub async fn destory(&self) -> ReplicationResult<()> {
        self.state.del_group(self.group_id())?;
        self.engine.write().await.destory().await?;
        self.raft_logs.write().await.destory().await?;

        Ok(())
    }

    fn get_snapshot_id(&self, log_id: &Option<LogId<u64>>) -> ReplicationResult<String> {
        let snapshot_idx = self.state.incr_snapshot_index(self.group_id(), 1)?;
        let snapshot_id = if let Some(log_id) = log_id {
            format!("{}-{}-{}", log_id.leader_id, log_id.index, snapshot_idx)
        } else {
            "".to_string()
        };

        Ok(snapshot_id)
    }

    async fn get_snapshot_log_id(&self, seq_no: u64) -> ReplicationResult<Option<LogId<u64>>> {
        if let Some(entry) = self.raft_logs.write().await.entry(seq_no).await? {
            if entry.log_id.index == seq_no {
                return Ok(Some(entry.log_id));
            }
        }

        if let Some(log_id) = self.state.get_snapshot_applied_log(self.group_id())? {
            if log_id.index == seq_no {
                return Ok(Some(log_id));
            }
        }

        if let Some(log_id) = self.state.get_last_applied_log(self.group_id())? {
            if log_id.index == seq_no {
                return Ok(Some(log_id));
            }
        }

        Ok(None)
    }

    fn get_snapshot_membership(
        &self,
        seq_no: u64,
    ) -> ReplicationResult<StoredMembership<u64, RaftNodeInfo>> {
        let mut distance = u64::MAX;
        let mut mem_ship = self.state.get_last_membership(self.group_id())?;

        let memberships = self.state.get_memberships(self.group_id())?;
        for (_, value) in memberships.iter() {
            if let Some(logid) = value.log_id() {
                if logid.index > seq_no {
                    continue;
                }

                if seq_no - logid.index < distance {
                    mem_ship = value.clone();
                    distance = seq_no - logid.index;
                }
            }
        }

        Ok(mem_ship)
    }

    async fn get_snapshot_meta(
        &self,
        seq_no: Option<u64>,
    ) -> ReplicationResult<SnapshotMeta<u64, RaftNodeInfo>> {
        if let Some(seq_no) = seq_no {
            let last_log_id = self.get_snapshot_log_id(seq_no).await?;
            let last_membership = self.get_snapshot_membership(seq_no)?;
            let snapshot_id = self.get_snapshot_id(&last_log_id)?;

            let _ = self.state.clear_memberships(self.group_id(), seq_no);

            Ok(SnapshotMeta {
                snapshot_id,
                last_log_id,
                last_membership,
            })
        } else {
            let last_log_id = self.state.get_last_applied_log(self.group_id())?;
            let last_membership = self.state.get_last_membership(self.group_id())?;
            let snapshot_id = self.get_snapshot_id(&last_log_id)?;

            if let Some(index) = last_log_id.index() {
                let _ = self.state.clear_memberships(self.group_id(), index);
            }

            Ok(SnapshotMeta {
                snapshot_id,
                last_log_id,
                last_membership,
            })
        }
    }

    async fn create_snapshot(&self, mode: SnapshotMode) -> ReplicationResult<StoredSnapshot> {
        let mut engine = self.engine.write().await;
        let (data, index) = engine.snapshot(mode).await?;
        let meta = self.get_snapshot_meta(index).await?;
        let snapshot = StoredSnapshot { data, meta };

        info!(
            "create snapshot index: {:?}, data len {}, meta {:?}",
            index,
            snapshot.data.len(),
            snapshot.meta
        );

        Ok(snapshot)
    }

    async fn apply_snapshot(&self, snap: StoredSnapshot) -> ReplicationResult<()> {
        let group_id = self.group_id();
        let log_id = snap.meta.last_log_id.unwrap_or_default();
        let membership = snap.meta.last_membership;
        self.state.set_last_applied_log(group_id, log_id)?;
        self.state.set_last_membership(group_id, membership)?;
        self.state.set_snapshot_applied_log(group_id, log_id)?;

        self.engine.write().await.restore(&snap.data).await?;

        Ok(())
    }
}

type StorageResult<T> = Result<T, StorageError<RaftNodeId>>;

impl RaftLogReader<TypeConfig> for Arc<NodeStorage> {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + Send>(
        &mut self,
        range: RB,
    ) -> StorageResult<Vec<Entry<TypeConfig>>> {
        debug!("Storage callback try_get_log_entries: [{:?})", range);

        let start = match range.start_bound() {
            std::ops::Bound::Included(x) => *x,
            std::ops::Bound::Excluded(x) => *x + 1,
            std::ops::Bound::Unbounded => 0,
        };

        let end = match range.end_bound() {
            std::ops::Bound::Included(x) => *x + 1,
            std::ops::Bound::Excluded(x) => *x,
            std::ops::Bound::Unbounded => u64::MAX,
        };

        let entries = self
            .raft_logs
            .write()
            .await
            .entries(start, end)
            .await
            .map_err(|e| StorageError::IO {
                source: StorageIOError::read_logs(&e),
            })?;

        Ok(entries)
    }
}

impl RaftSnapshotBuilder<TypeConfig> for Arc<NodeStorage> {
    #[tracing::instrument(level = "trace", skip(self))]
    async fn build_snapshot(&mut self) -> Result<Snapshot<TypeConfig>, StorageError<RaftNodeId>> {
        info!("Storage callback build_snapshot");

        let snapshot = self
            .create_snapshot(SnapshotMode::BuildSnapshot)
            .await
            .map_err(|e| StorageIOError::read_state_machine(&e))?;

        Ok(Snapshot {
            meta: snapshot.meta,
            snapshot: Box::new(Cursor::new(snapshot.data)),
        })
    }
}

impl RaftStorage<TypeConfig> for Arc<NodeStorage> {
    type LogReader = Self;
    type SnapshotBuilder = Self;

    async fn get_log_reader(&mut self) -> Self::LogReader {
        debug!("Storage callback get_log_reader");

        self.clone()
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        debug!("Storage callback get_snapshot_builder");

        self.clone()
    }

    async fn get_log_state(&mut self) -> StorageResult<LogState<TypeConfig>> {
        debug!("Storage callback get_log_state");

        let last = self
            .raft_logs
            .write()
            .await
            .last_entry()
            .await
            .map_err(|e| StorageIOError::read_logs(&e))?
            .map(|ent| ent.log_id);

        let last_purged_log_id = self
            .state
            .get_last_purged(self.group_id())
            .map_err(|e| StorageIOError::read(&e))?;

        let last_log_id = match last {
            None => last_purged_log_id,
            Some(x) => Some(x),
        };
        Ok(LogState {
            last_purged_log_id,
            last_log_id,
        })
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn save_vote(&mut self, vote: &Vote<RaftNodeId>) -> Result<(), StorageError<RaftNodeId>> {
        debug!("Storage callback save_vote vote: {:?}", vote);

        self.state
            .set_vote(self.group_id(), vote)
            .map_err(|e| StorageError::IO {
                source: StorageIOError::write_vote(&e),
            })?;

        Ok(())
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<RaftNodeId>>, StorageError<RaftNodeId>> {
        debug!("Storage callback read_vote");

        let vote = self
            .state
            .get_vote(self.group_id())
            .map_err(|e| StorageError::IO {
                source: StorageIOError::read_vote(&e),
            })?;

        Ok(vote)
    }

    #[tracing::instrument(level = "trace", skip(self, entries))]
    async fn append_to_log<I>(&mut self, entries: I) -> StorageResult<()>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + Send,
    {
        let entries: Vec<Entry<TypeConfig>> = entries.into_iter().collect();
        if entries.is_empty() {
            return Ok(());
        }

        let begin = entries.first().map_or(0, |ent| ent.log_id.index);
        let end = entries.last().map_or(0, |ent| ent.log_id.index);
        debug!("Storage callback append_to_log entires:[{}~{}]", begin, end);

        let mut logs = self.raft_logs.write().await;
        // Remove all entries overwritten by `ents`.
        logs.del_after(begin)
            .await
            .map_err(|e| StorageIOError::write_logs(&e))?;
        logs.append(&entries)
            .await
            .map_err(|e| StorageIOError::write_logs(&e))?;

        Ok(())
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn delete_conflict_logs_since(&mut self, log_id: LogId<RaftNodeId>) -> StorageResult<()> {
        debug!(
            "Storage callback delete_conflict_logs_since log_id: {:?}",
            log_id
        );

        self.raft_logs
            .write()
            .await
            .del_after(log_id.index)
            .await
            .map_err(|e| StorageIOError::write_logs(&e))?;

        Ok(())
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn purge_logs_upto(
        &mut self,
        log_id: LogId<RaftNodeId>,
    ) -> Result<(), StorageError<RaftNodeId>> {
        info!("Storage callback purge_logs_upto log_id: {:?}", log_id);

        self.state
            .set_last_purged(self.group_id(), log_id)
            .map_err(|e| StorageIOError::write(&e))?;

        self.raft_logs
            .write()
            .await
            .del_before(log_id.index + 1)
            .await
            .map_err(|e| StorageIOError::write_logs(&e))?;

        Ok(())
    }

    async fn last_applied_state(
        &mut self,
    ) -> Result<
        (
            Option<LogId<RaftNodeId>>,
            StoredMembership<RaftNodeId, RaftNodeInfo>,
        ),
        StorageError<RaftNodeId>,
    > {
        debug!("Storage callback last_applied_state");

        let log_id = self
            .state
            .get_last_applied_log(self.group_id())
            .map_err(|e| StorageIOError::read_state_machine(&e))?;

        let member_ship = self
            .state
            .get_last_membership(self.group_id())
            .map_err(|e| StorageIOError::read_state_machine(&e))?;

        Ok((log_id, member_ship))
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<<TypeConfig as RaftTypeConfig>::SnapshotData>, StorageError<RaftNodeId>> {
        debug!("Storage callback begin_receiving_snapshot");

        Ok(Box::new(Cursor::new(Vec::new())))
    }

    #[tracing::instrument(level = "trace", skip(self, snapshot))]
    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<RaftNodeId, RaftNodeInfo>,
        snapshot: Box<<TypeConfig as RaftTypeConfig>::SnapshotData>,
    ) -> Result<(), StorageError<RaftNodeId>> {
        debug!(
            "Storage callback install_snapshot size: {}",
            snapshot.get_ref().len()
        );

        let updated_snapshot = StoredSnapshot {
            meta: meta.clone(),
            data: snapshot.into_inner(),
        };

        self.apply_snapshot(updated_snapshot)
            .await
            .map_err(|e| StorageIOError::write(&e))?;

        Ok(())
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<TypeConfig>>, StorageError<RaftNodeId>> {
        info!("Storage callback get_current_snapshot");

        let snapshot = self
            .create_snapshot(SnapshotMode::GetSnapshot)
            .await
            .map_err(|e| StorageIOError::read_state_machine(&e))?;

        Ok(Some(Snapshot {
            meta: snapshot.meta,
            snapshot: Box::new(Cursor::new(snapshot.data)),
        }))
    }

    #[tracing::instrument(level = "trace", skip(self, entries))]
    async fn apply_to_state_machine(
        &mut self,
        entries: &[Entry<TypeConfig>],
    ) -> Result<Vec<Response>, StorageError<RaftNodeId>> {
        let mut res = Vec::with_capacity(entries.len());

        let mut engine = self.engine.write().await;
        for entry in entries {
            debug!(
                "Storage callback apply_to_state_machine: {:?}",
                entry.summary()
            ); // term-raftid-index:payload type

            self.state
                .set_last_applied_log(self.group_id(), entry.log_id)
                .map_err(|e| StorageIOError::write(&e))?;

            match entry.payload {
                EntryPayload::Blank => {
                    res.push(vec![]);
                }

                EntryPayload::Normal(ref req) => {
                    let ctx = ApplyContext {
                        apply_type: crate::APPLY_TYPE_WRITE,
                        index: entry.log_id.index,
                        raft_id: self.id,
                    };
                    let rsp = engine
                        .apply(&ctx, req)
                        .await
                        .map_err(|e| StorageIOError::write(&e))?;

                    res.push(rsp);
                }

                EntryPayload::Membership(ref mem) => {
                    self.state
                        .set_last_membership(
                            self.group_id(),
                            StoredMembership::new(Some(entry.log_id), mem.clone()),
                        )
                        .map_err(|e| StorageIOError::write(&e))?;

                    res.push(vec![]);
                }
            };
        }

        Ok(res)
    }
}

mod test {
    use std::sync::Arc;

    use tokio::sync::RwLock;

    use super::NodeStorage;
    use crate::apply_store::HeedApplyStorage;
    use crate::entry_store::HeedEntryStorage;
    use crate::state_store::StateStorage;
    use crate::{ApplyStorageRef, EntryStorageRef, RaftNodeInfo};

    #[test]
    pub fn test_node_store() {
        let path = "/tmp/cnosdb/test_raft_store".to_string();
        std::fs::remove_dir_all(path.clone());
        std::fs::create_dir_all(path.clone()).unwrap();

        openraft::testing::Suite::test_all(get_node_store).unwrap();
        std::fs::remove_dir_all(path);
    }

    pub async fn get_node_store() -> Arc<NodeStorage> {
        let path = tempfile::tempdir_in("/tmp/cnosdb/test_raft_store").unwrap();

        let max_size = 1024 * 1024 * 1024;
        let state = StateStorage::open(path.path().join("state"), max_size).unwrap();
        let entry = HeedEntryStorage::open(path.path().join("entry"), max_size).unwrap();
        let engine = HeedApplyStorage::open(path.path().join("engine"), max_size).unwrap();

        let state = Arc::new(state);
        let entry: EntryStorageRef = Arc::new(RwLock::new(entry));
        let engine: ApplyStorageRef = Arc::new(RwLock::new(engine));

        let info = RaftNodeInfo {
            group_id: 2222,
            address: "127.0.0.1:1234".to_string(),
        };

        let storage = NodeStorage::open(1000, info, state, engine, entry).unwrap();

        Arc::new(storage)
    }
}
