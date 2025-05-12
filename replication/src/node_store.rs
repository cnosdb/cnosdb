use std::fmt::Debug;
use std::io::Cursor;
use std::ops::RangeBounds;
use std::sync::Arc;

use openraft::storage::{LogState, Snapshot};
use openraft::{
    Entry, EntryPayload, LogId, MessageSummary, RaftLogReader, RaftSnapshotBuilder, RaftStorage,
    RaftTypeConfig, SnapshotMeta, StorageError, StorageIOError, StoredMembership, Vote,
};
use serde::{Deserialize, Serialize};
use trace::info;
use tracing::debug;

use crate::errors::ReplicationResult;
use crate::state_store::StateStorage;
use crate::{
    ApplyContext, ApplyStorageRef, EngineMetrics, EntriesMetrics, EntryStorageRef, RaftNodeId,
    RaftNodeInfo, Response, TypeConfig,
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
    pub async fn open(
        id: RaftNodeId,
        info: RaftNodeInfo,
        state: Arc<StateStorage>,
        engine: ApplyStorageRef,
        raft_logs: EntryStorageRef,
    ) -> ReplicationResult<Self> {
        let storage = Self {
            id,
            info,
            state,
            engine,
            raft_logs,
        };

        storage.create_snapshot().await?;

        Ok(storage)
    }

    fn group_id(&self) -> u32 {
        self.info.group_id
    }

    pub async fn destroy(&self) -> ReplicationResult<()> {
        self.state.del_group(self.group_id())?;
        self.engine.write().await.destroy().await?;
        self.raft_logs.write().await.destroy().await?;

        Ok(())
    }

    pub async fn engine_metrics(&self) -> ReplicationResult<EngineMetrics> {
        self.engine.read().await.metrics().await
    }

    pub async fn entries_metrics(&self) -> ReplicationResult<EntriesMetrics> {
        self.raft_logs.write().await.metrics().await
    }

    // term-raftid-index
    fn get_snapshot_id(&self, log_id: &Option<LogId<u64>>) -> ReplicationResult<String> {
        if let Some(log_id) = log_id {
            Ok(format!("{}-{}", log_id.leader_id, log_id.index))
        } else {
            Ok("".to_string())
        }
    }

    async fn get_snapshot_log_id(&self, index: u64) -> ReplicationResult<Option<LogId<u64>>> {
        if let Some(entry) = self.raft_logs.write().await.entry(index).await? {
            if entry.log_id.index == index {
                return Ok(Some(entry.log_id));
            }
        }

        if let Some(log_id) = self.state.get_snapshot_applied_log(self.group_id())? {
            if log_id.index == index {
                return Ok(Some(log_id));
            }
        }

        if let Some(log_id) = self.state.get_last_applied_log(self.group_id())? {
            if log_id.index == index {
                return Ok(Some(log_id));
            }
        }

        Ok(None)
    }

    fn get_snapshot_membership(
        &self,
        index: u64,
    ) -> ReplicationResult<StoredMembership<u64, RaftNodeInfo>> {
        let mut mem_ship = self.state.get_last_membership(self.group_id())?;

        let memberships = self.state.get_membership_list(self.group_id())?;
        let mut list: Vec<(String, StoredMembership<u64, RaftNodeInfo>)> =
            memberships.into_iter().collect();
        list.sort_by_key(|x| x.1.log_id().unwrap_or_default().index);
        list.retain(|x| x.1.log_id().unwrap_or_default().index <= index);

        if let Some((_, item)) = list.last() {
            mem_ship = item.clone();
        }

        Ok(mem_ship)
    }

    async fn get_snapshot_meta(
        &self,
        index: u64,
    ) -> ReplicationResult<SnapshotMeta<u64, RaftNodeInfo>> {
        let last_log_id = self.get_snapshot_log_id(index).await?;
        let last_membership = self.get_snapshot_membership(index)?;
        let snapshot_id = self.get_snapshot_id(&last_log_id)?;

        let _ = self.state.clear_memberships(self.group_id(), index);

        Ok(SnapshotMeta {
            snapshot_id,
            last_log_id,
            last_membership,
        })
    }

    async fn create_snapshot(&self) -> ReplicationResult<StoredSnapshot> {
        let mut engine = self.engine.write().await;
        let last_log_id = self.state.get_last_applied_log(self.group_id())?;
        if let Some(last_log_id) = last_log_id {
            let (data, index) = engine.create_snapshot(last_log_id.index).await?;
            let meta = self.get_snapshot_meta(index).await?;

            info!(
                "node-{} last applied:{}, create snapshot index: {:?}, data len {}, meta {:?}",
                self.id,
                last_log_id,
                index,
                data.len(),
                meta
            );

            Ok(StoredSnapshot { data, meta })
        } else {
            info!("node-{}  create snapshot None", self.id,);
            Ok(StoredSnapshot {
                data: vec![],
                meta: SnapshotMeta::default(),
            })
        }
    }

    async fn apply_snapshot(&self, snap: StoredSnapshot) -> ReplicationResult<()> {
        info!("node-{} apply snapshot  meta {:?}", self.id, snap.meta);

        let group_id = self.group_id();
        let log_id = snap.meta.last_log_id.unwrap_or_default();
        let membership = snap.meta.last_membership;
        self.state.set_last_applied_log(group_id, log_id)?;
        self.state.set_last_membership(group_id, membership)?;
        self.state.set_snapshot_applied_log(group_id, log_id)?;

        self.engine.write().await.restore(&snap.data).await?;

        Ok(())
    }
    pub async fn sync_wal_writer(&self) {
        let mut entry_storage = self.raft_logs.write().await;
        let _ = entry_storage.sync().await;
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
        debug!("Storage callback build_snapshot");

        let snapshot = self
            .create_snapshot()
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

    #[tracing::instrument(level = "trace", skip(self))]
    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<TypeConfig>>, StorageError<RaftNodeId>> {
        debug!("Storage callback get_current_snapshot");

        let mut engine = self.engine.write().await;
        if let Some((data, index)) = engine
            .get_snapshot()
            .await
            .map_err(|e| StorageIOError::read_state_machine(&e))?
        {
            let meta = self
                .get_snapshot_meta(index)
                .await
                .map_err(|e| StorageIOError::read_state_machine(&e))?;

            info!(
                "node-{}  get snapshot index: {:?}, data len {}, meta {:?}",
                self.id,
                index,
                data.len(),
                meta
            );

            Ok(Some(Snapshot {
                meta,
                snapshot: Box::new(Cursor::new(data)),
            }))
        } else {
            info!("node-{}  get snapshot None", self.id);
            Ok(None)
        }
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

        self.create_snapshot()
            .await
            .map_err(|e| StorageIOError::read_state_machine(&e))?;

        Ok(())
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
        let _ = std::fs::remove_dir_all(path.clone());
        std::fs::create_dir_all(path.clone()).unwrap();

        openraft::testing::Suite::test_all(get_node_store).unwrap();
        let _ = std::fs::remove_dir_all(path);
    }

    #[allow(dead_code)]
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

        let storage = NodeStorage::open(1000, info, state, engine, entry)
            .await
            .unwrap();

        Arc::new(storage)
    }
}
