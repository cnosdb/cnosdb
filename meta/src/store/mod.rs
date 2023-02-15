#![allow(clippy::module_inception)]

use std::fmt::Debug;
use std::io::Cursor;
use std::ops::{Bound, RangeBounds};
use std::sync::Arc;

use byteorder::{BigEndian, ByteOrder, ReadBytesExt};
use openraft::async_trait::async_trait;
use openraft::storage::{LogState, Snapshot};
use openraft::{
    AnyError, EffectiveMembership, Entry, EntryPayload, ErrorSubject, ErrorVerb, LogId,
    RaftLogReader, RaftSnapshotBuilder, RaftStorage, SnapshotMeta, StorageError, StorageIOError,
    Vote,
};
use tokio::sync::RwLock;
use tracing::info;

use crate::error::{
    l_r_err, l_w_err, s_r_err, s_w_err, sm_r_err, v_r_err, v_w_err, StorageIOResult, StorageResult,
};
use crate::store::state_machine::{CommandResp, StateMachineContent};
use crate::{ClusterNode, ClusterNodeId, TypeConfig};

pub mod command;
pub mod config;
pub mod key_path;
mod sled_store;
pub mod state_machine;
pub mod store;

use self::state_machine::StateMachine;

#[derive(serde::Deserialize, serde::Serialize, Debug)]
pub struct SnapshotInfo {
    pub meta: SnapshotMeta<ClusterNodeId, ClusterNode>,
    /// The data of the state machine at the time of this snapshot.
    pub data: Vec<u8>,
}

pub struct Store {
    db: Arc<sled::Db>,
    logs_tree: sled::Tree,
    store_tree: sled::Tree,
    /// The Raft state machine.
    pub state_machine: RwLock<StateMachine>,
}

impl Store {
    pub fn new(db: sled::Db) -> Self {
        let db = Arc::new(db);
        let sm = StateMachine::new(db.clone());
        Self {
            db: db.clone(),
            logs_tree: db.open_tree("logs").expect("store open failed"),
            store_tree: db.open_tree("store").expect("store open failed"),
            state_machine: RwLock::new(sm),
        }
    }
    fn get_last_purged_(&self) -> StorageIOResult<Option<LogId<u64>>> {
        let val = self
            .store_tree
            .get(b"last_purged_log_id")
            .map_err(|e| {
                StorageIOError::new(ErrorSubject::Store, ErrorVerb::Read, AnyError::new(&e))
            })?
            .and_then(|v| serde_json::from_slice(&v).ok());

        Ok(val)
    }

    async fn set_last_purged_(&self, log_id: LogId<u64>) -> StorageIOResult<()> {
        let val = serde_json::to_vec(&log_id).unwrap();
        self.store_tree
            .insert(b"last_purged_log_id", val.as_slice())
            .map_err(s_w_err)
            .map(|_| ())
    }

    fn get_snapshot_index_(&self) -> StorageIOResult<u64> {
        let val = self
            .store_tree
            .get(b"snapshot_index")
            .map_err(s_r_err)?
            .and_then(|v| serde_json::from_slice(&v).ok())
            .unwrap_or(0);

        Ok(val)
    }

    async fn set_snapshot_index_(&self, snapshot_index: u64) -> StorageIOResult<()> {
        let val = serde_json::to_vec(&snapshot_index).unwrap();
        self.store_tree
            .insert(b"snapshot_index", val.as_slice())
            .map_err(s_w_err)
            .map(|_| ())
    }

    async fn set_vote_(&self, vote: &Vote<ClusterNodeId>) -> StorageIOResult<()> {
        let val = serde_json::to_vec(vote).unwrap();
        self.store_tree
            .insert(b"vote", val)
            .map_err(v_w_err)
            .map(|_| ())
            .map(|_| ())
    }

    fn get_vote_(&self) -> StorageIOResult<Option<Vote<ClusterNodeId>>> {
        let val = self
            .store_tree
            .get(b"vote")
            .map_err(v_r_err)?
            .and_then(|v| serde_json::from_slice(&v).ok());

        Ok(val)
    }

    fn get_current_snapshot_(&self) -> StorageIOResult<Option<SnapshotInfo>> {
        let val = self
            .store_tree
            .get(b"snapshot")
            .map_err(s_r_err)?
            .and_then(|v| serde_json::from_slice(&v).ok());

        Ok(val)
    }

    async fn set_current_snapshot_(&self, snap: SnapshotInfo) -> StorageResult<()> {
        let val = serde_json::to_vec(&snap).unwrap();
        self.store_tree
            .insert(b"snapshot", val.as_slice())
            .map_err(|e| StorageError::IO {
                source: StorageIOError::new(
                    ErrorSubject::Snapshot(snap.meta.signature()),
                    ErrorVerb::Write,
                    AnyError::new(&e),
                ),
            })
            .map(|_| ())
    }
}

#[async_trait]
impl RaftSnapshotBuilder<TypeConfig, Cursor<Vec<u8>>> for Arc<Store> {
    #[tracing::instrument(level = "trace", skip(self))]
    async fn build_snapshot(
        &mut self,
    ) -> Result<Snapshot<ClusterNodeId, ClusterNode, Cursor<Vec<u8>>>, StorageError<ClusterNodeId>>
    {
        let data;
        let last_applied_log;
        let last_membership;

        {
            let state_machine = StateMachineContent::from(&*self.state_machine.read().await);
            data = serde_json::to_vec(&state_machine).map_err(sm_r_err)?;

            last_applied_log = state_machine.last_applied_log;
            last_membership = state_machine.last_membership;
        }

        let snapshot_idx: u64 = self.get_snapshot_index_()? + 1;
        let _ = self.set_snapshot_index_(snapshot_idx).await;

        let snapshot_id = if let Some(last) = last_applied_log {
            format!("{}-{}-{}", last.leader_id, last.index, snapshot_idx)
        } else {
            format!("--{}", snapshot_idx)
        };

        let meta = SnapshotMeta {
            last_log_id: last_applied_log,
            last_membership,
            snapshot_id,
        };

        let snapshot = SnapshotInfo {
            meta: meta.clone(),
            data: data.clone(),
        };

        self.set_current_snapshot_(snapshot).await?;

        Ok(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(data)),
        })
    }
}

/// converts an id to a byte vector for storing in the database.
/// Note that we're using big endian encoding to ensure correct sorting of keys
/// with notes form: https://github.com/spacejam/sled#a-note-on-lexicographic-ordering-and-endianness
fn id_to_bin(id: u64) -> [u8; 8] {
    let mut buf: [u8; 8] = [0; 8];
    BigEndian::write_u64(&mut buf, id);
    buf
}

fn bin_to_id(buf: &[u8]) -> u64 {
    (&buf[0..8]).read_u64::<BigEndian>().unwrap()
}

#[async_trait]
impl RaftLogReader<TypeConfig> for Arc<Store> {
    async fn get_log_state(&mut self) -> StorageResult<LogState<TypeConfig>> {
        let last_purged_log_id = self.get_last_purged_()?;

        let last_res = self.logs_tree.last();
        if last_res.is_err() {
            return Ok(LogState {
                last_purged_log_id,
                last_log_id: last_purged_log_id,
            });
        }

        let last = last_res.unwrap().and_then(|(_, ent)| {
            Some(
                serde_json::from_slice::<Entry<TypeConfig>>(&ent)
                    .ok()?
                    .log_id,
            )
        });

        let last_log_id = match last {
            None => last_purged_log_id,
            Some(x) => Some(x),
        };
        Ok(LogState {
            last_purged_log_id,
            last_log_id,
        })
    }

    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + Send + Sync>(
        &mut self,
        range: RB,
    ) -> StorageResult<Vec<Entry<TypeConfig>>> {
        let start_bound = range.start_bound();
        let start = match start_bound {
            Bound::Included(x) => id_to_bin(*x),
            Bound::Excluded(x) => id_to_bin(*x + 1),
            Bound::Unbounded => id_to_bin(0),
        };

        let logs = self
            .logs_tree
            .range::<&[u8], _>(start.as_slice()..)
            .map(|el_res| {
                let el = el_res.expect("Faile read log entry");
                let id = el.0;
                let val = el.1;
                let entry: StorageResult<Entry<TypeConfig>> = serde_json::from_slice(&val)
                    .map_err(|e| StorageError::IO { source: l_r_err(e) });
                let id = bin_to_id(&id);

                assert_eq!(Ok(id), entry.as_ref().map(|e| e.log_id.index));
                (id, entry)
            })
            .take_while(|(id, _)| range.contains(id))
            .map(|x| x.1)
            .collect();
        logs
    }
}

#[async_trait]
impl RaftStorage<TypeConfig> for Arc<Store> {
    type SnapshotData = Cursor<Vec<u8>>;
    type LogReader = Self;
    type SnapshotBuilder = Self;

    #[tracing::instrument(level = "trace", skip(self))]
    async fn save_vote(
        &mut self,
        vote: &Vote<ClusterNodeId>,
    ) -> Result<(), StorageError<ClusterNodeId>> {
        self.set_vote_(vote)
            .await
            .map_err(|e| StorageError::IO { source: e })
    }

    async fn read_vote(
        &mut self,
    ) -> Result<Option<Vote<ClusterNodeId>>, StorageError<ClusterNodeId>> {
        self.get_vote_().map_err(|e| StorageError::IO { source: e })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    #[tracing::instrument(level = "trace", skip(self, entries))]
    async fn append_to_log(&mut self, entries: &[&Entry<TypeConfig>]) -> StorageResult<()> {
        let mut batch = sled::Batch::default();
        for entry in entries {
            let id = id_to_bin(entry.log_id.index);
            assert_eq!(bin_to_id(&id), entry.log_id.index);
            let value =
                serde_json::to_vec(entry).map_err(|e| StorageError::IO { source: l_w_err(e) })?;
            batch.insert(id.as_slice(), value);
        }
        self.logs_tree
            .apply_batch(batch)
            .map_err(|e| StorageError::IO { source: l_w_err(e) })?;

        Ok(())
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn delete_conflict_logs_since(
        &mut self,
        log_id: LogId<ClusterNodeId>,
    ) -> StorageResult<()> {
        tracing::debug!("delete_log: [{:?}, +oo)", log_id);

        let from = id_to_bin(log_id.index);
        let to = id_to_bin(0xff_ff_ff_ff_ff_ff_ff_ff);
        let entries = self
            .logs_tree
            .range::<&[u8], _>(from.as_slice()..to.as_slice());
        let mut batch_del = sled::Batch::default();
        for entry_res in entries {
            let entry = entry_res.expect("Read db entry failed");
            batch_del.remove(entry.0);
        }
        self.logs_tree.apply_batch(batch_del).map_err(l_w_err)?;

        Ok(())
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn purge_logs_upto(&mut self, log_id: LogId<ClusterNodeId>) -> StorageResult<()> {
        tracing::debug!("delete_log: [0, {:?}]", log_id);

        self.set_last_purged_(log_id).await?;
        let from = id_to_bin(0);
        let to = id_to_bin(log_id.index);
        let entries = self
            .logs_tree
            .range::<&[u8], _>(from.as_slice()..=to.as_slice());
        let mut batch_del = sled::Batch::default();
        for entry_res in entries {
            let entry = entry_res.expect("Read db entry failed");
            batch_del.remove(entry.0);
        }
        self.logs_tree.apply_batch(batch_del).map_err(l_w_err)?;

        Ok(())
    }

    async fn last_applied_state(
        &mut self,
    ) -> StorageResult<(
        Option<LogId<ClusterNodeId>>,
        EffectiveMembership<ClusterNodeId, ClusterNode>,
    )> {
        let state_machine = self.state_machine.read().await;
        info!(
            "state_matchine info: {:?}",
            state_machine.get_last_applied_log().unwrap()
        );
        Ok((
            state_machine.get_last_applied_log()?,
            state_machine.get_last_membership()?,
        ))
    }

    #[tracing::instrument(level = "trace", skip(self, entries))]
    async fn apply_to_state_machine(
        &mut self,
        entries: &[&Entry<TypeConfig>],
    ) -> Result<Vec<CommandResp>, StorageError<ClusterNodeId>> {
        let mut res = Vec::with_capacity(entries.len());

        let sm = self.state_machine.read().await;
        for entry in entries {
            sm.set_last_applied_log(entry.log_id).await?;
            match entry.payload {
                EntryPayload::Blank => res.push(CommandResp::default()),
                EntryPayload::Membership(ref mem) => {
                    sm.set_last_membership(EffectiveMembership::new(
                        Some(entry.log_id),
                        mem.clone(),
                    ))
                    .await?;
                    res.push(CommandResp::default())
                }

                EntryPayload::Normal(ref req) => res.push(sm.process_write_command(req)),
            };
        }
        Ok(res)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<Self::SnapshotData>, StorageError<ClusterNodeId>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    #[tracing::instrument(level = "trace", skip(self, snapshot))]
    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<ClusterNodeId, ClusterNode>,
        snapshot: Box<Self::SnapshotData>,
    ) -> Result<(), StorageError<ClusterNodeId>> {
        tracing::info!(
            { snapshot_size = snapshot.get_ref().len() },
            "decoding snapshot for installation"
        );

        let new_snapshot = SnapshotInfo {
            meta: meta.clone(),
            data: snapshot.into_inner(),
        };

        let content: StateMachineContent =
            serde_json::from_slice(&new_snapshot.data).map_err(|e| {
                StorageIOError::new(
                    ErrorSubject::Snapshot(new_snapshot.meta.signature()),
                    ErrorVerb::Read,
                    AnyError::new(&e),
                )
            })?;

        {
            let mut state_machine = self.state_machine.write().await;
            *state_machine = StateMachine::from_serializable(content, self.db.clone()).await?;
        }

        self.set_current_snapshot_(new_snapshot).await?;
        Ok(())
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn get_current_snapshot(
        &mut self,
    ) -> StorageResult<Option<Snapshot<ClusterNodeId, ClusterNode, Self::SnapshotData>>> {
        match Store::get_current_snapshot_(self)? {
            Some(snapshot) => {
                let data = snapshot.data.clone();
                Ok(Some(Snapshot {
                    meta: snapshot.meta,
                    snapshot: Box::new(Cursor::new(data)),
                }))
            }
            None => Ok(None),
        }
    }
}
