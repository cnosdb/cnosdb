use std::collections::BTreeMap;
use std::sync::Arc;

use models::schema::split_owner;
use openraft::entry::FromAppData;
use openraft::EntryPayload;
use replication::entry_store::EntryStorage;
use replication::errors::{ReplicationError, ReplicationResult};
use replication::TypeConfig;
use tokio::sync::Mutex;

use crate::wal::writer::WriteTask;
use crate::wal::{writer, VnodeWal};

// https://datafuselabs.github.io/openraft/getting-started.html

// openraft::declare_raft_types!(
//     pub VnodeRaftConfig:
//         D            = reader::Block,
//         R            = u64,
//         NodeId       = u64,
//         Node         = openraft::BasicNode,
//         Entry        = openraft::Entry<VnodeRaftConfig>,
//         SnapshotData = std::io::Cursor<Vec<u8>>,
//         AsyncRuntime = openraft::TokioRuntime,
// );

pub type WalRaftEntry = openraft::Entry<TypeConfig>;

pub struct WalRaftEntryStorage {
    inner: Arc<Mutex<WalRaftEntryStorageInner>>,
}

impl WalRaftEntryStorage {
    pub fn new(wal: VnodeWal) -> Self {
        Self {
            inner: Arc::new(Mutex::new(WalRaftEntryStorageInner {
                wal,
                seq_wal_pos_index: BTreeMap::new(),
            })),
        }
    }
}

#[async_trait::async_trait]
impl EntryStorage for WalRaftEntryStorage {
    async fn entry(&self, seq: u64) -> ReplicationResult<Option<WalRaftEntry>> {
        let mut inner = self.inner.lock().await;

        let (wal_id, pos) = match inner.seq_wal_pos_index.get(&seq) {
            Some((wal_id, pos)) => (*wal_id, *pos),
            None => return Ok(None),
        };
        inner.read(wal_id, pos).await
    }

    async fn del_before(&self, seq: u64) -> ReplicationResult<()> {
        let mut inner = self.inner.lock().await;
        if inner.seq_wal_pos_index.contains_key(&seq) {
            inner.seq_wal_pos_index.retain(|&d, _| d >= seq);
        }
        Ok(())
    }

    async fn del_after(&self, seq: u64) -> ReplicationResult<()> {
        let mut inner = self.inner.lock().await;
        if inner.seq_wal_pos_index.contains_key(&seq) {
            inner.seq_wal_pos_index.retain(|&d, _| d <= seq);
        }
        Ok(())
    }

    async fn append(&self, entries: &[WalRaftEntry]) -> ReplicationResult<()> {
        if entries.is_empty() {
            return Ok(());
        }
        let mut inner = self.inner.lock().await;
        for ent in entries {
            let seq = ent.log_id.index;
            match &ent.payload {
                EntryPayload::Blank => {
                    trace::debug!("WAL received blank raft log at index {seq}.");
                    continue;
                }
                EntryPayload::Normal(data) => {
                    trace::debug!("WAL received normal raft log at index {seq}.");
                    // TODO(zipper): add special codec type only for raft read&write
                    let task: writer::Task = bincode::deserialize(data)?;
                    let wal_id = inner.wal.current_wal_id();
                    let (seq, pos) =
                        inner.wal.write_inner_task(seq, &task).await.map_err(|e| {
                            ReplicationError::RaftInternalErr { msg: e.to_string() }
                        })?;
                    inner.seq_wal_pos_index.insert(seq, (wal_id, pos));
                }
                EntryPayload::Membership(_data) => {
                    trace::debug!("WAL received membership raft log at index {seq}.");
                    continue;
                }
            }
        }
        Ok(())
    }

    async fn last_entry(&self) -> ReplicationResult<Option<WalRaftEntry>> {
        let mut inner = self.inner.lock().await;

        if let Some(wal_id_pos) = inner.seq_wal_pos_index.last_entry() {
            let (wal_id, pos) = wal_id_pos.get().to_owned();
            inner.read(wal_id, pos).await
        } else {
            Ok(None)
        }
    }

    async fn entries(&self, begin_seq: u64, end_seq: u64) -> ReplicationResult<Vec<WalRaftEntry>> {
        let mut inner = self.inner.lock().await;

        let min_seq: u64;
        let max_seq: u64;
        if let Some(wal_id_pos) = inner.seq_wal_pos_index.first_entry() {
            let seq = *wal_id_pos.key();
            min_seq = seq.max(begin_seq);
        } else {
            min_seq = u64::MAX;
        }
        if let Some(wal_id_pos) = inner.seq_wal_pos_index.last_entry() {
            let seq = *wal_id_pos.key();
            max_seq = seq.min(end_seq);
        } else {
            max_seq = 0;
        }

        if min_seq > max_seq {
            return Ok(Vec::new());
        }

        let mut ret_entries = Vec::new();
        for seq in min_seq..=max_seq {
            let (wal_id, pos) = match inner.seq_wal_pos_index.get(&seq) {
                Some((wal_id, pos)) => (*wal_id, *pos),
                None => continue,
            };
            if let Some(entry) = inner.read(wal_id, pos).await? {
                ret_entries.push(entry);
            }
        }

        Ok(ret_entries)
    }
}

struct WalRaftEntryStorageInner {
    wal: VnodeWal,
    /// Maps seq to (WAL id, position).
    seq_wal_pos_index: BTreeMap<u64, (u64, usize)>,
}

impl WalRaftEntryStorageInner {
    async fn read(&mut self, wal_id: u64, pos: usize) -> ReplicationResult<Option<WalRaftEntry>> {
        match self
            .wal
            .read(wal_id, pos)
            .await
            .map_err(|e| ReplicationError::RaftInternalErr { msg: e.to_string() })?
        {
            Some(d) => {
                // TODO(zipper): add special codec type only for raft read&write
                let (_, database_name) = split_owner(&self.wal.tenant_database);
                let mut task = writer::Task::try_from(&d)
                    .map_err(|e| ReplicationError::RaftInternalErr { msg: e.to_string() })?;
                if let writer::Task::Write(WriteTask { database, .. }) = &mut task {
                    database.push_str(database_name);
                }
                let task_bytes = bincode::serialize(&task)
                    .map_err(|e| ReplicationError::RaftInternalErr { msg: e.to_string() })?;
                let entry = WalRaftEntry::from_app_data(task_bytes);
                Ok(Some(entry))
            }
            None => Ok(None),
        }
    }
}
