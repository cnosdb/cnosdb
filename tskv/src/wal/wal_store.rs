use openraft::EntryPayload;
use protos::kv_service::RaftWriteCommand;
use protos::models_helper::parse_prost_bytes;
use replication::errors::{ReplicationError, ReplicationResult};
use replication::{EntryStorage, RaftNodeId, RaftNodeInfo, TypeConfig};
use trace::info;

use super::reader::WalRecordData;
use crate::file_system::async_filesystem::LocalFileSystem;
use crate::file_system::FileSystem;
use crate::vnode_store::VnodeStorage;
use crate::wal::reader::{Block, WalReader};
use crate::wal::VnodeWal;
use crate::{file_utils, Error, Result};

pub type RaftEntry = openraft::Entry<TypeConfig>;
pub type RaftLogMembership = openraft::Membership<RaftNodeId, RaftNodeInfo>;

pub struct RaftEntryStorage {
    inner: RaftEntryStorageInner,
}

impl RaftEntryStorage {
    pub fn new(wal: VnodeWal) -> Self {
        Self {
            inner: RaftEntryStorageInner {
                wal,
                files_meta: vec![],
                entry_cache: cache::CircularKVCache::new(256),
            },
        }
    }

    /// Read WAL files to recover
    pub async fn recover(&mut self, vode_store: &mut VnodeStorage) -> Result<()> {
        self.inner.recover(vode_store).await
    }
}

#[async_trait::async_trait]
impl EntryStorage for RaftEntryStorage {
    async fn append(&mut self, entries: &[RaftEntry]) -> ReplicationResult<()> {
        if entries.is_empty() {
            return Ok(());
        }

        for ent in entries {
            let (wal_id, pos) = self
                .inner
                .wal
                .write_raft_entry(ent)
                .await
                .map_err(|e| ReplicationError::RaftInternalErr { msg: e.to_string() })?;

            self.inner.mark_write_wal(ent.clone(), wal_id, pos).await;
        }
        Ok(())
    }

    async fn del_before(&mut self, seq_no: u64) -> ReplicationResult<()> {
        self.inner.mark_delete_before(seq_no).await;

        Ok(())
    }

    async fn del_after(&mut self, seq_no: u64) -> ReplicationResult<()> {
        self.inner.mark_delete_after(seq_no).await?;

        Ok(())
    }

    async fn entry(&mut self, seq_no: u64) -> ReplicationResult<Option<RaftEntry>> {
        self.inner.read_raft_entry(seq_no).await
    }

    async fn last_entry(&mut self) -> ReplicationResult<Option<RaftEntry>> {
        self.inner.wal_last_entry().await
    }

    async fn entries(&mut self, begin: u64, end: u64) -> ReplicationResult<Vec<RaftEntry>> {
        self.inner.read_raft_entry_range(begin, end).await
    }

    async fn destory(&mut self) -> ReplicationResult<()> {
        let _ = self.inner.wal.close().await;

        let path = self.inner.wal.wal_dir();
        info!("Remove wal files: {:?}", path);
        let _ = std::fs::remove_dir_all(path);

        Ok(())
    }
}

struct WalFileMeta {
    file_id: u64,
    min_seq: u64,
    max_seq: u64,
    reader: WalReader,
    entry_index: Vec<(u64, u64)>, // seq -> pos
}

impl WalFileMeta {
    fn is_empty(&self) -> bool {
        self.min_seq == u64::MAX || self.max_seq == u64::MAX
    }

    fn intersection(&self, start: u64, end: u64) -> Option<(u64, u64)> {
        if self.is_empty() {
            return None;
        }

        let start = self.min_seq.max(start);
        let end = (self.max_seq + 1).min(end); //[ ... )
        if start <= end {
            Some((start, end))
        } else {
            None
        }
    }

    fn mark_entry(&mut self, index: u64, pos: u64) {
        if self.min_seq == u64::MAX || self.min_seq > index {
            self.min_seq = index
        }

        if self.max_seq == u64::MAX || self.max_seq < index {
            self.max_seq = index
        }

        self.entry_index.push((index, pos));
    }

    fn del_befor(&mut self, index: u64) {
        if self.min_seq == u64::MAX || self.min_seq >= index {
            return;
        }

        self.min_seq = index;
        let idx = match self.entry_index.binary_search_by(|v| v.0.cmp(&index)) {
            Ok(idx) => idx,
            Err(idx) => idx,
        };

        self.entry_index.drain(0..idx);
    }

    fn del_after(&mut self, index: u64) -> u64 {
        if self.max_seq == u64::MAX || self.max_seq < index {
            return 0;
        }

        if index == 0 {
            self.min_seq = u64::MAX;
            self.max_seq = u64::MAX;
            self.entry_index.clear();
            return 0;
        }

        self.max_seq = index - 1;
        let idx = match self.entry_index.binary_search_by(|v| v.0.cmp(&index)) {
            Ok(idx) => idx,
            Err(idx) => idx,
        };

        let mut pos = 0;
        if idx < self.entry_index.len() {
            pos = self.entry_index[idx].1;
        }

        self.entry_index.drain(idx..);

        pos
    }

    async fn get_entry_by_index(&mut self, index: u64) -> ReplicationResult<Option<RaftEntry>> {
        if let Ok(idx) = self.entry_index.binary_search_by(|v| v.0.cmp(&index)) {
            let pos = self.entry_index[idx].1;
            self.get_entry(pos).await
        } else {
            Ok(None)
        }
    }

    async fn get_entry(&mut self, pos: u64) -> ReplicationResult<Option<RaftEntry>> {
        if let Some(record) = self
            .reader
            .read_wal_record_data(pos)
            .await
            .map_err(|e| ReplicationError::RaftInternalErr { msg: e.to_string() })?
        {
            if let Block::RaftLog(entry) = record.block {
                return Ok(Some(entry));
            }
        }

        Ok(None)
    }
}
struct RaftEntryStorageInner {
    wal: VnodeWal,
    files_meta: Vec<WalFileMeta>,
    entry_cache: cache::CircularKVCache<u64, RaftEntry>,
}

impl RaftEntryStorageInner {
    async fn mark_write_wal(&mut self, entry: RaftEntry, wal_id: u64, pos: u64) {
        let index = entry.log_id.index;
        if let Some(item) = self
            .files_meta
            .iter_mut()
            .rev()
            .find(|item| item.file_id == wal_id)
        {
            item.mark_entry(index, pos);
        } else {
            let mut item = WalFileMeta {
                file_id: wal_id,
                min_seq: u64::MAX,
                max_seq: u64::MAX,
                entry_index: vec![],
                reader: self.wal.current_wal.new_reader().await,
            };

            item.entry_index.reserve(8 * 1024);
            item.mark_entry(index, pos);
            self.files_meta.push(item);
        }

        self.entry_cache.put(index, entry);
    }

    async fn mark_delete_before(&mut self, seq_no: u64) {
        if self.min_sequence() >= seq_no {
            return;
        }

        for item in self.files_meta.iter_mut() {
            if item.min_seq < seq_no {
                item.del_befor(seq_no);
            } else {
                break;
            }
        }

        self.entry_cache.del_before(seq_no);

        let _ = self.wal.delete_wal_before_seq(seq_no).await;
    }

    async fn mark_delete_after(&mut self, seq_no: u64) -> ReplicationResult<()> {
        if self.max_sequence() < seq_no || self.max_sequence() == u64::MAX {
            return Ok(());
        }

        let mut delete_file_ids = vec![];
        for item in self.files_meta.iter_mut().rev() {
            if item.min_seq >= seq_no {
                delete_file_ids.push(item.file_id);
                continue;
            }

            if item.max_seq >= seq_no {
                let pos = item.del_after(seq_no);
                if pos > 0 {
                    let _ = self.wal.truncate_wal_file(item.file_id, pos, seq_no).await;
                }
            }

            break;
        }

        self.entry_cache.del_after(seq_no);
        self.files_meta
            .retain(|item| !delete_file_ids.contains(&item.file_id));

        self.wal
            .rollback_wal_writer(&delete_file_ids)
            .await
            .map_err(|e| ReplicationError::RaftInternalErr { msg: e.to_string() })?;

        Ok(())
    }

    fn entries_from_cache(&self, start: u64, end: u64) -> Option<Vec<RaftEntry>> {
        let mut entries = vec![];
        for index in start..end {
            if let Some(entry) = self.entry_cache.get(&index) {
                entries.push(entry.clone());
            } else {
                return None;
            }
        }

        Some(entries)
    }

    fn is_empty(&self) -> bool {
        if self.min_sequence() == u64::MAX || self.max_sequence() == u64::MAX {
            return true;
        }

        false
    }

    fn min_sequence(&self) -> u64 {
        if let Some(item) = self.files_meta.first() {
            return item.min_seq;
        }

        u64::MAX
    }

    fn max_sequence(&self) -> u64 {
        if let Some(item) = self.files_meta.last() {
            return item.max_seq;
        }

        u64::MAX
    }

    async fn wal_last_entry(&mut self) -> ReplicationResult<Option<RaftEntry>> {
        if let Some(entry) = self.entry_cache.last() {
            Ok(Some(entry.clone()))
        } else {
            Ok(None)
        }
    }

    async fn read_raft_entry_range(
        &mut self,
        start: u64,
        end: u64,
    ) -> ReplicationResult<Vec<RaftEntry>> {
        if let Some(entries) = self.entries_from_cache(start, end) {
            return Ok(entries);
        }

        let mut list = vec![];
        for item in self.files_meta.iter_mut() {
            if let Some((start, end)) = item.intersection(start, end) {
                for index in start..end {
                    if let Some(entry) = item.get_entry_by_index(index).await? {
                        list.push(entry);
                    }
                }
            }
        }

        Ok(list)
    }

    async fn read_raft_entry(&mut self, index: u64) -> ReplicationResult<Option<RaftEntry>> {
        if let Some(entry) = self.entry_cache.get(&index) {
            return Ok(Some(entry.clone()));
        }

        let location = match self
            .files_meta
            .iter_mut()
            .rev()
            .find(|item| (index >= item.min_seq) && (index <= item.max_seq))
        {
            Some(item) => item,
            None => return Ok(None),
        };

        location.get_entry_by_index(index).await
    }

    /// Read WAL files to recover: engine, index, cache.
    pub async fn recover(&mut self, vode_store: &mut VnodeStorage) -> Result<()> {
        let wal_files = LocalFileSystem::list_file_names(self.wal.wal_dir());
        for file_name in wal_files {
            // If file name cannot be parsed to wal id, skip that file.
            let wal_id = match file_utils::get_wal_file_id(&file_name) {
                Ok(id) => id,
                Err(_) => continue,
            };
            let path = self.wal.wal_dir().join(&file_name);
            if !LocalFileSystem::try_exists(&path) {
                continue;
            }
            let reader = self.wal.wal_reader(wal_id).await?;
            let mut record_reader = reader.take_record_reader();
            loop {
                let record = record_reader.read_record().await;
                match record {
                    Ok(r) => {
                        if r.data.len() < 9 {
                            continue;
                        }

                        let wal_entry = WalRecordData::new(r.data);
                        if let Block::RaftLog(entry) = wal_entry.block {
                            if let EntryPayload::Normal(ref req) = entry.payload {
                                let ctx = replication::ApplyContext {
                                    index: entry.log_id.index,
                                    raft_id: self.wal.vnode_id as u64,
                                    apply_type: replication::APPLY_TYPE_WAL,
                                };

                                let request = parse_prost_bytes::<RaftWriteCommand>(req).unwrap();
                                if let Some(command) = request.command {
                                    vode_store.apply(&ctx, command).await.unwrap();
                                }
                            }

                            self.mark_write_wal(entry, wal_id, r.pos).await;
                        }
                    }
                    Err(Error::Eof) => {
                        break;
                    }
                    Err(Error::RecordFileHashCheckFailed { .. }) => continue,
                    Err(e) => {
                        trace::error!("Error reading wal: {:?}", e);
                        return Err(Error::WalTruncated);
                    }
                }
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use std::path::{Path, PathBuf};
    use std::sync::atomic::AtomicUsize;
    use std::sync::{atomic, Arc};

    use models::schema::make_owner;
    use openraft::EntryPayload;
    use replication::apply_store::HeedApplyStorage;
    use replication::node_store::NodeStorage;
    use replication::state_store::StateStorage;
    use replication::{ApplyStorageRef, EntryStorage, EntryStorageRef, RaftNodeInfo};
    use tokio::sync::RwLock;

    use crate::wal::wal_store::{RaftEntry, RaftEntryStorage};
    use crate::wal::VnodeWal;
    use crate::Result;

    pub async fn get_vnode_wal(dir: impl AsRef<Path>) -> Result<VnodeWal> {
        let dir = dir.as_ref();
        let owner = make_owner("cnosdb", "test_db");
        let owner = Arc::new(owner);
        let wal_option = crate::kv_option::WalOptions {
            enabled: true,
            path: dir.to_path_buf(),
            wal_req_channel_cap: 1024,
            max_file_size: 1024 * 1024,
            flush_trigger_total_file_size: 128,
            sync: false,
            sync_interval: std::time::Duration::from_secs(3600),
        };

        VnodeWal::new(Arc::new(wal_option), owner, 1234).await
    }

    #[tokio::test]
    async fn test_raft_wal_entry_storage() {
        trace::debug!("----------------------------------------");
        let dir = PathBuf::from("/tmp/test/wal/raft_entry");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let wal = get_vnode_wal(dir).await.unwrap();
        let mut storage = RaftEntryStorage::new(wal);

        for i in 0..10 {
            let mut entry = RaftEntry::default();
            entry.log_id.index = i;
            entry.payload = EntryPayload::Normal(format!("payload_{}", i).as_bytes().to_vec());
            storage.append(&[entry]).await.unwrap();
        }
        for i in 0..12 {
            let entry = storage.entry(i).await.unwrap();
            if i < 10 {
                assert_eq!(i, entry.unwrap().log_id.index)
            } else {
                assert_eq!(None, entry)
            }
        }

        storage.del_after(8).await.unwrap();
        storage.del_before(3).await.unwrap();
        for i in 0..12 {
            let entry = storage.entry(i).await.unwrap();
            if (3..8).contains(&i) {
                assert_eq!(i, entry.unwrap().log_id.index)
            } else {
                assert_eq!(None, entry)
            }
        }

        for i in 8..10 {
            let mut entry = RaftEntry::default();
            entry.log_id.index = i;
            entry.payload = EntryPayload::Normal(format!("payload_{}", i).as_bytes().to_vec());
            storage.append(&[entry]).await.unwrap();
        }

        for i in 0..12 {
            let entry = storage.entry(i).await.unwrap();
            if (3..10).contains(&i) {
                assert_eq!(i, entry.unwrap().log_id.index)
            } else {
                assert_eq!(None, entry)
            }
        }
    }

    pub async fn get_node_store(dir: impl AsRef<Path>) -> Arc<NodeStorage> {
        trace::debug!("----------------------------------------");
        let dir = dir.as_ref();
        let wal = get_vnode_wal(dir).await.unwrap();
        let entry = RaftEntryStorage::new(wal);
        let entry: EntryStorageRef = Arc::new(RwLock::new(entry));

        let size = 1024 * 1024 * 1024;
        let state = StateStorage::open(dir.join("state"), size).unwrap();
        let engine = HeedApplyStorage::open(dir.join("engine"), size).unwrap();

        let state = Arc::new(state);
        let engine: ApplyStorageRef = Arc::new(RwLock::new(engine));

        let info = RaftNodeInfo {
            group_id: 2222,
            address: "127.0.0.1:12345".to_string(),
        };

        let storage = NodeStorage::open(1000, info, state, engine, entry).unwrap();

        Arc::new(storage)
    }

    #[test]
    fn test_wal_raft_storage_with_openraft_cases() {
        let dir = PathBuf::from("/tmp/test/wal/raft/1");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        trace::init_default_global_tracing(
            &dir,
            "test_wal_raft_storage_with_openraft_cases",
            "debug",
        );

        let case_id = AtomicUsize::new(0);
        if let Err(e) = openraft::testing::Suite::test_all(|| {
            let id = case_id.fetch_add(1, atomic::Ordering::Relaxed);
            get_node_store(dir.join(id.to_string()))
        }) {
            trace::error!("{e}");
            panic!("{e:?}");
        }
    }
}
