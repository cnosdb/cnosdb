use std::fs;
use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;
use heed::types::*;
use heed::{Database, Env};
use openraft::Entry;

use crate::errors::ReplicationResult;
use crate::TypeConfig;

#[async_trait]
pub trait EntryStorage: Send + Sync {
    // Get the entry by index
    async fn entry(&self, index: u64) -> ReplicationResult<Option<Entry<TypeConfig>>>;

    // Delete entries: from begin to index
    async fn del_before(&self, index: u64) -> ReplicationResult<()>; // [0, index)

    // Delete entries: from index to end
    async fn del_after(&self, index: u64) -> ReplicationResult<()>; // [index, ...)

    // Write entries
    async fn append(&self, ents: &[Entry<TypeConfig>]) -> ReplicationResult<()>;

    // Get the last entry
    async fn last_entry(&self) -> ReplicationResult<Option<Entry<TypeConfig>>>;

    // Get entries from begin to end
    async fn entries(&self, begin: u64, end: u64) -> ReplicationResult<Vec<Entry<TypeConfig>>>; // [begin, end)
}

pub type EntryStorageRef = Arc<dyn EntryStorage>;

// --------------------------------------------------------------------------- //
pub struct HeedEntryStorage {
    env: Env,
    db: Database<OwnedType<u64>, OwnedSlice<u8>>,
}

impl HeedEntryStorage {
    pub fn open(path: impl AsRef<Path>) -> ReplicationResult<Self> {
        fs::create_dir_all(&path)?;

        let env = heed::EnvOpenOptions::new()
            .map_size(1024 * 1024 * 1024)
            .max_dbs(128)
            .open(path)?;

        let db: Database<OwnedType<u64>, OwnedSlice<u8>> = env.create_database(Some("entries"))?;
        let storage = Self { env, db };

        Ok(storage)
    }

    fn clear(&self) -> ReplicationResult<()> {
        let mut writer = self.env.write_txn()?;
        self.db.clear(&mut writer)?;
        writer.commit()?;

        Ok(())
    }
}

#[async_trait]
impl EntryStorage for HeedEntryStorage {
    async fn append(&self, ents: &[Entry<TypeConfig>]) -> ReplicationResult<()> {
        if ents.is_empty() {
            return Ok(());
        }

        // Remove all entries overwritten by `ents`.
        let begin = ents[0].log_id.index;
        self.del_after(begin).await?;

        let mut writer = self.env.write_txn()?;
        for entry in ents {
            let index = entry.log_id.index;

            let data = bincode::serialize(entry)?;
            self.db.put(&mut writer, &index, &data)?;
        }
        writer.commit()?;

        Ok(())
    }

    async fn last_entry(&self) -> ReplicationResult<Option<Entry<TypeConfig>>> {
        let reader = self.env.read_txn()?;
        if let Some((_, data)) = self.db.last(&reader)? {
            let entry = bincode::deserialize::<Entry<TypeConfig>>(&data)?;
            Ok(Some(entry))
        } else {
            Ok(None)
        }
    }

    async fn entry(&self, index: u64) -> ReplicationResult<Option<Entry<TypeConfig>>> {
        let reader = self.env.read_txn()?;
        if let Some(data) = self.db.get(&reader, &index)? {
            let entry = bincode::deserialize::<Entry<TypeConfig>>(&data)?;
            Ok(Some(entry))
        } else {
            Ok(None)
        }
    }

    async fn entries(&self, low: u64, high: u64) -> ReplicationResult<Vec<Entry<TypeConfig>>> {
        let mut ents = vec![];

        let reader = self.env.read_txn()?;
        let iter = self.db.range(&reader, &(low..high))?;
        for pair in iter {
            let (_, data) = pair?;
            ents.push(bincode::deserialize::<Entry<TypeConfig>>(&data)?);
        }

        Ok(ents)
    }

    async fn del_after(&self, index: u64) -> ReplicationResult<()> {
        let mut writer = self.env.write_txn()?;
        self.db.delete_range(&mut writer, &(index..))?;
        writer.commit()?;

        Ok(())
    }

    async fn del_before(&self, index: u64) -> ReplicationResult<()> {
        let mut writer = self.env.write_txn()?;
        self.db.delete_range(&mut writer, &(..index))?;
        writer.commit()?;

        Ok(())
    }
}
