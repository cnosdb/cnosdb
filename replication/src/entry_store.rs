use std::fs;
use std::path::{Path, PathBuf};

use async_trait::async_trait;
use heed::byteorder::BigEndian;
use heed::types::*;
use heed::{Database, Env};
use openraft::Entry;
use snafu::ResultExt;

use crate::errors::{HeedSnafu, IOErrSnafu, MsgInvalidSnafu, ReplicationResult};
use crate::{EntriesMetrics, EntryStorage, TypeConfig};

// --------------------------------------------------------------------------- //
type BEU64 = U64<BigEndian>;
pub struct HeedEntryStorage {
    env: Env,
    path: PathBuf,
    db: Database<BEU64, Bytes>,
}

impl HeedEntryStorage {
    pub fn open(path: impl AsRef<Path>, size: usize) -> ReplicationResult<Self> {
        fs::create_dir_all(&path).context(IOErrSnafu)?;

        let path = path.as_ref().to_path_buf();
        let env = unsafe {
            heed::EnvOpenOptions::new()
                .map_size(size)
                .max_dbs(1)
                .open(path.clone())
                .context(HeedSnafu)?
        };

        let mut txn = env.write_txn().context(HeedSnafu)?;
        let db: Database<BEU64, Bytes> = env
            .create_database(&mut txn, Some("data"))
            .context(HeedSnafu)?;
        txn.commit().context(HeedSnafu)?;

        let storage = Self { env, db, path };

        Ok(storage)
    }

    async fn first_entry(&mut self) -> ReplicationResult<Option<Entry<TypeConfig>>> {
        let reader = self.env.read_txn().context(HeedSnafu)?;
        if let Some((_, data)) = self.db.first(&reader).context(HeedSnafu)? {
            let entry = bincode::deserialize::<Entry<TypeConfig>>(data)
                .map_err(|e| MsgInvalidSnafu { msg: e.to_string() }.build())?;
            Ok(Some(entry))
        } else {
            Ok(None)
        }
    }
}

#[async_trait]
impl EntryStorage for HeedEntryStorage {
    async fn append(&mut self, ents: &[Entry<TypeConfig>]) -> ReplicationResult<()> {
        if ents.is_empty() {
            return Ok(());
        }

        let mut writer = self.env.write_txn().context(HeedSnafu)?;
        for entry in ents {
            let index = entry.log_id.index;

            let data = bincode::serialize(entry)
                .map_err(|e| MsgInvalidSnafu { msg: e.to_string() }.build())?;
            self.db.put(&mut writer, &index, &data).context(HeedSnafu)?;
        }
        writer.commit().context(HeedSnafu)?;

        Ok(())
    }

    async fn last_entry(&mut self) -> ReplicationResult<Option<Entry<TypeConfig>>> {
        let reader = self.env.read_txn().context(HeedSnafu)?;
        if let Some((_, data)) = self.db.last(&reader).context(HeedSnafu)? {
            let entry = bincode::deserialize::<Entry<TypeConfig>>(data)
                .map_err(|e| MsgInvalidSnafu { msg: e.to_string() }.build())?;
            Ok(Some(entry))
        } else {
            Ok(None)
        }
    }

    async fn entry(&mut self, index: u64) -> ReplicationResult<Option<Entry<TypeConfig>>> {
        let reader = self.env.read_txn().context(HeedSnafu)?;
        if let Some(data) = self.db.get(&reader, &index).context(HeedSnafu)? {
            let entry = bincode::deserialize::<Entry<TypeConfig>>(data)
                .map_err(|e| MsgInvalidSnafu { msg: e.to_string() }.build())?;
            Ok(Some(entry))
        } else {
            Ok(None)
        }
    }

    async fn entries(&mut self, low: u64, high: u64) -> ReplicationResult<Vec<Entry<TypeConfig>>> {
        let mut ents = vec![];

        let reader = self.env.read_txn().context(HeedSnafu)?;
        let range = low..high;
        let iter = self.db.range(&reader, &range).context(HeedSnafu)?;
        for pair in iter {
            let (_, data) = pair.context(HeedSnafu)?;
            ents.push(
                bincode::deserialize::<Entry<TypeConfig>>(data)
                    .map_err(|e| MsgInvalidSnafu { msg: e.to_string() }.build())?,
            );
        }

        Ok(ents)
    }

    async fn del_after(&mut self, index: u64) -> ReplicationResult<()> {
        let mut writer = self.env.write_txn().context(HeedSnafu)?;
        let range = index..;
        self.db
            .delete_range(&mut writer, &range)
            .context(HeedSnafu)?;
        writer.commit().context(HeedSnafu)?;

        Ok(())
    }

    async fn del_before(&mut self, index: u64) -> ReplicationResult<()> {
        let mut writer = self.env.write_txn().context(HeedSnafu)?;
        let range = ..index;
        self.db
            .delete_range(&mut writer, &range)
            .context(HeedSnafu)?;
        writer.commit().context(HeedSnafu)?;

        Ok(())
    }

    async fn destroy(&mut self) -> ReplicationResult<()> {
        let _ = fs::remove_dir_all(&self.path);

        Ok(())
    }
    async fn metrics(&mut self) -> ReplicationResult<EntriesMetrics> {
        let first = self.first_entry().await?.unwrap_or_default();
        let last = self.last_entry().await?.unwrap_or_default();

        Ok(EntriesMetrics {
            min_seq: first.log_id.index,
            max_seq: last.log_id.index,
            avg_write_time: 0,
        })
    }

    async fn sync(&mut self) -> ReplicationResult<()> {
        Ok(())
    }
}

mod test {
    #[test]
    #[ignore]
    fn test_heed_range() {
        type BEU64 = heed::types::U64<heed::byteorder::BigEndian>;

        let path = "/tmp/cnosdb/8201-entry";
        let env = unsafe {
            heed::EnvOpenOptions::new()
                .map_size(1024 * 1024 * 1024)
                .max_dbs(128)
                .open(path)
                .unwrap()
        };

        let mut wtxn = env.write_txn().unwrap();
        let db: heed::Database<BEU64, heed::types::Unit> =
            env.create_database(&mut wtxn, Some("entries")).unwrap();

        let range = ..4;
        db.delete_range(&mut wtxn, &range).unwrap();
        wtxn.commit().unwrap();

        let reader = env.read_txn().unwrap();
        let range = 0..1000000;
        let iter = db.range(&reader, &range).unwrap();
        for pair in iter {
            let (index, _) = pair.unwrap();
            println!("--- {}", index);
        }
    }
}
