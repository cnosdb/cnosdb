use std::path::{Path, PathBuf};
use std::sync::Arc;

use models::meta_data::VnodeId;
use models::schema::{make_owner, Precision};
use serde::{Deserialize, Serialize};

use crate::file_system::file_manager;
use crate::kv_option::WalOptions;
use crate::record_file::{RecordDataType, RecordDataVersion};
use crate::wal::{raft, reader, WalType, WAL_FOOTER_MAGIC_NUMBER};
use crate::{record_file, Error, Result};

fn build_footer(min_sequence: u64, max_sequence: u64) -> [u8; record_file::FILE_FOOTER_LEN] {
    let mut footer = [0_u8; record_file::FILE_FOOTER_LEN];
    footer[0..4].copy_from_slice(&WAL_FOOTER_MAGIC_NUMBER.to_be_bytes());
    footer[16..24].copy_from_slice(&min_sequence.to_be_bytes());
    footer[24..32].copy_from_slice(&max_sequence.to_be_bytes());
    footer
}

pub struct WalWriter {
    id: u64,
    inner: record_file::Writer,
    size: u64,
    path: PathBuf,
    config: Arc<WalOptions>,

    buf: Vec<u8>,
    min_sequence: u64,
    max_sequence: u64,
}

impl WalWriter {
    /// Opens a wal file at path, returns a WalWriter with id and config.
    /// If wal file doesn't exist, create new wal file and set it's min_log_sequence(default 0).
    pub async fn open(
        config: Arc<WalOptions>,
        id: u64,
        path: impl AsRef<Path>,
        min_seq: u64,
    ) -> Result<Self> {
        let path = path.as_ref();

        // Use min_sequence existing in file, otherwise in parameter
        let (writer, min_sequence, max_sequence) = if file_manager::try_exists(path) {
            let writer = record_file::Writer::open(path, RecordDataType::Wal).await?;
            let (min_sequence, max_sequence) = match writer.footer() {
                Some(footer) => reader::parse_footer(footer).unwrap_or((min_seq, min_seq)),
                None => (min_seq, min_seq),
            };
            (writer, min_sequence, max_sequence)
        } else {
            (
                record_file::Writer::open(path, RecordDataType::Wal).await?,
                min_seq,
                min_seq,
            )
        };

        let size = writer.file_size();

        Ok(Self {
            id,
            inner: writer,
            size,
            path: PathBuf::from(path),
            config,
            buf: Vec::new(),
            min_sequence,
            max_sequence,
        })
    }

    /// Writes data, returns data sequence and data size.
    pub async fn write(
        &mut self,
        tenant: &str,
        vnode_id: VnodeId,
        precision: Precision,
        points: &[u8],
    ) -> Result<(u64, usize)> {
        let seq = self.max_sequence;
        let tenant_len = tenant.len() as u64;

        let written_size = self
            .inner
            .write_record(
                RecordDataVersion::V1 as u8,
                RecordDataType::Wal as u8,
                [
                    &[WalType::Write as u8][..],
                    &seq.to_be_bytes(),
                    &vnode_id.to_be_bytes(),
                    &(precision as u8).to_be_bytes(),
                    &tenant_len.to_be_bytes(),
                    tenant.as_bytes(),
                    points,
                ]
                .as_slice(),
            )
            .await?;

        if self.config.sync {
            self.inner.sync().await?;
        }
        // write & fsync succeed
        self.max_sequence += 1;
        self.size += written_size as u64;
        Ok((seq, written_size))
    }

    pub async fn delete_vnode(
        &mut self,
        tenant: &str,
        database: &str,
        vnode_id: VnodeId,
    ) -> Result<(u64, usize)> {
        let seq = self.max_sequence;
        let tenant_len = tenant.len() as u64;

        let written_size = self
            .inner
            .write_record(
                RecordDataVersion::V1 as u8,
                RecordDataType::Wal as u8,
                [
                    &[WalType::DeleteVnode as u8][..],
                    &seq.to_be_bytes(),
                    &vnode_id.to_be_bytes(),
                    &tenant_len.to_be_bytes(),
                    tenant.as_bytes(),
                    database.as_bytes(),
                ]
                .as_slice(),
            )
            .await?;

        if self.config.sync {
            self.inner.sync().await?;
        }
        // write & fsync succeed
        self.max_sequence += 1;
        self.size += written_size as u64;
        Ok((seq, written_size))
    }

    pub async fn delete_table(
        &mut self,
        tenant: &str,
        database: &str,
        table: &str,
    ) -> Result<(u64, usize)> {
        let seq = self.max_sequence;
        let tenant_len = tenant.len() as u64;
        let database_len = database.len() as u32;

        let written_size = self
            .inner
            .write_record(
                RecordDataVersion::V1 as u8,
                RecordDataType::Wal as u8,
                [
                    &[WalType::DeleteTable as u8][..],
                    &seq.to_be_bytes(),
                    &tenant_len.to_be_bytes(),
                    &database_len.to_be_bytes(),
                    tenant.as_bytes(),
                    database.as_bytes(),
                    table.as_bytes(),
                ]
                .as_slice(),
            )
            .await?;

        if self.config.sync {
            self.inner.sync().await?;
        }
        // write & fsync succeed
        self.max_sequence += 1;
        self.size += written_size as u64;
        Ok((seq, written_size))
    }

    pub async fn append_raft_entry(
        &mut self,
        raft_entry: &raft::RaftEntry,
    ) -> Result<(u64, usize)> {
        let wal_type = match raft_entry.payload {
            openraft::EntryPayload::Blank => WalType::RaftNormalLog,
            openraft::EntryPayload::Normal(_) => WalType::RaftNormalLog,
            openraft::EntryPayload::Membership(_) => WalType::RaftMembershipLog,
        };

        let seq = raft_entry.log_id.index;
        let raft_entry_bytes =
            bincode::serialize(raft_entry).map_err(|e| Error::Encode { source: e })?;

        let written_size = self
            .inner
            .write_record(
                RecordDataVersion::V1 as u8,
                RecordDataType::Wal as u8,
                [&[wal_type as u8][..], &seq.to_be_bytes(), &raft_entry_bytes].as_slice(),
            )
            .await?;

        if self.config.sync {
            self.inner.sync().await?;
        }
        // write & fsync succeed
        self.max_sequence = seq + 1;
        self.size += written_size as u64;
        Ok((seq, written_size))
    }

    pub async fn sync(&self) -> Result<()> {
        self.inner.sync().await
    }

    pub async fn close(&mut self) -> Result<usize> {
        trace::info!(
            "Closing wal with sequence: [{}, {})",
            self.min_sequence,
            self.max_sequence
        );
        let mut footer = build_footer(self.min_sequence, self.max_sequence);
        let size = self.inner.write_footer(&mut footer).await?;
        self.inner.close().await?;
        Ok(size)
    }

    pub fn id(&self) -> u64 {
        self.id
    }

    pub fn size(&self) -> u64 {
        self.size
    }

    pub fn min_sequence(&self) -> u64 {
        self.min_sequence
    }

    pub fn max_sequence(&self) -> u64 {
        self.max_sequence
    }

    pub fn set_max_sequence(&mut self, new_max_sequence: u64) {
        self.max_sequence = new_max_sequence
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]

pub enum Task {
    Write(WriteTask),
    DeleteVnode(DeleteVnodeTask),
    DeleteTable(DeleteTableTask),
}

impl Task {
    pub fn new_write(
        tenant: String,
        database: String,
        vnode_id: VnodeId,
        precision: Precision,
        points: Vec<u8>,
    ) -> Self {
        Self::Write(WriteTask {
            tenant,
            database,
            vnode_id,
            precision,
            points,
        })
    }

    pub fn new_delete_vnode(tenant: String, database: String, vnode_id: VnodeId) -> Self {
        Self::DeleteVnode(DeleteVnodeTask {
            tenant,
            database,
            vnode_id,
        })
    }

    pub fn new_delete_table(tenant: String, database: String, table: String) -> Self {
        Self::DeleteTable(DeleteTableTask {
            tenant,
            database,
            table,
        })
    }

    pub fn tenant_database(&self) -> String {
        match self {
            Self::Write(WriteTask {
                tenant, database, ..
            }) => make_owner(tenant, database),
            Self::DeleteVnode(DeleteVnodeTask {
                tenant, database, ..
            }) => make_owner(tenant, database),
            Self::DeleteTable(DeleteTableTask {
                tenant, database, ..
            }) => make_owner(tenant, database),
        }
    }

    pub fn vnode_id(&self) -> Option<VnodeId> {
        match self {
            Self::Write(WriteTask { vnode_id, .. }) => Some(*vnode_id),
            Self::DeleteVnode(DeleteVnodeTask { vnode_id, .. }) => Some(*vnode_id),
            //todo: change delete table to delete time series;
            Self::DeleteTable(DeleteTableTask { .. }) => None,
        }
    }
}

impl TryFrom<&reader::Block> for Task {
    type Error = crate::Error;

    fn try_from(b: &reader::Block) -> std::result::Result<Self, Self::Error> {
        match b {
            reader::Block::Write(blk) => {
                let task = WriteTask::try_from(blk)?;
                Ok(Self::Write(task))
            }
            reader::Block::DeleteTable(blk) => {
                let task = DeleteTableTask::try_from(blk)?;
                Ok(Self::DeleteTable(task))
            }
            _ => todo!(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WriteTask {
    pub tenant: String,
    pub database: String,
    pub vnode_id: VnodeId,
    pub precision: Precision,
    pub points: Vec<u8>,
}

impl TryFrom<&reader::WriteBlock> for WriteTask {
    type Error = crate::Error;

    fn try_from(b: &reader::WriteBlock) -> std::result::Result<Self, Self::Error> {
        Ok(Self {
            tenant: b.tenant_utf8()?.to_string(),
            database: String::new(),
            vnode_id: b.vnode_id(),
            precision: b.precision(),
            points: b.points().to_vec(),
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeleteVnodeTask {
    pub tenant: String,
    pub database: String,
    pub vnode_id: VnodeId,
}

impl TryFrom<&reader::DeleteVnodeBlock> for DeleteVnodeTask {
    type Error = crate::Error;

    fn try_from(b: &reader::DeleteVnodeBlock) -> std::result::Result<Self, Self::Error> {
        Ok(Self {
            tenant: b.tenant_utf8()?.to_string(),
            database: b.database_utf8()?.to_string(),
            vnode_id: b.vnode_id(),
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeleteTableTask {
    pub tenant: String,
    pub database: String,
    pub table: String,
}

impl TryFrom<&reader::DeleteTableBlock> for DeleteTableTask {
    type Error = crate::Error;

    fn try_from(b: &reader::DeleteTableBlock) -> std::result::Result<Self, Self::Error> {
        Ok(Self {
            tenant: b.tenant_utf8()?.to_string(),
            database: b.database_utf8()?.to_string(),
            table: b.table_utf8()?.to_string(),
        })
    }
}

#[cfg(test)]
mod test {
    use std::path::PathBuf;
    use std::sync::Arc;

    use models::schema::Precision;

    use crate::kv_option::WalOptions;
    use crate::wal::reader::{Block, DeleteTableBlock, DeleteVnodeBlock, WalReader, WriteBlock};
    use crate::wal::writer::WalWriter;
    use crate::Error;

    #[tokio::test]
    async fn test_write() {
        let dir = "/tmp/test/wal_writer/1";
        let _ = std::fs::remove_dir_all(dir);

        let mut global_config = config::get_config_for_test();
        global_config.wal.path = dir.to_string();
        let wal_config = Arc::new(WalOptions::from(&global_config));

        #[rustfmt::skip]
        let entries = vec![
            Block::Write(WriteBlock::build(
                1,  "cnosdb", 3, Precision::NS, vec![1, 2, 3],
            )),
            Block::DeleteVnode(DeleteVnodeBlock::build(2, "cnosdb", "public", 6)),
            Block::DeleteTable(DeleteTableBlock::build(3, "cnosdb", "public", "table")),
        ];

        let wal_path = PathBuf::from(dir).join("1.wal");
        let wal_path = {
            let mut writer = WalWriter::open(wal_config, 1, wal_path, 1).await.unwrap();
            for ent in entries.iter() {
                match ent {
                    Block::Write(d) => {
                        let tenant = String::from_utf8(d.tenant().to_vec()).unwrap();
                        writer
                            .write(&tenant, d.vnode_id(), d.precision(), d.points())
                            .await
                            .unwrap();
                    }
                    Block::DeleteVnode(d) => {
                        let tenant = String::from_utf8(d.tenant().to_vec()).unwrap();
                        let database = String::from_utf8(d.database().to_vec()).unwrap();
                        writer
                            .delete_vnode(&tenant, &database, d.vnode_id())
                            .await
                            .unwrap();
                    }
                    Block::DeleteTable(d) => {
                        let tenant = String::from_utf8(d.tenant().to_vec()).unwrap();
                        let database = String::from_utf8(d.database().to_vec()).unwrap();
                        let table = String::from_utf8(d.table().to_vec()).unwrap();
                        writer
                            .delete_table(&tenant, &database, &table)
                            .await
                            .unwrap();
                    }
                    Block::RaftLog(e) => {
                        writer.append_raft_entry(e).await.unwrap();
                    }
                    Block::Unknown => {
                        // ignore
                    }
                }
            }
            writer.path
        };

        let mut reader = WalReader::open(&wal_path).await.unwrap();
        let mut i = 0;
        loop {
            match reader.next_wal_entry().await {
                Ok(Some(blk)) => {
                    assert_eq!(blk.block, entries[i]);
                }
                Ok(None) | Err(Error::WalTruncated) => break,
                Err(e) => {
                    panic!("Failed reading from wal {}: {e}", wal_path.display());
                }
            }
            i += 1;
        }
    }
}
