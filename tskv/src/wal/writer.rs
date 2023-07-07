use std::path::{Path, PathBuf};
use std::sync::Arc;

use models::meta_data::VnodeId;
use models::schema::Precision;

use super::reader::WalReader;
use super::{WalEntryType, FOOTER_MAGIC_NUMBER};
use crate::file_system::file_manager;
use crate::kv_option::WalOptions;
use crate::record_file::{RecordDataType, RecordDataVersion};
use crate::{record_file, Result};

fn build_footer(min_sequence: u64, max_sequence: u64) -> [u8; record_file::FILE_FOOTER_LEN] {
    let mut footer = [0_u8; record_file::FILE_FOOTER_LEN];
    footer[0..4].copy_from_slice(&FOOTER_MAGIC_NUMBER.to_be_bytes());
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
                Some(footer) => WalReader::parse_footer(footer).unwrap_or((min_seq, min_seq)),
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
        tenant: String,
        vnode_id: VnodeId,
        precision: Precision,
        points: Vec<u8>,
    ) -> Result<(u64, usize)> {
        let seq = self.max_sequence;
        let tenant_len = tenant.len() as u64;

        let written_size = self
            .inner
            .write_record(
                RecordDataVersion::V1 as u8,
                RecordDataType::Wal as u8,
                [
                    &[WalEntryType::Write as u8][..],
                    &seq.to_be_bytes(),
                    &vnode_id.to_be_bytes(),
                    &(precision as u8).to_be_bytes(),
                    &tenant_len.to_be_bytes(),
                    tenant.as_bytes(),
                    &points,
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
        tenant: String,
        database: String,
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
                    &[WalEntryType::DeleteVnode as u8][..],
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
        tenant: String,
        database: String,
        table: String,
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
                    &[WalEntryType::DeleteTable as u8][..],
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

    pub async fn sync(&self) -> Result<()> {
        self.inner.sync().await
    }

    pub async fn close(mut self) -> Result<usize> {
        trace::info!(
            "Closing wal with sequence: [{}, {})",
            self.min_sequence,
            self.max_sequence
        );
        let footer = build_footer(self.min_sequence, self.max_sequence);
        let size = self.inner.write_footer(footer).await?;
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

#[cfg(test)]
mod test {
    use std::path::PathBuf;
    use std::sync::Arc;

    use models::schema::Precision;

    use crate::kv_option::WalOptions;
    use crate::wal::reader::{DeleteTableBlock, DeleteVnodeBlock, WalEntry, WalReader, WriteBlock};
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
            WalEntry::Write(WriteBlock::build(
                1,  "cnosdb", 3, Precision::NS, vec![1, 2, 3],
            )),
            WalEntry::DeleteVnode(DeleteVnodeBlock::build(2, "cnosdb", "public", 6)),
            WalEntry::DeleteTable(DeleteTableBlock::build(3, "cnosdb", "public", "table")),
        ];

        let wal_path = PathBuf::from(dir).join("1.wal");
        let wal_path = {
            let mut writer = WalWriter::open(wal_config, 1, wal_path, 1).await.unwrap();
            for ent in entries.iter() {
                match ent {
                    WalEntry::Write(d) => {
                        let tenant = String::from_utf8(d.tenant().to_vec()).unwrap();
                        writer
                            .write(tenant, d.vnode_id(), d.precision(), d.points().to_vec())
                            .await
                            .unwrap();
                    }
                    WalEntry::DeleteVnode(d) => {
                        let tenant = String::from_utf8(d.tenant().to_vec()).unwrap();
                        let database = String::from_utf8(d.database().to_vec()).unwrap();
                        writer
                            .delete_vnode(tenant, database, d.vnode_id())
                            .await
                            .unwrap();
                    }
                    WalEntry::DeleteTable(d) => {
                        let tenant = String::from_utf8(d.tenant().to_vec()).unwrap();
                        let database = String::from_utf8(d.database().to_vec()).unwrap();
                        let table = String::from_utf8(d.table().to_vec()).unwrap();
                        writer.delete_table(tenant, database, table).await.unwrap();
                    }
                    WalEntry::Unknown => {
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
                    assert_eq!(blk.entry, entries[i]);
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
