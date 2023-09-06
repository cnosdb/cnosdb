use std::io::IoSlice;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use num_traits::ToPrimitive;
use snafu::ResultExt;

use super::{
    file_crc_source_len, reader, RecordDataType, FILE_FOOTER_LEN, FILE_MAGIC_NUMBER,
    FILE_MAGIC_NUMBER_LEN, RECORD_MAGIC_NUMBER,
};
use crate::error::{self, Error, Result};
use crate::file_system::file::async_file::AsyncFile;
use crate::file_system::file::IFile;
use crate::file_system::file_manager;

pub struct Writer {
    path: PathBuf,
    file: Arc<AsyncFile>,

    footer: Option<[u8; FILE_FOOTER_LEN]>,
    pos: u64,
    file_size: u64,
}

impl Writer {
    pub async fn open(path: impl AsRef<Path>, _data_type: RecordDataType) -> Result<Self> {
        let path = path.as_ref();
        let file = file_manager::open_create_file(path).await?;
        if file.is_empty() {
            // For new file, write file magic number, next write is at 4.
            let file_size = file
                .write_at(0, &FILE_MAGIC_NUMBER.to_be_bytes())
                .await
                .context(error::IOSnafu)? as u64;
            Ok(Writer {
                path: path.to_path_buf(),
                file: Arc::new(file),
                footer: None,
                pos: file_size,
                file_size,
            })
        } else {
            let footer = match reader::read_footer(&path).await {
                Ok((_, f)) => Some(f),
                Err(Error::NoFooter) => None,
                Err(e) => {
                    trace::error!(
                        "Failed to read footer of record_file '{}': {e}",
                        path.display()
                    );
                    return Err(e);
                }
            };

            // For wrote file, skip to footer position, next write is at (file.len() - footer_len).
            // Note that footer_len may be zero.
            let footer_len = footer.map(|f| f.len()).unwrap_or(0);
            // Truncate this file to overwrite footer data.
            let file_size = file
                .len()
                .checked_sub(footer_len as u64)
                .unwrap_or(FILE_MAGIC_NUMBER_LEN as u64);
            file.truncate(file_size).await?;

            Ok(Writer {
                path: path.to_path_buf(),
                file: Arc::new(file),
                footer,
                pos: file_size,
                file_size,
            })
        }
    }

    // Writes record data and returns the written data size.
    pub async fn write_record<R, D>(
        &mut self,
        data_version: u8,
        data_type: u8,
        data: R,
    ) -> Result<usize>
    where
        D: AsRef<[u8]>,
        R: AsRef<[D]>,
    {
        let data = data.as_ref();
        let data_len: usize = data.iter().map(|d| d.as_ref().len()).sum();
        let data_len = match data_len.to_u32() {
            Some(v) => v,
            None => {
                return Err(Error::InvalidParam {
                    reason: format!(
                        "record(type: {}) length ({}) is not a valid u32, ignore this record",
                        data_type,
                        data.len()
                    ),
                });
            }
        };

        // Build record header and hash.
        let mut hasher = crc32fast::Hasher::new();
        let data_meta = [data_version, data_type];
        hasher.update(&data_meta);
        let data_len = data_len.to_be_bytes();
        hasher.update(&data_len);
        for d in data.iter() {
            hasher.update(d.as_ref());
        }
        let data_crc = hasher.finalize().to_be_bytes();

        let magic_number = RECORD_MAGIC_NUMBER.to_be_bytes();
        let mut write_buf: Vec<IoSlice> = Vec::with_capacity(6);
        write_buf.push(IoSlice::new(&magic_number));
        write_buf.push(IoSlice::new(&data_meta));
        write_buf.push(IoSlice::new(&data_len));
        write_buf.push(IoSlice::new(&data_crc));
        for d in data {
            write_buf.push(IoSlice::new(d.as_ref()));
        }

        // Write record header and record data.
        let written_size = self
            .file
            .write_vec(self.pos, &mut write_buf)
            .await
            .map_err(|e| Error::WriteFile {
                path: self.path.clone(),
                source: e,
            })?;
        self.pos += written_size as u64;
        self.file_size += written_size as u64;
        Ok(written_size)
    }

    pub async fn write_footer(&mut self, footer: &mut [u8; FILE_FOOTER_LEN]) -> Result<usize> {
        self.sync().await?;

        // Get file crc
        let mut buf = vec![0_u8; file_crc_source_len(self.file_size(), 0_usize)];
        self.file
            .read_at(FILE_MAGIC_NUMBER_LEN as u64, &mut buf)
            .await
            .map_err(|e| Error::ReadFile {
                path: self.path.clone(),
                source: e,
            })?;
        let crc = crc32fast::hash(&buf);

        // Set file crc to footer
        footer[4..8].copy_from_slice(&crc.to_be_bytes());
        self.footer = Some(*footer);

        let written_size =
            self.file
                .write_at(self.pos, footer)
                .await
                .map_err(|e| Error::WriteFile {
                    path: self.path.clone(),
                    source: e,
                })?;
        // Only add file_size, does not add pos
        self.file_size += written_size as u64;
        Ok(written_size)
    }

    pub async fn sync(&self) -> Result<()> {
        self.file.sync_data().await.context(error::SyncFileSnafu)
    }

    pub async fn close(&mut self) -> Result<()> {
        self.sync().await
    }

    pub fn new_reader(&self) -> reader::Reader {
        reader::Reader::new(
            self.file.clone(),
            self.path.clone(),
            self.file_size,
            self.footer,
        )
    }

    pub fn path(&self) -> PathBuf {
        self.path.clone()
    }

    pub fn footer(&self) -> Option<[u8; FILE_FOOTER_LEN]> {
        self.footer
    }

    pub fn pos(&self) -> u64 {
        self.pos
    }

    pub fn file_size(&self) -> u64 {
        self.file_size
    }
}

#[cfg(test)]
pub(crate) mod test {
    use std::path::PathBuf;

    use super::Writer;
    use crate::record_file::reader::test::assert_record_file_data_eq;
    use crate::record_file::{
        RecordDataType, FILE_FOOTER_LEN, RECORD_CRC32_NUMBER_LEN, RECORD_DATA_SIZE_LEN,
        RECORD_DATA_TYPE_LEN, RECORD_DATA_VERSION_LEN, RECORD_MAGIC_NUMBER_LEN,
    };

    pub(crate) fn record_length(data_len: usize) -> usize {
        RECORD_MAGIC_NUMBER_LEN
            + RECORD_DATA_VERSION_LEN
            + RECORD_DATA_TYPE_LEN
            + RECORD_DATA_SIZE_LEN
            + RECORD_CRC32_NUMBER_LEN
            + data_len
    }

    #[tokio::test]
    async fn test_record_writer() {
        let dir = PathBuf::from("/tmp/test/record_file/writer/1");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let records = vec![[b"hello".to_vec(), b" ".to_vec(), b"world".to_vec()]; 10];
        let mut footer = [0_u8; FILE_FOOTER_LEN];
        {
            // Test write a record file with footer.
            let path = dir.join("has_footer.log");

            let mut writer = Writer::open(&path, RecordDataType::Summary).await.unwrap();
            for data in records.iter() {
                let data_size = writer.write_record(1, 1, data).await.unwrap();
                assert_eq!(data_size, record_length(11));
            }
            let footer_size = writer.write_footer(&mut footer).await.unwrap();
            assert_eq!(footer_size, FILE_FOOTER_LEN);
            writer.close().await.unwrap();
            assert_record_file_data_eq(&path, &records, true).await;
        }
        {
            // Test write a record file that has no footer.
            let path = dir.join("no_footer.log");

            let mut writer = Writer::open(&path, RecordDataType::Summary).await.unwrap();
            for data in records.iter() {
                let data_size = writer.write_record(1, 1, data).await.unwrap();
                assert_eq!(data_size, record_length(11));
            }
            writer.close().await.unwrap();
            assert_record_file_data_eq(&path, &records, false).await;
        }
    }
}
