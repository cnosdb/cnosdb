use std::io::IoSlice;
use std::path::{Path, PathBuf};

use num_traits::ToPrimitive;
use snafu::ResultExt;

use super::{
    file_crc_source_len, reader, FILE_FOOTER_LEN, FILE_MAGIC_NUMBER, FILE_MAGIC_NUMBER_LEN,
    RECORD_MAGIC_NUMBER,
};
use crate::error::{
    self, IOSnafu, InvalidParamSnafu, ReadFileSnafu, TskvError, TskvResult, WriteFileSnafu,
};
use crate::file_system::async_filesystem::{LocalFileSystem, LocalFileType};
use crate::file_system::file::stream_writer::FileStreamWriter;
use crate::file_system::FileSystem;

pub struct Writer {
    path: PathBuf,
    file: Box<FileStreamWriter>,
    footer: Option<[u8; FILE_FOOTER_LEN]>,
}

impl Writer {
    pub async fn open(path: impl AsRef<Path>, buf_size: usize) -> TskvResult<Self> {
        let path = path.as_ref();
        let file_system = LocalFileSystem::new(LocalFileType::ThreadPool);
        let mut file = file_system
            .open_file_writer(path, buf_size)
            .await
            .map_err(|e| TskvError::FileSystemError { source: e })?;
        if file.is_empty() {
            // For new file, write file magic number, next write is at 4.
            file.write(&FILE_MAGIC_NUMBER.to_be_bytes())
                .await
                .context(error::IOSnafu)?;
            Ok(Writer {
                path: path.to_path_buf(),
                file,
                footer: None,
            })
        } else {
            let footer = match reader::read_footer(&path).await {
                Ok((_, f)) => Some(f),
                Err(TskvError::NoFooter) => None,
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
                .checked_sub(footer_len)
                .unwrap_or(FILE_MAGIC_NUMBER_LEN);
            file.truncate(file_size).await.context(IOSnafu)?;

            Ok(Writer {
                path: path.to_path_buf(),
                file,
                footer,
            })
        }
    }

    // Writes record data and returns the written data size.
    pub async fn write_record<R, D>(
        &mut self,
        data_version: u8,
        data_type: u8,
        data: R,
    ) -> TskvResult<usize>
    where
        D: AsRef<[u8]>,
        R: AsRef<[D]>,
    {
        let data = data.as_ref();
        let data_len: usize = data.iter().map(|d| d.as_ref().len()).sum();
        let data_len = match data_len.to_u32() {
            Some(v) => v,
            None => {
                return Err(InvalidParamSnafu {
                    reason: format!(
                        "record(type: {}) length ({}) is not a valid u32, ignore this record",
                        data_type,
                        data.len()
                    ),
                }
                .build());
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
            .write_vec(&write_buf)
            .await
            .context(WriteFileSnafu {
                path: self.path.clone(),
            })?;
        Ok(written_size)
    }

    #[allow(dead_code)]
    pub async fn write_footer(&mut self, footer: &mut [u8; FILE_FOOTER_LEN]) -> TskvResult<usize> {
        self.sync().await?;

        // Get file crc
        let mut buf = vec![0_u8; file_crc_source_len(self.file.len(), 0)];
        let file_system = LocalFileSystem::new(LocalFileType::ThreadPool);
        let file = file_system
            .open_file_reader(&self.path)
            .await
            .map_err(|e| TskvError::FileSystemError { source: e })?;
        file.read_at(FILE_MAGIC_NUMBER_LEN, &mut buf)
            .await
            .context(ReadFileSnafu {
                path: self.path.clone(),
            })?;
        let crc = crc32fast::hash(&buf);

        // Set file crc to footer
        footer[4..8].copy_from_slice(&crc.to_be_bytes());
        self.footer = Some(*footer);

        let written_size = self.file.write(footer).await.context(WriteFileSnafu {
            path: self.path.clone(),
        })?;
        // Only add file_size, does not add pos
        Ok(written_size)
    }

    pub async fn truncate(&mut self, size: u64) -> TskvResult<()> {
        self.file.truncate(size as usize).await.context(IOSnafu)?;

        Ok(())
    }

    pub async fn sync(&mut self) -> TskvResult<()> {
        self.file.flush().await.context(error::SyncFileSnafu)
    }

    pub async fn close(&mut self) -> TskvResult<()> {
        self.sync().await
    }

    pub async fn new_reader(&mut self) -> TskvResult<reader::Reader> {
        self.file.flush().await.context(error::SyncFileSnafu)?;
        if let Some(r) = self.file.shared_file() {
            let len = r.len();
            return Ok(reader::Reader::new(
                r,
                self.path.clone(),
                len as u64,
                self.footer,
            ));
        }
        Err(InvalidParamSnafu {
            reason: "file is not shared file reader".to_string(),
        }
        .build())
    }

    pub fn path(&self) -> PathBuf {
        self.path.clone()
    }

    #[allow(dead_code)]
    pub fn footer(&self) -> Option<[u8; FILE_FOOTER_LEN]> {
        self.footer
    }

    pub fn file_size(&self) -> u64 {
        self.file.len() as u64
    }
}

#[cfg(test)]
pub(crate) mod test {
    use std::path::PathBuf;

    use super::Writer;
    use crate::record_file::reader::test::assert_record_file_data_eq;
    use crate::record_file::{
        FILE_FOOTER_LEN, RECORD_CRC32_NUMBER_LEN, RECORD_DATA_SIZE_LEN, RECORD_DATA_TYPE_LEN,
        RECORD_DATA_VERSION_LEN, RECORD_MAGIC_NUMBER_LEN,
    };

    const TEST_SUMMARY_BUFFER_SIZE: usize = 1024 * 1024;

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

            let mut writer = Writer::open(&path, TEST_SUMMARY_BUFFER_SIZE).await.unwrap();
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

            let mut writer = Writer::open(&path, TEST_SUMMARY_BUFFER_SIZE).await.unwrap();
            for data in records.iter() {
                let data_size = writer.write_record(1, 1, data).await.unwrap();
                assert_eq!(data_size, record_length(11));
            }
            writer.close().await.unwrap();
            assert_record_file_data_eq(&path, &records, false).await;
        }
    }
}
