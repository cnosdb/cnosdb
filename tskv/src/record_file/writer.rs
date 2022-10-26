use std::{
    borrow::Borrow,
    fs,
    path::{Path, PathBuf},
};

use num_traits::ToPrimitive;
use parking_lot::Mutex;
use snafu::ResultExt;
use trace::error;

use super::*;
use crate::file_system::file_manager;
use crate::file_system::{DmaFile, FileSync};
use crate::record_file::error::SyncFileSnafu;
use crate::record_file::error::WriteFileSnafu;

pub struct Writer {
    path: PathBuf,
    file: Mutex<DmaFile>,
}

impl Writer {
    pub fn new(path: &Path) -> Option<Self> {
        let file = match open_file(path) {
            Ok(v) => v,
            Err(e) => {
                error!("failed to open file path : {:?}, in case {:?}", path, e);
                return None;
            }
        };
        Some(Writer {
            path: path.to_path_buf(),
            file: Mutex::new(file),
        })
    }

    // Returns the POS of record in the file
    pub async fn write_record(
        &mut self,
        data_version: u8,
        data_type: u8,
        data: &[u8],
    ) -> RecordFileResult<u64> {
        let mut buf = Vec::<u8>::with_capacity(
            RECORD_MAGIC_NUMBER_LEN
                + RECORD_DATA_SIZE_LEN
                + RECORD_DATA_VERSION_LEN
                + RECORD_DATA_TYPE_LEN
                + data.len()
                + RECORD_CRC32_NUMBER_LEN,
        );

        let data_len = match data.len().to_u32() {
            None => {
                error!("failed to get data len, return 0");
                0
            }
            Some(v) => v,
        };

        // build buf
        buf.append(&mut MAGIC_NUMBER.to_le_bytes().to_vec()); // magic_number
        buf.append(&mut data_len.to_le_bytes().to_vec()); //data_size
        buf.append(&mut data_version.to_le_bytes().to_vec()); //data_version
        buf.append(&mut data_type.to_le_bytes().to_vec()); //data_type
        buf.append(&mut data.to_vec()); //data
        buf.append(
            &mut crc32fast::hash(buf[RECORD_MAGIC_NUMBER_LEN..].borrow())
                .to_le_bytes()
                .to_vec(),
        ); // crc32_number

        // write file
        let mut p = 0;
        let mut pos = self.file.lock().len();
        let origin_pos = pos;
        let pos_usize = match pos.to_usize() {
            None => {
                error!("pos is illegal");
                0
            }
            Some(v) => v,
        };
        while p < buf.len() {
            let mut write_len = BLOCK_SIZE - pos_usize % BLOCK_SIZE;
            if write_len > buf.len() - p {
                write_len = buf.len() - p;
            }

            self.file
                .lock()
                .write_at(pos, &buf[p..p + write_len])
                .map(|_| {
                    p += write_len;
                    pos += match write_len.to_u64() {
                        None => {
                            error!("write len is illegal");
                            0
                        }
                        Some(v) => v,
                    }
                })
                .context(WriteFileSnafu)?;
        }

        Ok(origin_pos)
    }

    pub async fn soft_sync(&self) -> RecordFileResult<()> {
        self.file
            .lock()
            .sync_all(FileSync::Soft)
            .context(SyncFileSnafu)
    }

    pub async fn hard_sync(&self) -> RecordFileResult<()> {
        self.file
            .lock()
            .sync_all(FileSync::Hard)
            .context(SyncFileSnafu)
    }

    pub async fn close(&mut self) -> RecordFileResult<()> {
        self.file
            .lock()
            .sync_all(FileSync::Hard)
            .context(SyncFileSnafu)
    }

    pub fn file_size(&self) -> u64 {
        self.file.lock().len()
    }

    pub fn path(&self) -> &PathBuf {
        &self.path
    }
}

impl From<&str> for Writer {
    fn from(path: &str) -> Self {
        Writer::new(&PathBuf::from(path)).unwrap()
    }
}

mod test {
    use crate::record_file::{Reader, RecordFileError, Writer};
    use serial_test::serial;

    #[tokio::test]
    #[serial]
    async fn test_writer() -> Result<(), RecordFileError> {
        let mut w = Writer::from("/tmp/test.log_file");
        for i in 0..10 {
            let pos = w.write_record(1, 1, &Vec::from("hello")).await?;
            println!("{}", pos);
        }
        w.close().await?;

        test_reader_read_one().await;
        test_reader().await;
        Ok(())
    }

    async fn test_reader_read_one() {
        let r = Reader::from("/tmp/test.log_file");
        let record = r.read_one(19).await.unwrap();
        println!(
            "{}, {}, {}, {:?}",
            record.pos, record.data_type, record.data_version, record.data
        );
    }

    async fn test_reader() {
        let mut r = Reader::from("/tmp/test.log_file");

        while let Ok(record) = r.read_record().await {
            println!(
                "{}, {}, {}, {:?}",
                record.pos, record.data_type, record.data_version, record.data
            );
        }
    }
}
