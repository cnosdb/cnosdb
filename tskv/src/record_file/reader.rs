use std::{
    borrow::Borrow,
    cmp::Ordering,
    fs,
    hash::Hasher,
    io::Read,
    path::{Path, PathBuf},
};

use async_recursion::async_recursion;
use bytes::{Buf, BufMut};
use direct_io::File;
use futures::future::ok;
use logger::info;
use num_traits::ToPrimitive;
use parking_lot::Mutex;

use super::*;

pub struct Reader {
    path: PathBuf,
    file: Mutex<File>,
    buf: Vec<u8>,
    pos: usize,
    buf_len: usize,
    buf_use: usize,
}

impl Reader {
    pub fn new(path: &Path) -> Self {
        let file = open_file(path).unwrap();
        let mut buf = Vec::<u8>::new();
        buf.resize(READER_BUF_SIZE, 0);
        Reader { path: path.to_path_buf(),
                 file: Mutex::new(file),
                 buf,
                 pos: 0,
                 buf_len: 0,
                 buf_use: 0 }
    }

    async fn set_pos(&mut self, pos: usize) -> RecordFileResult<()> {
        if pos > self.file.lock().len().to_usize().unwrap() {
            return Err(RecordFileError::InvalidPos);
        }
        match self.pos.cmp(&pos) {
            Ordering::Greater => {
                let size = self.pos - pos;
                self.pos = pos;
                match self.buf_use.cmp(&size) {
                    Ordering::Greater => {
                        self.buf_use -= size;
                        Ok(())
                    },
                    _ => self.load_buf().await,
                }
            },
            Ordering::Less => {
                let size = pos - self.pos;
                self.pos = pos;
                match (self.buf_len - self.buf_use).cmp(&size) {
                    Ordering::Greater => {
                        self.buf_use += size;
                        Ok(())
                    },
                    _ => self.load_buf().await,
                }
            },
            Ordering::Equal => Ok(()),
        }
    }

    async fn find_magic(&mut self) -> RecordFileResult<()> {
        loop {
            let (origin_pos, data) = self.read_buf(RECORD_MAGIC_NUMBER_LEN).await?;
            let magic_number = u32::from_le_bytes(data.try_into().unwrap());
            if magic_number != MAGIC_NUMBER {
                self.set_pos(origin_pos + 1).await?;
            } else {
                self.set_pos(origin_pos).await?;
                return Ok(());
            }
        }
    }

    // Result<data_type, data_version, data>, if Result is err, it means EOF.
    #[async_recursion]
    pub async fn read_record(&mut self) -> RecordFileResult<Record> {
        let (origin_pos, buf) = self.read_buf(RECORD_MAGIC_NUMBER_LEN
                                              + RECORD_DATA_SIZE_LEN
                                              + RECORD_DATA_VERSION_LEN
                                              + RECORD_DATA_TYPE_LEN)
                                    .await?;

        let mut p = 0;
        let magic_number =
            u32::from_le_bytes(buf[p..p + RECORD_MAGIC_NUMBER_LEN].try_into().unwrap());
        if magic_number != MAGIC_NUMBER {
            self.set_pos(origin_pos).await?;
            self.find_magic().await?;
            return self.read_record().await;
        }

        p += RECORD_MAGIC_NUMBER_LEN;
        let data_size = u32::from_le_bytes(buf[p..p + RECORD_DATA_SIZE_LEN].try_into().unwrap());
        p += RECORD_DATA_SIZE_LEN;
        let data_version =
            u8::from_le_bytes(buf[p..p + RECORD_DATA_VERSION_LEN].try_into().unwrap());
        p += RECORD_DATA_VERSION_LEN;
        let data_type = u8::from_le_bytes(buf[p..p + RECORD_DATA_TYPE_LEN].try_into().unwrap());

        let data = match self.read_buf(data_size.to_usize().unwrap()).await {
            Ok((_, data)) => data,
            Err(_) => {
                self.set_pos(origin_pos + 1).await?;
                self.find_magic().await?;
                return self.read_record().await;
            },
        };
        let (_, crc32_number_buf) = self.read_buf(RECORD_CRC32_NUMBER_LEN).await?;
        let crc32_number = u32::from_le_bytes(crc32_number_buf.try_into().unwrap());

        // check crc32 number
        let mut hasher = crc32fast::Hasher::new();
        hasher.write(buf[RECORD_MAGIC_NUMBER_LEN..].borrow());
        hasher.write(data.borrow());
        if hasher.finalize() != crc32_number {
            self.set_pos(origin_pos + 1).await?;
            self.find_magic().await?;
            return self.read_record().await;
        }

        Ok(Record { pos: origin_pos.to_u64().unwrap(), data_type, data_version, data })
    }

    async fn load_buf(&mut self) -> RecordFileResult<()> {
        self.buf_len = self.file
                           .lock()
                           .read_at(self.pos.to_u64().unwrap(), &mut self.buf)
                           .map_err(|err| RecordFileError::ReadFile { source: err })?;
        self.buf_use = 0;
        Ok(())
    }

    // result: origin_pos, data
    async fn read_buf(&mut self, size: usize) -> RecordFileResult<(usize, Vec<u8>)> {
        if self.buf_len - self.buf_use < size {
            self.load_buf().await?;
        }

        if self.buf_len - self.buf_use >= size {
            let origin_pos = self.pos;
            let data = self.buf[self.buf_use..self.buf_use + size].to_vec();

            self.pos += size;
            self.buf_use += size;

            Ok((origin_pos, data))
        } else {
            Err(RecordFileError::Eof)
        }
    }

    pub async fn read_one(&self, pos: usize) -> RecordFileResult<Record> {
        let mut head_buf = Vec::<u8>::new();
        let head_len = RECORD_MAGIC_NUMBER_LEN
                       + RECORD_DATA_SIZE_LEN
                       + RECORD_DATA_VERSION_LEN
                       + RECORD_DATA_TYPE_LEN;
        head_buf.resize(head_len, 0);
        let len = self.file
                      .lock()
                      .read_at(pos.to_u64().unwrap(), &mut head_buf)
                      .map_err(|err| RecordFileError::ReadFile { source: err })?;
        if len != head_len {
            return Err(RecordFileError::InvalidPos);
        }

        let mut p = 0;
        let magic_number =
            u32::from_le_bytes(head_buf[p..p + RECORD_MAGIC_NUMBER_LEN].try_into().unwrap());
        if magic_number != MAGIC_NUMBER {
            return Err(RecordFileError::InvalidPos);
        }
        p += RECORD_MAGIC_NUMBER_LEN;
        let data_size =
            u32::from_le_bytes(head_buf[p..p + RECORD_DATA_SIZE_LEN].try_into().unwrap());
        p += RECORD_DATA_SIZE_LEN;
        let data_version =
            u8::from_le_bytes(head_buf[p..p + RECORD_DATA_VERSION_LEN].try_into().unwrap());
        p += RECORD_DATA_VERSION_LEN;
        let data_type =
            u8::from_le_bytes(head_buf[p..p + RECORD_DATA_TYPE_LEN].try_into().unwrap());

        let mut data = Vec::<u8>::new();
        data.resize(data_size.to_usize().unwrap(), 0);
        let read_data_len =
            self.file
                .lock()
                .read_at(pos.to_u64().unwrap() + head_len.to_u64().unwrap(), &mut data)
                .map_err(|err| RecordFileError::ReadFile { source: err })?;
        if read_data_len != data_size as usize {
            return Err(RecordFileError::InvalidPos);
        }

        Ok(Record { pos: pos.to_u64().unwrap(), data_type, data_version, data })
    }
}

impl From<&str> for Reader {
    fn from(path: &str) -> Self {
        Reader::new(&PathBuf::from(path))
    }
}

#[tokio::test]
async fn test_reader() {
    let mut r = Reader::from("/tmp/test.log_file");

    while let Ok(record) = r.read_record().await {
        println!("{}, {}, {}, {:?}",
                 record.pos, record.data_type, record.data_version, record.data);
    }
}
