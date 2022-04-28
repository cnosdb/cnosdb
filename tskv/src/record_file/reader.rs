use async_recursion::async_recursion;
use num_traits::ToPrimitive;
use std::borrow::Borrow;
use std::fs;
use std::hash::Hasher;
use std::io::Read;
use std::path::PathBuf;

use super::*;

pub struct Reader {
    path: PathBuf,
    file: direct_io::File,
    buf: Vec<u8>,
    pos: usize,
    buf_len: usize,
    buf_use: usize,
}

impl Reader {
    pub fn new(path: &PathBuf) -> Self {
        let file = open_file(path).unwrap();
        let mut buf = Vec::<u8>::new();
        buf.resize(READER_BUF_SIZE, 0);
        Reader {
            path: path.clone(),
            file,
            buf,
            pos: 0,
            buf_len: 0,
            buf_use: 0,
        }
    }

    async fn set_pos(&mut self, pos: usize) -> LogFileResult<()> {
        if pos > self.file.len().to_usize().unwrap() {
            return Err(LogFileError::InvalidPos);
        }

        return if self.pos > pos {
            let size = self.pos - pos;
            self.pos = pos;
            if self.buf_use > size {
                self.buf_use -= size;
                Ok(())
            } else {
                self.load_buf().await
            }
        } else if self.pos < pos {
            let size = pos - self.pos;
            self.pos = pos;
            if self.buf_len - self.buf_use > size {
                self.buf_use += size;
                Ok(())
            } else {
                self.load_buf().await
            }
        } else {
            Ok(())
        };
    }

    async fn find_magic(&mut self) -> LogFileResult<()> {
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
    pub async fn read_record(&mut self) -> LogFileResult<Record> {
        let (origin_pos, buf) = self
            .read_buf(
                RECORD_MAGIC_NUMBER_LEN
                    + RECORD_DATA_SIZE_LEN
                    + RECORD_DATA_VERSION_LEN
                    + RECORD_DATA_TYPE_LEN,
            )
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
        let data_size = u16::from_le_bytes(buf[p..p + RECORD_DATA_SIZE_LEN].try_into().unwrap());
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
            }
        };

        let (_, crc32_number_buf) = self.read_buf(RECORD_CRC32_NUMBER_LEN).await?;
        let crc32_number = u32::from_le_bytes(crc32_number_buf.try_into().unwrap());

        //check crc32 number
        let mut hasher = crc32fast::Hasher::new();
        hasher.write(&mut buf[RECORD_MAGIC_NUMBER_LEN..].borrow());
        hasher.write(&mut data.borrow());
        if hasher.finalize() != crc32_number {
            self.set_pos(origin_pos + 1).await?;
            self.find_magic().await?;
            return self.read_record().await;
        }

        Ok(Record { data_type, data_version, data })
    }

    async fn load_buf(&mut self) -> LogFileResult<()> {
        self.buf_len = self
            .file
            .read_at(self.pos.to_u64().unwrap(), &mut self.buf)
            .map_err(|err| LogFileError::ReadFile { source: err })?;
        self.buf_use = 0;
        Ok(())
    }

    // result: origin_pos, data
    async fn read_buf(&mut self, size: usize) -> LogFileResult<(usize, Vec<u8>)> {
        if self.buf_len - self.buf_use < size {
            self.load_buf().await?;
        }

        return if self.buf_len - self.buf_use >= size {
            let origin_pos = self.pos;
            let data = self.buf[self.buf_use..self.buf_use + size].to_vec();

            self.pos += size;
            self.buf_use += size;

            Ok((origin_pos, data))
        } else {
            Err(LogFileError::EOF)
        };
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

    loop {
        match r.read_record().await {
            Ok(record) => {
                println!("{}, {}, {:?}", record.data_type, record.data_version, record.data);
            }
            Err(_) => {
                break;
            }
        }
    }
}
