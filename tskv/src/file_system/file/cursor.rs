use std::io::{Error, ErrorKind, IoSlice, Result, SeekFrom};

use config::FILE_BUFFER_SIZE;

use crate::file_system::file::async_file::AsyncFile;
use crate::file_system::file::IFile;

const DEFAULT_FILE_BUFFER_SIZE: usize = 1024 * 1024;

pub struct FileCursor {
    file: AsyncFile,
    pos: u64,
    buf: Vec<u8>,
}

impl FileCursor {
    pub fn file_ref(&self) -> &AsyncFile {
        &self.file
    }

    pub fn pos(&self) -> u64 {
        self.pos + self.buf.len() as u64
    }

    fn set_pos(&mut self, pos: u64) {
        self.buf.clear();
        self.pos = pos;
    }

    pub async fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        let read = self.file.read_at(self.pos, buf).await?;
        self.pos += read as u64;
        Ok(read)
    }

    pub async fn write(&mut self, buf: &[u8]) -> Result<usize> {
        self.buf.extend_from_slice(buf);
        self.try_flush(*FILE_BUFFER_SIZE.get_or_init(|| DEFAULT_FILE_BUFFER_SIZE))
            .await?;
        Ok(buf.len())
    }

    pub async fn write_vec<'a>(&mut self, bufs: &'a [IoSlice<'a>]) -> Result<usize> {
        let sum = bufs.iter().fold(0, |acc, buf| acc + buf.len());
        self.buf.reserve(sum);
        bufs.iter().for_each(|buf| self.buf.extend_from_slice(buf));
        self.try_flush(*FILE_BUFFER_SIZE.get_or_init(|| DEFAULT_FILE_BUFFER_SIZE))
            .await?;
        Ok(sum)
    }

    pub async fn try_flush(&mut self, buffer_size: usize) -> Result<()> {
        if self.buf.is_empty() || self.buf.len() < buffer_size {
            return Ok(());
        }
        let size = self.file.write_at(self.pos, &self.buf).await?;
        self.set_pos(self.pos + size as u64);
        Ok(())
    }

    pub async fn sync_data(&mut self) -> Result<()> {
        self.try_flush(0).await?;
        self.file.sync_data().await
    }

    pub async fn seek(&mut self, pos: SeekFrom) -> Result<u64> {
        self.try_flush(0).await?;
        self.pos = match pos {
            SeekFrom::Start(pos) => Some(pos),
            SeekFrom::End(delta) => {
                if delta >= 0 {
                    // TODO: AsyncFile::len() cannot get current length after
                    // an appending write.
                    self.len().checked_add(delta as u64)
                } else {
                    self.len().checked_sub(-delta as u64)
                }
            }
            SeekFrom::Current(delta) => {
                if delta >= 0 {
                    self.pos.checked_add(delta as u64)
                } else {
                    self.pos.checked_sub(-delta as u64)
                }
            }
        }
        .ok_or_else(|| Error::new(ErrorKind::InvalidInput, "underflow or overflow during seek"))?;
        Ok(self.pos)
    }

    pub fn len(&self) -> u64 {
        self.file.len()
    }
    pub fn is_empty(&self) -> bool {
        self.file.is_empty()
    }
}

impl From<AsyncFile> for FileCursor {
    fn from(file: AsyncFile) -> Self {
        FileCursor {
            file,
            pos: 0,
            buf: Vec::with_capacity(*FILE_BUFFER_SIZE.get_or_init(|| DEFAULT_FILE_BUFFER_SIZE)),
        }
    }
}
