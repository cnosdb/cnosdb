use std::io::{ErrorKind, IoSlice};
use std::{
    io::{Error, Result, SeekFrom},
    ops::Deref,
};

use crate::file_system::file::async_file::{AsyncFile, IFile};

pub struct FileCursor {
    file: AsyncFile,
    pos: u64,
}

impl FileCursor {
    pub fn into_file(self) -> AsyncFile {
        self.file
    }

    pub fn pos(&self) -> u64 {
        self.pos
    }

    pub fn set_pos(&mut self, pos: u64) {
        self.pos = pos;
    }

    pub async fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        let read = self.file.read_at(self.pos, buf).await?;
        self.seek(SeekFrom::Current(read.try_into().unwrap()))
            .unwrap();
        Ok(read)
    }

    pub async fn write(&mut self, buf: &[u8]) -> Result<usize> {
        let size = self.file.write_at(self.pos, buf).await?;
        self.seek(SeekFrom::Current(buf.len().try_into().unwrap()))
            .unwrap();
        Ok(size)
    }

    pub async fn write_vec<'a>(&mut self, bufs: &'a mut [IoSlice<'a>]) -> Result<usize> {
        let mut p = self.pos;
        for buf in bufs {
            p += self.write_at(p, buf.deref()).await? as u64;
        }
        let pos = self.pos;
        self.seek(SeekFrom::Start(p.try_into().unwrap())).unwrap();
        Ok((p - pos) as usize)
    }

    pub fn seek(&mut self, pos: SeekFrom) -> Result<u64> {
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
}

impl From<AsyncFile> for FileCursor {
    fn from(file: AsyncFile) -> Self {
        FileCursor { file, pos: 0 }
    }
}

impl Deref for FileCursor {
    type Target = AsyncFile;

    fn deref(&self) -> &Self::Target {
        &self.file
    }
}
