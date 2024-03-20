use std::io::{Error, ErrorKind, Result, SeekFrom};

use crate::file_system::file::ReadableFile;

//todo: add buffer for read
pub struct FileStreamReader {
    file: Box<dyn ReadableFile>,
    pos: usize,
}

impl FileStreamReader {
    pub fn new(file: Box<dyn ReadableFile>) -> Self {
        Self { file, pos: 0 }
    }

    pub async fn read(&mut self, data: &mut [u8]) -> Result<usize> {
        let read_size = self.file.read_at(self.pos as u64, data).await?;
        self.pos += read_size;
        Ok(read_size)
    }
    pub fn seek(&mut self, pos: SeekFrom) -> Result<usize> {
        self.pos = match pos {
            SeekFrom::Start(pos) => Some(pos as usize),
            SeekFrom::End(delta) => {
                if delta >= 0 {
                    self.len().checked_add(delta as usize)
                } else {
                    self.len().checked_sub(-delta as usize)
                }
            }
            SeekFrom::Current(delta) => {
                if delta >= 0 {
                    self.pos.checked_add(delta as usize)
                } else {
                    self.pos.checked_sub(-delta as usize)
                }
            }
        }
        .ok_or_else(|| Error::new(ErrorKind::InvalidInput, "underflow or overflow during seek"))?;
        Ok(self.pos)
    }
    pub fn len(&self) -> usize {
        self.file.file_size()
    }
}
