use std::io::{Error, ErrorKind, IoSlice, Result, SeekFrom};
use std::path::PathBuf;

use crate::file_system::file::async_file::AsyncFile;
use crate::file_system::file::stream_reader::FileStreamReader;
use crate::file_system::file::WritableFile;

#[derive(Debug)]
struct Buffer {
    data: Vec<u8>,
    buffer_size: usize,
}

impl Buffer {
    pub fn new(buffer_size: usize) -> Buffer {
        Buffer {
            data: Vec::with_capacity(buffer_size),
            buffer_size,
        }
    }
    // fn clear(&mut self) {
    //     self.data.clear();
    // }

    fn is_full(&self) -> bool {
        self.data.len() >= self.buffer_size
    }

    // fn len(&self) -> usize {
    //     self.data.len()
    // }

    fn extend_from_slice(&mut self, buf: &[u8]) {
        self.data.extend_from_slice(buf);
    }

    fn consume_data(&mut self) -> Vec<u8> {
        std::mem::take(&mut self.data)
    }

    fn reserve(&mut self, size: usize) {
        self.data.reserve(size);
    }
}

pub struct FileStreamWriter {
    file: Box<dyn WritableFile>,
    pos: usize,
    buf: Buffer,
    path: PathBuf,
}

impl FileStreamWriter {
    pub fn new(file: Box<dyn WritableFile>, path_buf: PathBuf, buf_size: usize) -> Self {
        let pos = file.file_size();
        Self {
            file,
            pos,
            buf: Buffer::new(buf_size),
            path: path_buf,
        }
    }

    fn set_pos(&mut self, pos: usize) {
        self.pos = pos;
    }

    pub async fn write(&mut self, buf: &[u8]) -> Result<()> {
        self.buf.extend_from_slice(buf);
        self.try_flush().await
    }

    pub async fn write_vec<'a>(&mut self, bufs: &'a [IoSlice<'a>]) -> Result<usize> {
        let sum = bufs.iter().fold(0, |acc, buf| acc + buf.len());
        self.buf.reserve(sum);
        bufs.iter().for_each(|buf| self.buf.extend_from_slice(buf));
        self.try_flush().await?;
        Ok(sum)
    }

    pub async fn try_flush(&mut self) -> Result<()> {
        if !self.buf.is_full() {
            return Ok(());
        }
        let data = self.buf.consume_data();
        self.file.write_at(self.pos, &data).await?;
        self.set_pos(self.pos + data.len());
        Ok(())
    }

    pub async fn flush(&mut self) -> Result<()> {
        let data = self.buf.consume_data();
        self.file.write_at(self.pos, &data).await?;
        self.set_pos(self.pos + data.len());
        self.file.sync_data().await?;
        Ok(())
    }

    pub async fn seek(&mut self, pos: SeekFrom) -> Result<usize> {
        self.flush().await?;
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

    pub async fn truncate(&mut self, size: usize) -> Result<()> {
        self.flush().await?;
        self.file.truncate(size as u64).await?;
        self.pos = size;
        Ok(())
    }

    pub fn len(&self) -> usize {
        self.file.file_size() + self.buf.data.len()
    }

    pub fn is_empty(&self) -> bool {
        self.file.is_empty()
    }

    pub fn path(&self) -> &PathBuf {
        &self.path
    }

    pub fn shared_file(&self) -> Option<Box<FileStreamReader>> {
        self.file.as_any().downcast_ref::<AsyncFile>().map(|file| {
            Box::new(FileStreamReader::new(
                Box::new(file.clone()),
                self.path.clone(),
            ))
        })
    }
}

#[cfg(test)]
mod test {
    use std::fs::OpenOptions;

    use crate::file_system::async_filesystem::LocalFileType;
    use crate::file_system::FileSystem;

    #[tokio::test]
    async fn test() {
        let mut opt = OpenOptions::new();
        opt.read(true).write(true).create(true).append(true);
        let file_system =
            crate::file_system::async_filesystem::LocalFileSystem::new(LocalFileType::ThreadPool);
        let mut wfile = file_system
            .open_file_writer("reader.txt", 1024)
            .await
            .unwrap();
        wfile.write(b"hello world").await.unwrap();
        wfile.flush().await.unwrap();
        let mut buf = vec![0_u8; 11];
        let rfile = wfile.shared_file().unwrap();
        let len = rfile.read_at(0, &mut buf).await.unwrap();
        assert_eq!(len, 11);
        assert_eq!(buf, b"hello world".to_vec());
    }
}
