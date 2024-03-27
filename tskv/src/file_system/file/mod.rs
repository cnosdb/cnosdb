pub(crate) mod async_file;
mod linux_aio_file;
pub(crate) mod mmap_file;
mod os;
mod raw_file;
pub mod stream_reader;
pub mod stream_writer;

use std::io::{Error, ErrorKind, Result};

use async_trait::async_trait;
use tokio::task::spawn_blocking;

#[async_trait]
pub trait ReadableFile: Send + Sync {
    async fn read_at(&self, pos: usize, data: &mut [u8]) -> Result<usize>;
    fn file_size(&self) -> usize;
}

#[async_trait]
pub trait WritableFile: Send + Sync {
    // async fn write_vec<'a>(&self, pos: u64, bufs: &'a mut [IoSlice<'a>]) -> Result<usize>{
    //     let mut p = pos;
    //     for buf in bufs {
    //         p += self.write_at(p, buf.deref()).await? as u64;
    //     }
    //     Ok((p - pos) as usize)
    // }
    async fn write_at(&mut self, pos: usize, data: &[u8]) -> Result<usize>;
    async fn sync_data(&self) -> Result<()>;

    async fn sync_all(&self) -> Result<()>;
    async fn truncate(&self, size: u64) -> Result<()>;
    // async fn allocate(&self, offset: u64, len: u64) -> Result<()>;

    fn file_size(&self) -> usize;
    fn is_empty(&self) -> bool;
}

pub(crate) async fn asyncify<F, T>(f: F) -> Result<T>
where
    F: FnOnce() -> Result<T> + Send + 'static,
    T: Send + 'static,
{
    match spawn_blocking(f).await {
        Ok(res) => res,
        Err(e) => Err(Error::new(
            ErrorKind::Other,
            format!("background task failed: {:?}", e),
        )),
    }
}

#[cfg(test)]
mod test {
    use std::path::Path;

    use crate::file_system::async_filesystem::{LocalFileSystem, LocalFileType};
    use crate::file_system::FileSystem;

    #[tokio::test]
    #[ignore]
    async fn test_delete_file_when_reading() {
        let dir = "/tmp/test/file_system/test_delete_file_when_reading";
        let _ = std::fs::remove_dir_all(dir);
        std::fs::create_dir_all(dir).unwrap();

        let path = Path::new(dir).join("test.txt");
        let file_system = LocalFileSystem::new(LocalFileType::Mmap);
        let mut write_file = file_system.open_file_writer(&path).await.unwrap();
        let read_file = file_system.open_file_reader(&path).await.unwrap();
        let mut data = b"hello world".to_vec();

        for _ in 0..1000 {
            write_file.write(&data).await.unwrap();
        }

        std::fs::remove_file(&path).unwrap();
        let mut pos = 0_usize;
        for _ in 0..1000 {
            let read_size = read_file.read_at(pos, &mut data).await.unwrap();
            assert_eq!(data, b"hello world".to_vec());
            pos += read_size;
        }
    }

    #[tokio::test]
    #[ignore]
    async fn test_delete_file_when_writing() {
        let dir = "/tmp/test/file_system/test_delete_file_when_reading";
        let _ = std::fs::remove_dir_all(dir);
        std::fs::create_dir_all(dir).unwrap();
        let file_system = LocalFileSystem::new(LocalFileType::Mmap);

        let path = Path::new(dir).join("test.txt");
        let mut write_file = file_system.open_file_writer(&path).await.unwrap();
        let read_file = file_system.open_file_reader(&path).await.unwrap();
        let mut data = b"hello world".to_vec();

        for i in 0..1000 {
            let _wrote_size = write_file.write(&data).await.unwrap();
            if i == 500 {
                std::fs::remove_file(&path).unwrap();
            }
        }

        let mut pos = 0_usize;
        for _ in 0..1000 {
            let read_size = read_file.read_at(pos, &mut data).await.unwrap();
            assert_eq!(data, b"hello world".to_vec());
            pos += read_size;
        }
    }
}
