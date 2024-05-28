pub(crate) mod async_file;
pub(crate) mod mmap_file;
mod os;
mod raw_file;
pub mod stream_reader;
pub mod stream_writer;

use std::any::Any;
use std::future::Future;
use std::io::Result;
use std::pin::Pin;
use std::task::{Context, Poll};

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
    fn as_any(&self) -> &dyn Any;
}

pub(crate) async unsafe fn asyncify<'a, F, T>(f: F) -> Result<T>
where
    F: FnOnce() -> Result<T> + Send + 'a,
    T: Send + 'static,
{
    struct Fut<F, T>(Option<F>)
    where
        F: Future<Output = T>;

    impl<F, T> Future for Fut<F, T>
    where
        F: Future<Output = T> + Unpin,
    {
        type Output = T;

        fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
            let r = Pin::new(self.0.as_mut().unwrap()).poll(cx);
            if r.is_ready() {
                self.0 = None;
            }
            r
        }
    }

    impl<F, T> Drop for Fut<F, T>
    where
        F: Future<Output = T>,
    {
        fn drop(&mut self) {
            if let Some(f) = self.0.take() {
                tokio::runtime::Handle::current().block_on(f);
            }
        }
    }

    let f: Box<dyn FnOnce() -> Result<T> + Send> = Box::new(f);
    let f: Box<dyn FnOnce() -> Result<T> + Send + 'static> = std::mem::transmute(f);
    Fut(Some(spawn_blocking(f))).await?
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
        let file_system_2 = LocalFileSystem::new(LocalFileType::ThreadPool);
        let mut write_file = file_system_2.open_file_writer(&path, 1024).await.unwrap();
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
        let file_system_2 = LocalFileSystem::new(LocalFileType::ThreadPool);

        let path = Path::new(dir).join("test.txt");
        let mut write_file = file_system_2.open_file_writer(&path, 1024).await.unwrap();
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
