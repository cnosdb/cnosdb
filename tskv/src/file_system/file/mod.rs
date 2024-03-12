pub(crate) mod async_file;
pub(crate) mod cursor;
mod os;

use std::io;
use std::io::IoSlice;

use async_trait::async_trait;

#[async_trait]
pub trait IFile: Send {
    async fn write_vec<'a>(&self, pos: u64, bufs: &'a mut [IoSlice<'a>]) -> io::Result<usize>;
    async fn write_at(&self, pos: u64, data: &[u8]) -> io::Result<usize>;
    async fn read_at(&self, pos: u64, data: &mut [u8]) -> io::Result<usize>;
    async fn sync_data(&self) -> io::Result<()>;
    async fn truncate(&self, size: u64) -> io::Result<()>;
    fn len(&self) -> u64;
    fn is_empty(&self) -> bool;
}

#[cfg(test)]
mod test {
    use std::path::Path;

    use crate::file_system::file::IFile;
    use crate::file_system::file_manager;

    #[tokio::test]
    #[ignore]
    async fn test_delete_file_when_reading() {
        let dir = "/tmp/test/file_system/test_delete_file_when_reading";
        let _ = std::fs::remove_dir_all(dir);
        std::fs::create_dir_all(dir).unwrap();

        let path = Path::new(dir).join("test.txt");
        let file = file_manager::open_create_file(&path).await.unwrap();
        let mut data = b"hello world".to_vec();

        let mut pos = 0_usize;
        for _ in 0..1000 {
            let wrote_size = file.write_at(pos as u64, &data).await.unwrap();
            pos += wrote_size;
        }

        std::fs::remove_file(&path).unwrap();
        pos = 0;
        for _ in 0..1000 {
            let read_size = file.read_at(pos as u64, &mut data).await.unwrap();
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

        let path = Path::new(dir).join("test.txt");
        let file = file_manager::open_create_file(&path).await.unwrap();
        let mut data = b"hello world".to_vec();

        let mut pos = 0_usize;
        for i in 0..1000 {
            let wrote_size = file.write_at(pos as u64, &data).await.unwrap();
            if i == 500 {
                std::fs::remove_file(&path).unwrap();
            }
            pos += wrote_size;
        }

        pos = 0;
        for _ in 0..1000 {
            let read_size = file.read_at(pos as u64, &mut data).await.unwrap();
            assert_eq!(data, b"hello world".to_vec());
            pos += read_size;
        }
    }
}
