use std::fs;
use std::fs::OpenOptions;
use std::path::Path;

use crate::file_system::error::{FileSystemError, FileSystemResult};
use crate::file_system::file::async_file::AsyncFile;
use crate::file_system::file::mmap_file::MmapFile;
use crate::file_system::file::stream_reader::FileStreamReader;
use crate::file_system::file::stream_writer::FileStreamWriter;
use crate::file_system::FileSystem;

#[derive(Clone)]
pub struct LocalFileSystem {
    file_type: LocalFileType, // ThreadPool, Mmap, Aio, IoUring
}

impl LocalFileSystem {
    pub fn new(file_type: LocalFileType) -> Self {
        Self { file_type }
    }
}

unsafe impl Send for LocalFileSystem {}
unsafe impl Sync for LocalFileSystem {}
#[async_trait::async_trait]
impl FileSystem for LocalFileSystem {
    async fn open_file_reader(
        &self,
        path: impl AsRef<Path> + Send + Sync,
    ) -> FileSystemResult<Box<FileStreamReader>> {
        match self.file_type {
            LocalFileType::ThreadPool => Self::read_thread_pool_file(path).await,
            LocalFileType::Mmap => Self::read_mmap_file(path).await,
            _ => unimplemented!(),
        }
    }
    async fn open_file_writer(
        &self,
        path: impl AsRef<Path> + Send + Sync,
        buf_size: usize,
    ) -> FileSystemResult<Box<FileStreamWriter>> {
        match self.file_type {
            LocalFileType::ThreadPool => Self::write_thread_pool_file(path, buf_size).await,
            _ => unimplemented!(),
        }
    }
    fn remove(path: impl AsRef<Path>) -> FileSystemResult<()> {
        fs::remove_file(path).map_err(|e| FileSystemError::StdIOError { source: e })?;
        Ok(())
    }
    fn rename(
        old_filename: impl AsRef<Path>,
        new_filename: impl AsRef<Path>,
    ) -> FileSystemResult<()> {
        fs::rename(old_filename, new_filename)
            .map_err(|e| FileSystemError::StdIOError { source: e })?;
        Ok(())
    }

    fn create_dir_if_not_exists(parent: Option<&Path>) -> FileSystemResult<()> {
        if let Some(p) = parent {
            if !Self::try_exists(p) {
                fs::create_dir_all(p).map_err(|e| FileSystemError::StdIOError { source: e })?;
            }
        }
        Ok(())
    }

    #[inline(always)]
    fn try_exists(path: impl AsRef<Path>) -> bool {
        fs::metadata(path).is_ok()
    }

    fn list_dir_names(dir: impl AsRef<Path>) -> Vec<String> {
        let mut list = Vec::new();

        for file_name in walkdir::WalkDir::new(dir)
            .min_depth(1)
            .max_depth(1)
            .sort_by_file_name()
            .into_iter()
            .filter_map(|e| {
                let dir_entry = match e {
                    Ok(dir_entry) if dir_entry.file_type().is_dir() => dir_entry,
                    _ => {
                        return None;
                    }
                };
                dir_entry
                    .file_name()
                    .to_str()
                    .map(|file_name| file_name.to_string())
            })
        {
            list.push(file_name);
        }

        list
    }

    fn list_file_names(dir: impl AsRef<Path>) -> Vec<String> {
        let mut list = Vec::new();

        for file_name in walkdir::WalkDir::new(dir)
            .min_depth(1)
            .max_depth(1)
            .sort_by_file_name()
            .into_iter()
            .filter_map(|e| {
                let dir_entry = match e {
                    Ok(dir_entry) if dir_entry.file_type().is_file() => dir_entry,
                    _ => {
                        return None;
                    }
                };
                dir_entry
                    .file_name()
                    .to_str()
                    .map(|file_name| file_name.to_string())
            })
        {
            list.push(file_name);
        }

        list
    }
    fn get_file_length(file: String) -> u64 {
        fs::metadata(file).map(|m| m.len()).unwrap_or(0)
    }
}

#[derive(Debug, Clone)]
pub enum LocalFileType {
    ThreadPool,
    Mmap,
    Aio,
    IOUring,
}

impl LocalFileSystem {
    /// Open a file to read.
    pub async fn read_mmap_file(path: impl AsRef<Path>) -> FileSystemResult<Box<FileStreamReader>> {
        let mut opt = OpenOptions::new();
        opt.read(true).write(true);
        let file = MmapFile::open(&path, opt)
            .await
            .map_err(|e| FileSystemError::StdIOError { source: e })?;
        Ok(Box::new(FileStreamReader::new(
            Box::new(file),
            path.as_ref().to_path_buf(),
        )))
    }

    pub async fn read_thread_pool_file(
        path: impl AsRef<Path>,
    ) -> FileSystemResult<Box<FileStreamReader>> {
        let mut opt = OpenOptions::new();
        opt.read(true);
        let file = AsyncFile::open(&path, opt)
            .await
            .map_err(|e| FileSystemError::StdIOError { source: e })?;
        Ok(Box::new(FileStreamReader::new(
            Box::new(file),
            path.as_ref().to_path_buf(),
        )))
    }

    pub async fn write_thread_pool_file(
        path: impl AsRef<Path>,
        buf_size: usize,
    ) -> FileSystemResult<Box<FileStreamWriter>> {
        let p = path.as_ref();
        Self::create_dir_if_not_exists(p.parent())?;
        let mut opt = OpenOptions::new();
        opt.write(true).create(true).read(true);
        let file = AsyncFile::open(&path, opt)
            .await
            .map_err(|e| FileSystemError::StdIOError { source: e })?;
        Ok(Box::new(FileStreamWriter::new(
            Box::new(file),
            p.to_path_buf(),
            buf_size,
        )))
    }

    pub fn remove_if_exists(path: impl AsRef<Path>) -> FileSystemResult<()> {
        if Self::try_exists(&path) {
            fs::remove_file(path).map_err(|e| FileSystemError::StdIOError { source: e })?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use std::io::SeekFrom;
    use std::path::PathBuf;

    use tokio::fs::OpenOptions;
    use tokio::io::AsyncSeekExt;

    use super::LocalFileSystem;
    use crate::file_system::FileSystem;

    #[tokio::test]
    async fn test_open_file() {
        let dir = "/tmp/test/file_manager/test_open_file";
        let _ = std::fs::remove_dir_all(dir);
        let path = PathBuf::from(dir).join("test.txt");
        let file_system = LocalFileSystem::new(super::LocalFileType::Mmap);
        let file_system_2 = LocalFileSystem::new(super::LocalFileType::ThreadPool);

        let open_file_ret_1 = file_system.open_file_reader(&path).await;
        assert!(open_file_ret_1.is_err());

        let _ = file_system_2.open_file_writer(&path, 1024).await.unwrap();

        let open_file_ret_2 = file_system.open_file_reader(&path).await;
        assert!(open_file_ret_2.is_ok());
    }

    #[tokio::test]
    async fn test_io_basic() {
        let dir = "/tmp/test/file_manager/test_io_basic";
        let _ = std::fs::remove_dir_all(dir);
        let path = PathBuf::from(dir).join("test.txt");
        let file_system = LocalFileSystem::new(super::LocalFileType::ThreadPool);

        {
            // Test creating a new file and write.
            let mut file = file_system.open_file_writer(&path, 1024).await.unwrap();

            let data = [0, 1, 2, 3];
            // Write 4 bytes data.
            let len = file.write(&data).await.unwrap();
            file.flush().await.unwrap();
            assert_eq!(data.len(), len);

            let file = file_system.open_file_reader(&path).await.unwrap();

            let mut buf = [0_u8; 2];
            let size = file.read_at(1, &mut buf).await.unwrap();
            assert_eq!(size, buf.len());
            assert_eq!(buf, [1, 2]);
        }
        {
            // Test overwriting a file.
            let mut file = file_system.open_file_writer(&path, 1024).await.unwrap();
            file.seek(SeekFrom::Start(0)).await.unwrap();

            let data = [3, 2, 1, 0];
            // Write 4 bytes data.
            let len = file.write(&data).await.unwrap();
            file.flush().await.unwrap();
            assert_eq!(data.len(), len);

            let file = file_system.open_file_reader(&path).await.unwrap();

            let mut buf = [0_u8; 2];
            let size = file.read_at(1, &mut buf).await.unwrap();
            assert_eq!(size, 2);
            assert_eq!(buf, [2, 1]);
        }
        {
            // Test appending a file.
            let mut file = file_system.open_file_writer(&path, 1024).await.unwrap();

            let data = [0, 1, 2, 3];
            // Append 4 bytes data.
            let len = file.write(&data).await.unwrap();
            file.flush().await.unwrap();
            assert_eq!(data.len(), len);

            let file = file_system.open_file_reader(&path).await.unwrap();

            let mut buf = [0_u8; 100];
            let size = file.read_at(0, &mut buf).await.unwrap();
            assert_eq!(size, 8);
            assert_eq!(&buf[0..10], &[3, 2, 1, 0, 0, 1, 2, 3, 0, 0]);
        }
    }

    #[tokio::test]
    async fn test_write() {
        let dir = "/tmp/test/file_manager/test_write";
        let _ = std::fs::remove_dir_all(dir);
        let path = PathBuf::from(dir).join("test.txt");

        let chunk = &[0, 1, 2, 3];
        let mut data = Vec::<u8>::with_capacity(1024 * 1024);
        for _ in (0..1024 * 1024 / 4).step_by(chunk.len()) {
            data.extend_from_slice(chunk);
        }

        let file_system = LocalFileSystem::new(super::LocalFileType::ThreadPool);

        let mut write_file = file_system.open_file_writer(&path, 1024).await.unwrap();
        // Write 32MB data.
        let file_len = data.len() * 32;
        for _ in 0..32 {
            write_file.write(&data).await.unwrap();
        }
        write_file.flush().await.unwrap();
        assert_eq!(write_file.len(), file_len);

        let mut buf = [0_u8; 8];
        let read_file = file_system.open_file_reader(&path).await.unwrap();

        // Read 8 bytes at 1024.
        read_file.read_at(1024, &mut buf).await.unwrap();
        assert_eq!(buf, [0, 1, 2, 3, 0, 1, 2, 3]);

        // Write 4 bytes at 1024 and read 8 bytes.
        write_file.seek(SeekFrom::Start(1024)).await.unwrap();
        let mut len = write_file.write(&[1, 1, 1, 1]).await.unwrap();
        write_file.flush().await.unwrap();
        assert_eq!(len, 4);
        let size = read_file.read_at(1024, &mut buf).await.unwrap();
        assert_eq!(size, buf.len());
        assert_eq!(buf, [1, 1, 1, 1, 0, 1, 2, 3]);

        // Write 8 bytes at end -4 and read 7 bytes at end - 4.
        let new_chunk = [11, 12, 13, 14, 15, 16, 17, 18];
        write_file
            .seek(SeekFrom::Start((file_len - 4) as u64))
            .await
            .unwrap();
        len = write_file.write(&new_chunk).await.unwrap();
        write_file.flush().await.unwrap();
        assert_eq!(len, new_chunk.len());
        len = read_file.read_at(file_len - 4, &mut buf).await.unwrap();
        assert_eq!(len, buf.len());
        assert_eq!(buf, new_chunk);
    }

    #[tokio::test]
    async fn test_truncate() {
        let dir = "/tmp/test/file_manager/test_truncate";
        let _ = std::fs::remove_dir_all(dir);
        let path = PathBuf::from(dir).join("test.txt");
        let file_system = LocalFileSystem::new(super::LocalFileType::ThreadPool);

        let data = &[0, 1, 2, 3, 4, 5];
        {
            let mut write_file = file_system.open_file_writer(&path, 1024).await.unwrap();
            let read_file = file_system.open_file_reader(&path).await.unwrap();
            let mut len = write_file.write(data).await.unwrap();
            write_file.flush().await.unwrap();
            assert_eq!(len, data.len());

            let mut buf = [0_u8; 2];
            len = read_file.read_at(1, &mut buf).await.unwrap();
            assert_eq!(len, buf.len());
            assert_eq!(buf, [1, 2]);

            write_file.truncate(3).await.unwrap();
        }

        let read_file = file_system.open_file_reader(&path).await.unwrap();
        assert_eq!(read_file.len(), 3);
        let mut buf = vec![0; 3];
        let len = read_file.read_at(0, &mut buf).await.unwrap();
        assert_eq!(len, 3);
        assert_eq!(buf.as_slice(), &data[0..3]);
    }

    #[tokio::test]
    async fn test_tokio_fs() {
        use std::io::SeekFrom;

        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let dir = "/tmp/test/file_manager/test_tokio_fs";
        let _ = std::fs::remove_dir_all(dir);
        let path = PathBuf::from(dir).join("test.txt");
        // let p = path.as_ref();
        LocalFileSystem::create_dir_if_not_exists(path.parent()).unwrap();
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .read(true)
            .open(path)
            .await
            .unwrap();
        let data = [0, 1, 2, 3, 4, 5];
        file.write_all(&data).await.unwrap();
        let mut buf = [0_u8; 2];
        file.seek(SeekFrom::Start(1)).await.unwrap();
        let n = file.read(&mut buf).await.unwrap();
        assert_eq!(n, 2);
        assert_eq!(buf, [1, 2]);
    }

    #[tokio::test]
    async fn test_cursor() {
        let dir = "/tmp/test/file_manager/test_cursor";
        let _ = std::fs::remove_dir_all(dir);
        let path = PathBuf::from(dir).join("test.txt");
        let file_system = LocalFileSystem::new(super::LocalFileType::ThreadPool);
        let mut write_file = file_system.open_file_writer(&path, 1024).await.unwrap();

        for _ in 0..16 {
            let len = write_file.write(&[0, 1, 2, 3, 4]).await.unwrap();
            assert_eq!(len, 5);
        }
        write_file.flush().await.unwrap();
        let mut read_file = file_system.open_file_reader(&path).await.unwrap();
        read_file.seek(SeekFrom::Start(5)).unwrap();
        let mut buf = [0_u8; 8];
        let len = read_file.read(&mut buf[0..5]).await.unwrap();
        assert_eq!(len, 5);
        assert_eq!(buf, [0, 1, 2, 3, 4, 0, 0, 0]);
    }

    #[test]
    #[cfg(feature = "io_uring")]
    fn test_io_uring() {
        tokio_uring::start(async {
            let file = tokio_uring::fs::File::create("/tmp/test/file_manager/hello.txt")
                .await
                .unwrap();
            let (res, buf) = file.write_at(&b"hello"[..], 0).await;
            let fd = file.as_raw_fd();
            println!("main thread: file: {:?}, {:?}", file, fd);
            let (sender, mut receiver) = mpsc::channel();
            drop(file);
            let thread = std::thread::spawn(move || {
                let fd = receiver.blocking_recv().unwrap();
                println!("thread 2: fd {:?}", fd);
                tokio_uring::start(async {
                    tokio_uring::spawn(async move {
                        let file2 = unsafe { tokio_uring::fs::File::from_raw_fd(fd) };
                        println!("thread 2: file: {:?}, {:?}", file2, fd);
                        let (res, buf) = file2.write_at(&b"hello world"[..], 0).await;
                        let n = res.unwrap();
                        println!("wrote {} bytes", n);
                        // // Sync data to the file system.
                        // file2.sync_all().await.unwrap();
                        // // Close the file
                        // file2.close().await.unwrap();
                    })
                    .await
                    .unwrap()
                })
            });
            sender.send(fd).unwrap();
            thread.join().unwrap();
        })
    }
}

// failures:
//     file_system::async_filesystem::test::test_tokio_fs
//     wal::wal_store::test::test_wal_entry_storage_restart
//     wal::wal_store::test::test_wal_raft_storage_with_openraft_cases
