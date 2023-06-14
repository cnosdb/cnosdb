use std::fs;
use std::fs::OpenOptions;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use once_cell::sync::OnceCell;
use snafu::{ResultExt, Snafu};

use crate::file_system::file::async_file::{AsyncFile, FsRuntime};
use crate::{error, Error, Result};

#[derive(Snafu, Debug)]
pub enum FileError {
    #[snafu(display("Unable to open file '{}': {}", path.display(), source))]
    UnableToOpenFile {
        path: PathBuf,
        source: std::io::Error,
    },

    #[snafu(display("Unable to write file '{}': {}", path.display(), source))]
    UnableToWriteBytes {
        path: PathBuf,
        source: std::io::Error,
    },

    #[snafu(display("Unable to sync file '{}': {}", path.display(), source))]
    UnableToSyncFile {
        path: PathBuf,
        source: std::io::Error,
    },

    #[snafu(display("async file system stopped"))]
    Cancel,
}

static INSTANCE: OnceCell<FileManager> = OnceCell::new();

pub struct FileManager {
    fs_runtime: Arc<FsRuntime>,
}

pub fn get_file_manager() -> &'static FileManager {
    INSTANCE.get_or_init(FileManager::new)
}

impl FileManager {
    fn new() -> Self {
        Self {
            fs_runtime: Arc::new(FsRuntime::new_runtime()),
        }
    }

    pub async fn open_file_with(
        &self,
        path: impl AsRef<Path>,
        options: OpenOptions,
    ) -> Result<AsyncFile> {
        AsyncFile::open(path.as_ref(), self.fs_runtime.clone(), options)
            .await
            .map_err(|e| Error::OpenFile {
                path: path.as_ref().to_path_buf(),
                source: e,
            })
    }

    /// Open a file to read,.
    pub async fn open_file(&self, path: impl AsRef<Path>) -> Result<AsyncFile> {
        let mut opt = OpenOptions::new();
        opt.read(true);
        self.open_file_with(path, opt).await
    }

    fn create_dir_if_not_exists(parent: Option<&Path>) -> Result<()> {
        if let Some(p) = parent {
            if !try_exists(p) {
                fs::create_dir_all(p).context(error::IOSnafu)?;
            }
        }
        Ok(())
    }

    /// Create a file if not exists, overwrite if already existed.
    pub async fn create_file(&self, path: impl AsRef<Path>) -> Result<AsyncFile> {
        let p = path.as_ref();
        Self::create_dir_if_not_exists(p.parent())?;
        let mut opt = OpenOptions::new();
        opt.read(true).write(true).create(true);
        self.open_file_with(p, opt).await
    }

    /// Open a file to read or write(append mode), if file does not exists then create it.
    pub async fn open_create_file(&self, path: impl AsRef<Path>) -> Result<AsyncFile> {
        let p = path.as_ref();
        Self::create_dir_if_not_exists(p.parent())?;
        let mut opt = OpenOptions::new();
        opt.read(true).write(true).create(true).append(true);
        self.open_file_with(path, opt).await
    }
}

pub fn list_file_names(dir: impl AsRef<Path>) -> Vec<String> {
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

pub fn list_dir_names(dir: impl AsRef<Path>) -> Vec<String> {
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

/// Case `std::fs::try_exists` is unstable, so copied the same logic to here.
/// Todo For that reason, this way to check file exists may be disabled someday.
#[inline(always)]
pub fn try_exists(path: impl AsRef<Path>) -> bool {
    fs::metadata(path).is_ok()
}

/// Open a file to read,.
#[inline(always)]
pub async fn open_file(path: impl AsRef<Path>) -> Result<AsyncFile> {
    get_file_manager().open_file(path).await
}

/// Create a file if not exists, overwrite if already existed.
#[inline(always)]
pub async fn create_file(path: impl AsRef<Path>) -> Result<AsyncFile> {
    get_file_manager().create_file(path).await
}

/// Open a file to read or write(append mode), if file does not exists then create it.
#[inline(always)]
pub async fn open_create_file(path: impl AsRef<Path>) -> Result<AsyncFile> {
    get_file_manager().open_create_file(path).await
}

#[cfg(test)]
mod test {
    use std::path::PathBuf;

    use trace::info;

    use super::FileManager;
    use crate::file_system::file::cursor::FileCursor;
    use crate::file_system::file::IFile;
    use crate::file_system::file_manager;

    #[tokio::test]
    async fn test_get_instance() {
        let file_manager_1 = file_manager::get_file_manager();
        info!("0x{:X}", file_manager_1 as *const FileManager as usize);
        let file_manager_2 = file_manager::get_file_manager();
        info!("0x{:X}", file_manager_2 as *const FileManager as usize);
        assert_eq!(
            file_manager_1 as *const FileManager as usize,
            file_manager_2 as *const FileManager as usize
        );

        let file_manager_3 = FileManager::new();
        info!("0x{:X}", &file_manager_3 as *const FileManager as usize);
        assert_ne!(
            file_manager_1 as *const FileManager as usize,
            &file_manager_3 as *const FileManager as usize
        );
    }

    #[tokio::test]
    async fn test_open_file() {
        let dir = "/tmp/test/file_manager/test_open_file";
        let _ = std::fs::remove_dir_all(dir);
        let path = PathBuf::from(dir).join("test.txt");

        let open_file_ret_1 = file_manager::open_file(&path).await;
        assert!(open_file_ret_1.is_err());

        let _ = file_manager::create_file(&path).await.unwrap();

        let open_file_ret_2 = file_manager::open_file(&path).await;
        assert!(open_file_ret_2.is_ok());
    }

    #[tokio::test]
    async fn test_io_basic() {
        let dir = "/tmp/test/file_manager/test_io_basic";
        let _ = std::fs::remove_dir_all(dir);
        let path = PathBuf::from(dir).join("test.txt");

        {
            // Test creating a new file and write.
            let file = file_manager::create_file(&path).await.unwrap();

            let data = [0, 1, 2, 3];
            // Write 4 bytes data.
            let len = file.write_at(0, &data).await.unwrap();
            assert_eq!(data.len(), len);

            let mut buf = [0_u8; 2];
            let size = file.read_at(1, &mut buf).await.unwrap();
            assert_eq!(size, buf.len());
            assert_eq!(buf, [1, 2]);
        }
        {
            // Test overwriting a file.
            let file = file_manager::create_file(&path).await.unwrap();

            let data = [3, 2, 1, 0];
            // Write 4 bytes data.
            let len = file.write_at(0, &data).await.unwrap();
            assert_eq!(data.len(), len);

            let mut buf = [0_u8; 2];
            let size = file.read_at(1, &mut buf).await.unwrap();
            assert_eq!(size, 2);
            assert_eq!(buf, [2, 1]);
        }
        {
            // Test appending a file.
            let file = file_manager::create_file(&path).await.unwrap();

            let data = [0, 1, 2, 3];
            // Append 4 bytes data.
            let len = file.write_at(file.len(), &data).await.unwrap();
            assert_eq!(data.len(), len);

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

        let file = file_manager::create_file(&path).await.unwrap();
        let mut len = 0_usize;
        // Write 32MB data.
        let file_len = data.len() * 32;
        for _ in 0..32 {
            len += file.write_at(len as u64, &data).await.unwrap();
        }
        assert_eq!(len, file_len);

        let mut buf = [0_u8; 8];

        // Read 8 bytes at 1024.
        len = file.read_at(1024, &mut buf).await.unwrap();
        assert_eq!(len, buf.len());
        assert_eq!(buf, [0, 1, 2, 3, 0, 1, 2, 3]);

        // Write 4 bytes at 1024 and read 8 bytes.
        len = file.write_at(1024, &[1, 1, 1, 1]).await.unwrap();
        assert_eq!(len, 4);
        let size = file.read_at(1024, &mut buf).await.unwrap();
        assert_eq!(size, buf.len());
        assert_eq!(buf, [1, 1, 1, 1, 0, 1, 2, 3]);

        // Write 8 bytes at end -4 and read 7 bytes at end - 4.
        let new_chunk = [11, 12, 13, 14, 15, 16, 17, 18];
        len = file
            .write_at((file_len - 4) as u64, &new_chunk)
            .await
            .unwrap();
        assert_eq!(len, new_chunk.len());
        len = file.read_at((file_len - 4) as u64, &mut buf).await.unwrap();
        assert_eq!(len, buf.len());
        assert_eq!(buf, new_chunk);
    }

    #[tokio::test]
    async fn test_truncate() {
        let dir = "/tmp/test/file_manager/test_truncate";
        let _ = std::fs::remove_dir_all(dir);
        let path = PathBuf::from(dir).join("test.txt");

        let data = &[0, 1, 2, 3, 4, 5];
        {
            let file = file_manager::create_file(&path).await.unwrap();
            let mut len = file.write_at(0, data).await.unwrap();
            assert_eq!(len, data.len());

            let mut buf = [0_u8; 2];
            len = file.read_at(1, &mut buf).await.unwrap();
            assert_eq!(len, buf.len());
            assert_eq!(buf, [1, 2]);

            file.truncate(3).await.unwrap();
        }

        let file = file_manager::open_file(path).await.unwrap();
        assert_eq!(file.len(), 3);
        let mut buf = vec![0; 3];
        let len = file.read_at(0, &mut buf).await.unwrap();
        assert_eq!(len, 3);
        assert_eq!(buf.as_slice(), &data[0..3]);
    }

    #[tokio::test]
    async fn test_cursor() {
        let dir = "/tmp/test/file_manager/test_cursor";
        let _ = std::fs::remove_dir_all(dir);
        let path = PathBuf::from(dir).join("test.txt");

        let file = file_manager::create_file(&path).await.unwrap();
        let mut cursor: FileCursor = file.into();
        for _ in 0..16 {
            let len = cursor.write(&[0, 1, 2, 3, 4]).await.unwrap();
            assert_eq!(len, 5);
        }
        cursor.set_pos(5);
        let mut buf = [0_u8; 8];
        let len = cursor.read(&mut buf[0..5]).await.unwrap();
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
