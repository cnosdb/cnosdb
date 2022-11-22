use std::fs::OpenOptions;
use std::{
    borrow::BorrowMut,
    fs,
    path::{Path, PathBuf},
    sync::Arc,
};

use once_cell::sync::OnceCell;
use snafu::{ResultExt, Snafu};

use crate::file_system::file::async_file::{AsyncFile, FsRuntime};
use crate::{
    error,
    file_system::{self},
    Error, Result,
};

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

pub struct FileManager {
    fs_runtime: Arc<FsRuntime>,
}

pub fn get_file_manager() -> &'static FileManager {
    static INSTANCE: OnceCell<FileManager> = OnceCell::new();
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
            .context(error::OpenFileSnafu {
                path: path.as_ref(),
            })
    }

    pub async fn open_file(&self, path: impl AsRef<Path>) -> Result<AsyncFile> {
        let options = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .clone();
        AsyncFile::open(path.as_ref(), self.fs_runtime.clone(), options)
            .await
            .context(error::OpenFileSnafu {
                path: path.as_ref(),
            })
    }

    pub async fn create_file(&self, path: impl AsRef<Path>) -> Result<AsyncFile> {
        if let Some(p) = path.as_ref().parent() {
            if !try_exists(p) {
                fs::create_dir_all(p).context(error::IOSnafu)?;
            }
        }
        self.open_file(&path).await
    }

    pub async fn open_create_file(&self, path: impl AsRef<Path>) -> Result<AsyncFile> {
        self.create_file(path).await
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

/// Case `std::fs::try_exists` is unstable, so copied the same logic to here.
/// Todo For that reason, this way to check file exists may be disabled someday.
#[inline(always)]
pub fn try_exists(path: impl AsRef<Path>) -> bool {
    fs::metadata(path).is_ok()
}

#[inline(always)]
pub async fn open_file(path: impl AsRef<Path>) -> Result<AsyncFile> {
    get_file_manager().open_file(path).await
}

#[inline(always)]
pub async fn create_file(path: impl AsRef<Path>) -> Result<AsyncFile> {
    get_file_manager().create_file(path).await
}

#[inline(always)]
pub async fn open_create_file(path: impl AsRef<Path>) -> Result<AsyncFile> {
    get_file_manager().open_create_file(path).await
}

#[cfg(test)]
mod test {
    use std::os::unix::prelude::AsRawFd;
    use std::sync::Arc;
    use std::{os::unix::io::FromRawFd, path::Path};

    use libc::send;
    use tokio::sync::{mpsc, oneshot};
    use trace::info;

    use crate::file_system::{file_manager, FileCursor, IFile};

    use super::FileManager;

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

    async fn test_basic_io(dir: impl AsRef<Path>) {
        println!("start basic io");
        let file = file_manager::get_file_manager()
            .open_file(dir.as_ref().join("fs.test1"))
            .await
            .unwrap();
        println!("start write");
        let len = file.write_at(0, &[0, 1, 2, 3, 4]).await.unwrap();
        println!("end write");
        let mut buf = [0_u8; 2];
        let size = file.read_at(1, &mut buf).await.unwrap();
        assert_eq!(size, buf.len());
        let expect = [1, 2];
        assert_eq!(buf, expect);
    }

    async fn test_write(dir: impl AsRef<Path>) {
        let mut len = 0;
        let file = file_manager::get_file_manager()
            .open_file(dir.as_ref().join("fs.test2"))
            .await
            .unwrap();
        for i in 0..1024 {
            len += file.write_at(len, &[0, 1, 2, 3, 4]).await.unwrap() as u64;
        }
        let len = file.write_at(0, &[0, 1, 2, 3, 4]).await.unwrap();
        let mut buf = [0_u8; 5];
        let size = file.read_at(2, &mut buf).await.unwrap();
        assert_eq!(size, buf.len());
        let expect = [2_u8, 3, 4, 0, 1];
        assert_eq!(buf, expect);
    }

    async fn test_truncate(dir: impl AsRef<Path>) {
        let path = dir.as_ref().join("fs.test3");
        let file = file_manager::get_file_manager()
            .open_file(&path)
            .await
            .unwrap();
        let len = file.write_at(0, &[0, 1, 2, 3, 4, 5]).await.unwrap();
        let mut buf = [0_u8; 2];
        let size = file.read_at(1, &mut buf).await.unwrap();
        assert_eq!(size, buf.len());
        let expect = [1, 2];
        assert_eq!(buf, expect);
        file.truncate(3).await.unwrap();
        let file1 = file_manager::get_file_manager()
            .open_file(path)
            .await
            .unwrap();
        assert_eq!(file1.len(), 3);
    }

    async fn test_cursor(dir: impl AsRef<Path>) {
        let file = file_manager::get_file_manager()
            .open_file(dir.as_ref().join("fs.test4"))
            .await
            .unwrap();
        let mut cursor: FileCursor = file.into();
        for i in 0..1024 {
            cursor.write(&[0, 1, 2, 3, 4]).await.unwrap();
        }
        cursor.set_pos(5);
        let mut buf = [0_u8; 5];
        let read = cursor.read(&mut buf).await.unwrap();
        assert_eq!(buf, [0, 1, 2, 3, 4]);
    }

    #[tokio::test]
    async fn test_all() {
        let dir = "/tmp/test/file_manager";
        let _ = std::fs::remove_dir(dir);
        std::fs::create_dir_all(dir).unwrap();
        test_basic_io(dir).await;
        test_write(dir).await;
        test_truncate(dir).await;
        test_cursor(dir).await;
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
            let (sender, mut receiver) = mpsc::unbounded_channel();
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
