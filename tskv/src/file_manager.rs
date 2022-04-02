use std::borrow::BorrowMut;
use std::path::Path;
use std::sync::Arc;
use std::thread::JoinHandle;
use std::{fs, thread};

use crate::{direct_io, error, run_io_task, AsyncContext, File, IoTask, TaskType};
use once_cell::sync::OnceCell;
use parking_lot::Mutex;
use snafu::{ResultExt, Snafu};

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("Unable to open file: {}", source))]
    UnableToOpenFile { source: std::io::Error },

    #[snafu(display("Unable to write file: {}", source))]
    UnableToWriteBytes { source: std::io::Error },

    #[snafu(display("Unable to sync file: {}", source))]
    UnableToSyncFile { source: std::io::Error },

    #[snafu(display("async file system stopped"))]
    Cancel,
}

type Result<T> = std::result::Result<T, Error>;

pub struct FileManager {
    file_system: direct_io::FileSystem,
    async_rt: Arc<AsyncContext>,
    thread_pool: Mutex<Vec<thread::JoinHandle<()>>>,
}

impl FileManager {
    fn new() -> Self {
        let fs_options = direct_io::Options::default();
        let thread_num = fs_options.get_thread_num();
        let rt = Arc::new(AsyncContext::new(fs_options.get_thread_num()));
        let mut pool = Vec::new();
        for i in 0..thread_num {
            let mrt = rt.clone();
            let h = thread::Builder::new()
                .name("AsyncIOThread_".to_string() + &i.to_string())
                .spawn(move || run_io_task(mrt, i))
                .unwrap();
            pool.push(h);
        }

        return Self {
            file_system: direct_io::FileSystem::new(&fs_options),
            async_rt: rt,
            thread_pool: Mutex::new(pool),
        };
    }

    pub fn get_instance() -> &'static Self {
        static INSTANCE: OnceCell<FileManager> = OnceCell::new();
        INSTANCE.get_or_init(|| Self::new())
    }

    pub fn open_file_with(
        &self,
        path: impl AsRef<Path>,
        options: &fs::OpenOptions,
    ) -> Result<direct_io::File> {
        self.file_system
            .open_with(path, options)
            .map_err(|err| Error::UnableToOpenFile { source: err })
    }

    pub fn open_file(&self, path: impl AsRef<Path>) -> Result<direct_io::File> {
        self.file_system
            .open(path)
            .map_err(|err| Error::UnableToOpenFile { source: err })
    }

    pub fn create_file(&self, path: impl AsRef<Path>) -> Result<direct_io::File> {
        self.file_system
            .create(path)
            .map_err(|err| Error::UnableToOpenFile { source: err })
    }

    pub async fn sync_all(&self, sync: direct_io::FileSync) -> Result<()> {
        self.file_system
            .sync_all(sync)
            .map_err(|err| Error::UnableToSyncFile { source: err })
    }

    pub async fn sync_data(&self, sync: direct_io::FileSync) -> Result<()> {
        self.file_system
            .sync_data(sync)
            .map_err(|err| Error::UnableToSyncFile { source: err })
    }

    pub async fn write_at(&self, file: direct_io::File, pos: u64, size: u64) {}

    pub async fn read_at(&self, file: direct_io::File, pos: u64, size: u64) {}

    fn put_io_task(&self, task: IoTask) -> Result<()> {
        if !self.async_rt.is_closed() {
            return Err(Error::Cancel);
        }
        if task.is_pri_high() {
            let _ = self.async_rt.high_op_queue.push(task);
        } else if task.task_type == TaskType::BackRead {
            let _ = self.async_rt.read_queue.push(task);
        } else if task.task_type == TaskType::BackWrite {
            let _ = self.async_rt.write_queue.push(task);
        }
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::FileManager;

    #[test]
    fn test() {
        let file_manager_1 = FileManager::get_instance();
        println!("0x{:X}", file_manager_1 as *const FileManager as usize);
        let file_manager_2 = FileManager::get_instance();
        println!("0x{:X}", file_manager_2 as *const FileManager as usize);
        assert_eq!(
            file_manager_1 as *const FileManager as usize,
            file_manager_2 as *const FileManager as usize
        );

        let file_manager_3 = FileManager::new();
        println!("0x{:X}", &file_manager_3 as *const FileManager as usize);
        assert_ne!(
            file_manager_1 as *const FileManager as usize,
            &file_manager_3 as *const FileManager as usize
        );
    }
}
