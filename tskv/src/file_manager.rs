use std::{
    borrow::BorrowMut,
    fs,
    path::{Path, PathBuf},
    sync::Arc,
    thread,
    thread::JoinHandle,
};

use futures::channel::oneshot::{self, Sender};
use once_cell::sync::OnceCell;
use parking_lot::Mutex;
use snafu::{ResultExt, Snafu};

use crate::{
    direct_io::{self, make_io_task, run_io_task, AsyncContext, File, IoTask, TaskType},
    error, Error, Result,
};

#[derive(Snafu, Debug)]
pub enum FileError {
    #[snafu(display("Unable to open file: {}", source))]
    UnableToOpenFile { source: std::io::Error },

    #[snafu(display("Unable to write file: {}", source))]
    UnableToWriteBytes { source: std::io::Error },

    #[snafu(display("Unable to sync file: {}", source))]
    UnableToSyncFile { source: std::io::Error },

    #[snafu(display("async file system stopped"))]
    Cancel,
}

pub struct FileManager {
    file_system: Arc<direct_io::FileSystem>,
    async_rt: Arc<AsyncContext>,
    thread_pool: Mutex<Vec<thread::JoinHandle<()>>>,
}

pub fn get_file_manager() -> &'static FileManager {
    static INSTANCE: OnceCell<FileManager> = OnceCell::new();
    INSTANCE.get_or_init(FileManager::new)
}

impl FileManager {
    fn new() -> Self {
        let fs_options = direct_io::Options::default();
        let thread_num = fs_options.get_thread_num();
        let rt = Arc::new(AsyncContext::new(fs_options.get_thread_num()));
        let mut pool = Vec::new();
        for i in 0..thread_num {
            let mrt = rt.clone();
            let h = thread::Builder::new().name("AsyncIOThread_".to_string() + &i.to_string())
                                          .spawn(move || run_io_task(mrt, i))
                                          .unwrap();
            pool.push(h);
        }

        Self { file_system: Arc::new(direct_io::FileSystem::new(&fs_options)),
               async_rt: rt,
               thread_pool: Mutex::new(pool) }
    }

    pub fn open_file_with(&self,
                          path: impl AsRef<Path>,
                          options: &fs::OpenOptions)
                          -> Result<direct_io::File> {
        self.file_system.open_with(path, options).context(error::OpenFileSnafu)
    }

    pub fn open_file(&self, path: impl AsRef<Path>) -> Result<direct_io::File> {
        self.file_system.open(path).context(error::OpenFileSnafu)
    }

    pub fn create_file(&self, path: impl AsRef<Path>) -> Result<direct_io::File> {
        self.file_system.create(path).context(error::OpenFileSnafu)
    }

    pub fn open_create_file(&self, path: impl AsRef<Path>) -> Result<direct_io::File> {
        if try_exists(path.as_ref()) { self.open_file(path) } else { self.create_file(path) }
    }

    pub async fn sync_all(&self, sync: direct_io::FileSync) -> Result<()> {
        self.file_system.sync_all(sync).context(error::SyncFileSnafu)
    }

    pub async fn sync_data(&self, sync: direct_io::FileSync) -> Result<()> {
        self.file_system.sync_data(sync).context(error::SyncFileSnafu)
    }

    pub async fn write_at(&self, file: Arc<direct_io::File>, pos: u64, buf: &mut [u8]) {
        let (cb, rx) = oneshot::channel::<crate::error::Result<usize>>();
        let task = make_io_task(TaskType::FrontWrite, buf.as_mut_ptr(), buf.len(), pos, file, cb);

        self.put_io_task(task).unwrap();

        self.async_rt.try_wakeup();

        let ret = rx.await.unwrap();
    }

    pub async fn read_at(&self, file: direct_io::File, pos: u64, size: u64) {}

    pub fn put_io_task(&self, task: IoTask) -> Result<()> {
        if self.async_rt.is_closed() {
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

pub fn list_file_names(dir: impl AsRef<Path>) -> Vec<String> {
    let mut list = Vec::new();

    for file_name in
        walkdir::WalkDir::new(dir).min_depth(1)
                                  .max_depth(1)
                                  .sort_by_file_name()
                                  .into_iter()
                                  .filter_map(|e| {
                                      let dir_entry = match e {
                                          Ok(dir_entry) if dir_entry.file_type().is_file() => {
                                              dir_entry
                                          },
                                          _ => {
                                              return None;
                                          },
                                      };
                                      dir_entry.file_name()
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
pub fn open_file(path: impl AsRef<Path>) -> Result<direct_io::File> {
    get_file_manager().open_file(path)
}

#[inline(always)]
pub fn create_file(path: impl AsRef<Path>) -> Result<direct_io::File> {
    get_file_manager().create_file(path)
}

#[inline(always)]
pub fn open_create_file(path: impl AsRef<Path>) -> Result<direct_io::File> {
    get_file_manager().open_create_file(path)
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use futures::channel::oneshot;
    use logger::info;
    use tokio::runtime::Builder;

    use super::FileManager;
    use crate::{
        direct_io::{make_io_task, FileSync, TaskType},
        file_manager,
    };

    #[test]
    fn test_get_instance() {
        let file_manager_1 = file_manager::get_file_manager();
        info!("0x{:X}", file_manager_1 as *const FileManager as usize);
        let file_manager_2 = file_manager::get_file_manager();
        info!("0x{:X}", file_manager_2 as *const FileManager as usize);
        assert_eq!(file_manager_1 as *const FileManager as usize,
                   file_manager_2 as *const FileManager as usize);

        let file_manager_3 = FileManager::new();
        info!("0x{:X}", &file_manager_3 as *const FileManager as usize);
        assert_ne!(file_manager_1 as *const FileManager as usize,
                   &file_manager_3 as *const FileManager as usize);
    }

    #[tokio::test]
    #[ignore]
    async fn test_io_task() {
        let file_manager = file_manager::get_file_manager();

        let mut buf = vec![1_u8; 1024];
        let file = file_manager.create_file("./a.hex").unwrap();

        let (cb, rx) = oneshot::channel::<crate::error::Result<usize>>();
        let task =
            make_io_task(TaskType::FrontWrite, buf.as_mut_ptr(), buf.len(), 0, Arc::new(file), cb);

        file_manager.put_io_task(task).unwrap();

        file_manager.async_rt.try_wakeup();

        let ret = rx.await.unwrap();
        file_manager.sync_all(FileSync::Hard).await.unwrap();
        ret.unwrap();
    }
    #[test]
    fn test_file() {
        let file_manager = file_manager::get_file_manager();
        let rt = Builder::new_current_thread()
                                              // let rt = Builder::new_multi_thread()
                                              .enable_all()
                                              .build()
                                              .unwrap();
        rt.block_on(async move {
              let mut buf = vec![1_u8; 1024];
              let file = Arc::new(file_manager.create_file("./test_lyt.log").unwrap());

              file_manager.write_at(file.clone(), 0, &mut buf[..]).await;
          });
    }
}
