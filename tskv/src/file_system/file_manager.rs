use std::{
    borrow::BorrowMut,
    fs,
    path::{Path, PathBuf},
    sync::Arc,
};

use futures::channel::oneshot::{self, Sender};
use once_cell::sync::OnceCell;
use parking_lot::Mutex;
use snafu::{ResultExt, Snafu};

use crate::file_system::Options;
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

static INSTANCE: OnceCell<FileManager> = OnceCell::new();

pub struct FileManager {
    file_system: Arc<file_system::FileSystemCache>,
}

pub fn get_file_manager() -> &'static FileManager {
    INSTANCE.get_or_init(FileManager::default)
}

pub fn init_file_manager(fs_options: &mut Options) {
    INSTANCE.get_or_init(|| FileManager::new(fs_options));
}

impl FileManager {
    fn new(fs_options: &Options) -> Self {
        Self {
            file_system: Arc::new(file_system::FileSystemCache::new(fs_options)),
        }
    }

    pub fn open_file_with(
        &self,
        path: impl AsRef<Path>,
        options: &fs::OpenOptions,
    ) -> Result<file_system::DmaFile> {
        self.file_system
            .open_with(&path, options)
            .context(error::OpenFileSnafu {
                path: path.as_ref(),
            })
    }

    pub fn open_file(&self, path: impl AsRef<Path>) -> Result<file_system::DmaFile> {
        self.file_system.open(&path).context(error::OpenFileSnafu {
            path: path.as_ref(),
        })
    }

    pub fn create_file(&self, path: impl AsRef<Path>) -> Result<file_system::DmaFile> {
        if let Some(p) = path.as_ref().parent() {
            if !try_exists(p) {
                std::fs::create_dir_all(p).context(error::IOSnafu)?;
            }
        }
        self.file_system
            .create(&path)
            .context(error::OpenFileSnafu {
                path: path.as_ref(),
            })
    }

    pub fn open_create_file(&self, path: impl AsRef<Path>) -> Result<file_system::DmaFile> {
        if try_exists(path.as_ref()) {
            self.open_file(path)
        } else {
            self.create_file(path)
        }
    }

    pub async fn sync_all(&self, sync: file_system::FileSync) -> Result<()> {
        self.file_system
            .sync_all(sync)
            .context(error::SyncFileSnafu)
    }

    pub async fn sync_data(&self, sync: file_system::FileSync) -> Result<()> {
        self.file_system
            .sync_data(sync)
            .context(error::SyncFileSnafu)
    }
}

impl Default for FileManager {
    fn default() -> Self {
        let fs_options = file_system::Options::default();
        Self {
            file_system: Arc::new(file_system::FileSystemCache::new(&fs_options)),
        }
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
pub fn open_file(path: impl AsRef<Path>) -> Result<file_system::DmaFile> {
    get_file_manager().open_file(path)
}

#[inline(always)]
pub fn create_file(path: impl AsRef<Path>) -> Result<file_system::DmaFile> {
    get_file_manager().create_file(path)
}

#[inline(always)]
pub fn open_create_file(path: impl AsRef<Path>) -> Result<file_system::DmaFile> {
    get_file_manager().open_create_file(path)
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use futures::channel::oneshot;
    use tokio::runtime::Builder;
    use trace::info;

    use super::FileManager;
    use crate::file_system::file_manager;
    use crate::file_system::FileSync;

    #[test]
    fn test_get_instance() {
        let file_manager_1 = file_manager::get_file_manager();
        info!("0x{:X}", file_manager_1 as *const FileManager as usize);
        let file_manager_2 = file_manager::get_file_manager();
        info!("0x{:X}", file_manager_2 as *const FileManager as usize);
        assert_eq!(
            file_manager_1 as *const FileManager as usize,
            file_manager_2 as *const FileManager as usize
        );

        let file_manager_3 = FileManager::default();
        info!("0x{:X}", &file_manager_3 as *const FileManager as usize);
        assert_ne!(
            file_manager_1 as *const FileManager as usize,
            &file_manager_3 as *const FileManager as usize
        );
    }
}
