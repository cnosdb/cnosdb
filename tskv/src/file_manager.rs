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

use crate::{
    direct_io::{self},
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
}

pub fn get_file_manager() -> &'static FileManager {
    static INSTANCE: OnceCell<FileManager> = OnceCell::new();
    INSTANCE.get_or_init(FileManager::new)
}

impl FileManager {
    fn new() -> Self {
        let fs_options = direct_io::Options::default();
        Self {
            file_system: Arc::new(direct_io::FileSystem::new(&fs_options)),
        }
    }

    pub fn open_file_with(
        &self,
        path: impl AsRef<Path>,
        options: &fs::OpenOptions,
    ) -> Result<direct_io::File> {
        self.file_system
            .open_with(path, options)
            .context(error::OpenFileSnafu)
    }

    pub fn open_file(&self, path: impl AsRef<Path>) -> Result<direct_io::File> {
        self.file_system.open(path).context(error::OpenFileSnafu)
    }

    pub fn create_file(&self, path: impl AsRef<Path>) -> Result<direct_io::File> {
        if let Some(p) = path.as_ref().parent() {
            if !try_exists(p) {
                std::fs::create_dir_all(p).context(error::IOSnafu)?;
            }
        }
        self.file_system.create(path).context(error::OpenFileSnafu)
    }

    pub fn open_create_file(&self, path: impl AsRef<Path>) -> Result<direct_io::File> {
        if try_exists(path.as_ref()) {
            self.open_file(path)
        } else {
            self.create_file(path)
        }
    }

    pub async fn sync_all(&self, sync: direct_io::FileSync) -> Result<()> {
        self.file_system
            .sync_all(sync)
            .context(error::SyncFileSnafu)
    }

    pub async fn sync_data(&self, sync: direct_io::FileSync) -> Result<()> {
        self.file_system
            .sync_data(sync)
            .context(error::SyncFileSnafu)
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
    use tokio::runtime::Builder;
    use trace::info;

    use super::FileManager;
    use crate::{direct_io::FileSync, file_manager};

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

        let file_manager_3 = FileManager::new();
        info!("0x{:X}", &file_manager_3 as *const FileManager as usize);
        assert_ne!(
            file_manager_1 as *const FileManager as usize,
            &file_manager_3 as *const FileManager as usize
        );
    }
}
