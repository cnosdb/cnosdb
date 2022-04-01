use std::fs;
use std::path::Path;

use crate::direct_io;
use once_cell::sync::OnceCell;
use snafu::Snafu;

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("Unable to open file: {}", source))]
    UnableToOpenFile { source: std::io::Error },

    #[snafu(display("Unable to write file: {}", source))]
    UnableToWriteBytes { source: std::io::Error },

    #[snafu(display("Unable to sync file: {}", source))]
    UnableToSyncFile { source: std::io::Error },
}

type Result<T> = std::result::Result<T, Error>;

pub struct FileManager {
    file_system: direct_io::FileSystem,
}

impl FileManager {
    fn new() -> Self {
        let fs_options = direct_io::Options::default();
        return Self {
            file_system: direct_io::FileSystem::new(&fs_options),
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

    pub fn sync_all(&self, sync: direct_io::FileSync) -> Result<()> {
        self.file_system
            .sync_all(sync)
            .map_err(|err| Error::UnableToSyncFile { source: err })
    }

    pub fn sync_data(&self, sync: direct_io::FileSync) -> Result<()> {
        self.file_system
            .sync_data(sync)
            .map_err(|err| Error::UnableToSyncFile { source: err })
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
