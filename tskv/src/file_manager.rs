use std::fs;
use std::path::Path;

use snafu::Snafu;
use util::direct_fio;

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
    file_system: direct_fio::FileSystem,
}

impl FileManager {
    pub fn new() -> Self {
        let fs_options = direct_fio::Options::default();
        return Self {
            file_system: direct_fio::FileSystem::new(&fs_options),
        };
    }

    pub fn open_with(
        &self,
        path: impl AsRef<Path>,
        options: &fs::OpenOptions,
    ) -> Result<direct_fio::File> {
        self.file_system
            .open_with(path, options)
            .map_err(|err| Error::UnableToOpenFile { source: err })
    }

    pub fn open(&self, path: impl AsRef<Path>) -> Result<direct_fio::File> {
        self.file_system
            .open(path)
            .map_err(|err| Error::UnableToOpenFile { source: err })
    }

    pub fn create(&self, path: impl AsRef<Path>) -> Result<direct_fio::File> {
        self.file_system
            .create(path)
            .map_err(|err| Error::UnableToOpenFile { source: err })
    }

    pub fn sync_all(&self, sync: direct_fio::FileSync) -> Result<()> {
        self.file_system
            .sync_all(sync)
            .map_err(|err| Error::UnableToSyncFile { source: err })
    }

    pub fn sync_data(&self, sync: direct_fio::FileSync) -> Result<()> {
        self.file_system
            .sync_data(sync)
            .map_err(|err| Error::UnableToSyncFile { source: err })
    }
}
