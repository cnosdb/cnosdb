use std::path::Path;

use async_trait::async_trait;

use crate::file_system::file::stream_reader::FileStreamReader;
use crate::file_system::file::stream_writer::FileStreamWriter;
use crate::file_system::file::{ReadableFile, WritableFile};

pub mod async_filesystem;
pub(crate) mod file;
pub mod file_info;

/// File system operations
/// S3 / HDFS / GCS / Azure / local filesystem
/// on local filesystem  try to support  aoi(linux) / threadpool_io / io_uring(linux) / etc.
///
#[async_trait]
pub trait FileSystem: Send + Sync {
    async fn open_file_reader(
        &self,
        path: impl AsRef<Path>,
    ) -> crate::Result<Box<FileStreamReader>>;
    async fn open_file_writer(
        &self,
        path: impl AsRef<Path>,
    ) -> crate::Result<Box<FileStreamWriter>>;
    fn remove(path: impl AsRef<Path>) -> crate::Result<()>;
    fn rename(old_filename: impl AsRef<Path>, new_filename: impl AsRef<Path>) -> crate::Result<()>;
    fn create_dir_if_not_exists(parent: Option<&Path>) -> crate::Result<()>;

    fn try_exists(path: impl AsRef<Path>) -> bool;

    fn list_dir_names(dir: impl AsRef<Path>) -> Vec<String>;

    fn list_file_names(dir: impl AsRef<Path>) -> Vec<String>;
}
