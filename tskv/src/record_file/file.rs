use crate::file_system::AsyncFile;
use std::{
    fs,
    path::{Path, PathBuf},
};

use super::*;
use crate::Result;

pub async fn open_file(path: &Path) -> Result<AsyncFile> {
    file_manager::get_file_manager()
        .open_file_with(
            path,
            fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .clone(),
        )
        .await
}
