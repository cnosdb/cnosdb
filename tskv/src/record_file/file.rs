use std::{
    fs,
    path::{Path, PathBuf},
};

use super::*;
use crate::Result;

pub fn open_file(path: &Path) -> Result<file_system::DmaFile> {
    file_manager::get_file_manager().open_file_with(
        path,
        fs::OpenOptions::new().read(true).write(true).create(true),
    )
}
