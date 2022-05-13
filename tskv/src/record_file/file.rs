use std::{fs, path::PathBuf};

use super::*;
use crate::Result;

pub fn open_file(path: &PathBuf) -> Result<direct_io::File> {
    file_manager::get_file_manager().open_file_with(path.clone(),
                                                    &fs::OpenOptions::new().read(true)
                                                                           .write(true)
                                                                           .create(true))
}
