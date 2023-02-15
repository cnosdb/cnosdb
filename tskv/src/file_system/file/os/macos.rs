use std::fs::{File, OpenOptions};
use std::io::Result;
use std::os::unix::io::AsRawFd;
use std::path::Path;

use super::unix::check_err;

pub fn open(path: impl AsRef<Path>, options: OpenOptions) -> Result<File> {
    let file = options.open(path)?;
    check_err(unsafe {
        const F_NOCACHE: libc::c_int = 48;
        libc::fcntl(file.as_raw_fd(), F_NOCACHE, 1)
    })?;
    Ok(file)
}
