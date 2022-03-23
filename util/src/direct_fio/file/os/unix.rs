use std::fs::File;
use std::io::{Error, Result};
use std::mem::MaybeUninit;
use std::os::unix::io::AsRawFd;

#[cfg(not(target_os = "macos"))]
mod not_macos {
    use super::*;
    use std::fs::OpenOptions;
    use std::os::unix::fs::OpenOptionsExt;
    use std::path::Path;

    pub fn open(path: impl AsRef<Path>, options: &OpenOptions) -> Result<File> {
        let mut options = options.clone();
        options.custom_flags(libc::O_DIRECT);
        options.open(path)
    }
}

#[cfg(not(target_os = "macos"))]
pub use not_macos::*;

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct FileId {
    dev: libc::dev_t,
    inode: libc::ino_t,
}

impl FileId {
    pub fn of(file: &File) -> Result<(FileId, u64)> {
        let mut stat = MaybeUninit::<libc::stat>::zeroed();
        check_err(unsafe { libc::fstat(file.as_raw_fd(), stat.as_mut_ptr()) })?;
        let stat = unsafe { stat.assume_init() };
        let id = Self {
            dev: stat.st_dev,
            inode: stat.st_ino,
        };
        assert!(stat.st_size >= 0);
        let len = stat.st_size as u64;
        Ok((id, len))
    }
}

pub fn read_at(file: &File, pos: u64, buf: &mut [u8]) -> Result<usize> {
    check_err_size(unsafe {
        libc::pread(
            file.as_raw_fd(),
            buf.as_mut_ptr() as *mut _,
            buf.len() as _,
            pos as libc::off_t,
        )
    })
}

pub fn write_at(file: &File, pos: u64, buf: &[u8]) -> Result<usize> {
    check_err_size(unsafe {
        libc::pwrite(
            file.as_raw_fd(),
            buf.as_ptr() as *const _,
            buf.len() as libc::size_t,
            pos as libc::off_t,
        )
    })
}

pub(super) fn check_err(r: libc::c_int) -> Result<libc::c_int> {
    if r == -1 {
        Err(Error::last_os_error())
    } else {
        Ok(r)
    }
}

fn check_err_size(e: libc::ssize_t) -> Result<usize> {
    if e == -1 as libc::ssize_t {
        Err(Error::last_os_error())
    } else {
        Ok(e as usize)
    }
}
