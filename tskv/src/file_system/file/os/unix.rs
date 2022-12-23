use std::os::unix::io::RawFd;
use std::{
    fs::File,
    io::{Error, Result},
    mem::MaybeUninit,
    os::unix::io::AsRawFd,
};

#[cfg(not(target_os = "macos"))]
pub use not_macos::*;

#[cfg(not(target_os = "macos"))]
mod not_macos {
    use std::{fs::OpenOptions, os::unix::fs::OpenOptionsExt, path::Path};

    use super::*;

    pub fn open(path: impl AsRef<Path>, options: OpenOptions) -> Result<File> {
        //let mut options = options.clone();
        // options.custom_flags(libc::O_DIRECT);
        options.open(path)
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct FileId {
    pub dev: libc::dev_t,
    pub inode: libc::ino_t,
}

impl FileId {
    pub fn file_size(file: &File) -> Result<(FileId, u64)> {
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

pub fn pread(file: RawFd, pos: u64, len: usize, ptr: u64) -> Result<usize> {
    check_err_size(unsafe { libc::pread(file, ptr as *mut _, len as _, pos as libc::off_t) })
}

pub fn pwrite(file: RawFd, pos: u64, len: usize, ptr: u64) -> Result<usize> {
    check_err_size(unsafe {
        libc::pwrite(
            file,
            ptr as *const _,
            len as libc::size_t,
            pos as libc::off_t,
        )
    })
}

pub fn check_err(r: libc::c_int) -> Result<libc::c_int> {
    if r == -1 {
        Err(Error::last_os_error())
    } else {
        Ok(r)
    }
}

fn check_err_size(e: libc::ssize_t) -> Result<usize> {
    if e == -1_isize {
        Err(Error::last_os_error())
    } else {
        Ok(e as usize)
    }
}
