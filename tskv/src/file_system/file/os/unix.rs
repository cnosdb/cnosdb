use std::fs::File;
use std::io::{Error, Result};
use std::mem::MaybeUninit;
use std::os::unix::io::{AsRawFd, RawFd};

pub fn fd(file: &File) -> usize {
    file.as_raw_fd() as usize
}

pub fn pread(raw_fd: usize, pos: u64, len: usize, ptr: u64) -> Result<usize> {
    check_err_size(unsafe {
        libc::pread(raw_fd as RawFd, ptr as *mut _, len as _, pos as libc::off_t)
    })
}

pub fn pwrite(raw_fd: usize, pos: u64, len: usize, ptr: u64) -> Result<usize> {
    check_err_size(unsafe {
        libc::pwrite(
            raw_fd as RawFd,
            ptr as *const _,
            len as libc::size_t,
            pos as libc::off_t,
        )
    })
}

pub fn file_size(raw_fd: usize) -> Result<u64> {
    let mut stat = MaybeUninit::<libc::stat>::zeroed();
    check_err(unsafe { libc::fstat(raw_fd as RawFd, stat.as_mut_ptr()) })?;
    let stat = unsafe { stat.assume_init() };
    Ok(stat.st_size as u64)
}

fn check_err(r: libc::c_int) -> Result<libc::c_int> {
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
