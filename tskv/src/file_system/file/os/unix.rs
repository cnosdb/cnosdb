use std::fs::File;
use std::io::{Error, Result};
use std::mem::MaybeUninit;
use std::os::unix::io::{AsRawFd, RawFd};
use std::sync::Arc;

use crate::file_system::file::async_file::asyncify;

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

#[derive(Debug)]
#[cfg(not(feature = "io_uring"))]
pub struct RawFile(pub Arc<File>);

#[derive(Debug)]
#[cfg(feature = "io_uring")]
pub struct RawFile(Arc<File>, Arc<rio::Rio>);

impl RawFile {
    pub fn file_size(&self) -> Result<u64> {
        let mut stat = MaybeUninit::<libc::stat>::zeroed();
        check_err(unsafe { libc::fstat(self.0.as_raw_fd(), stat.as_mut_ptr()) })?;
        let stat = unsafe { stat.assume_init() };
        Ok(stat.st_size as u64)
    }

    pub async fn pwrite(&self, pos: u64, data: &[u8]) -> Result<usize> {
        #[cfg(feature = "io_uring")]
        {
            let completion = self.1.write_at(&self.0, &data, pos).await?;
            Ok(data.len())
        }

        #[cfg(not(feature = "io_uring"))]
        {
            let len = data.len();
            let ptr = data.as_ptr() as u64;
            let file = self.0.as_raw_fd();
            asyncify(move || pwrite(file, pos, len, ptr)).await
        }
    }

    pub async fn pread(&self, pos: u64, data: &mut [u8]) -> Result<usize> {
        #[cfg(feature = "io_uring")]
        {
            let completion = self.1.read_at(&self.0, &data, pos).await?;
            Ok(data.len())
        }
        #[cfg(not(feature = "io_uring"))]
        {
            let file = self.0.as_raw_fd();
            let len = data.len();
            let ptr = data.as_mut_ptr() as u64;
            let _buf = asyncify(move || pread(file, pos, len, ptr)).await?;
            Ok(len)
        }
    }

    pub async fn sync_data(&self) -> Result<()> {
        #[cfg(feature = "io_uring")]
        {
            self.1.fsync(&self.0).await?;
            Ok(())
        }
        #[cfg(not(feature = "io_uring"))]
        {
            let file = self.0.clone();
            asyncify(move || file.sync_data()).await
        }
    }

    pub async fn truncate(&self, size: u64) -> Result<()> {
        #[cfg(feature = "io_uring")]
        {
            let file = self.0.clone();
            asyncify(move || file.set_len(size)).await
        }
        #[cfg(not(feature = "io_uring"))]
        {
            let file = self.0.clone();
            asyncify(move || file.set_len(size)).await
        }
    }
}
