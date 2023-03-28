use std::convert::TryFrom;
use std::fs::File;
use std::io::{Error, Result};
use std::mem::MaybeUninit;
use std::os::windows::io::AsRawHandle;
use std::os::windows::prelude::RawHandle;
use std::sync::Arc;

use winapi::shared::minwindef::*;
use winapi::um::fileapi::*;
use winapi::um::minwinbase::OVERLAPPED;

use crate::file_system::file::async_file::asyncify;

pub fn pread(raw_handle: usize, pos: u64, len: usize, buf_ptr: u64) -> Result<usize> {
    let mut bytes: DWORD = 0;
    let mut ov = overlapped(pos);
    check_err(unsafe {
        ReadFile(
            raw_handle as RawHandle,
            buf_ptr as LPVOID,
            DWORD::try_from(len).unwrap(),
            &mut bytes,
            &mut ov,
        )
    })?;
    Ok(usize::try_from(bytes).unwrap())
}

pub fn pwrite(raw_handle: usize, pos: u64, len: usize, buf_ptr: u64) -> Result<usize> {
    let mut bytes: DWORD = 0;
    let mut ov = overlapped(pos);
    check_err(unsafe {
        WriteFile(
            raw_handle as RawHandle,
            buf_ptr as LPVOID,
            DWORD::try_from(len).unwrap(),
            &mut bytes,
            &mut ov,
        )
    })?;
    Ok(bytes as usize)
}

fn overlapped(pos: u64) -> OVERLAPPED {
    unsafe {
        let mut r: OVERLAPPED = std::mem::zeroed();
        r.u.s_mut().Offset = pos as u32;
        r.u.s_mut().OffsetHigh = (pos >> 32) as u32;
        r
    }
}

fn check_err(r: BOOL) -> Result<()> {
    if r == FALSE {
        Err(Error::last_os_error())
    } else {
        Ok(())
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
        let mut info = MaybeUninit::<BY_HANDLE_FILE_INFORMATION>::zeroed();
        check_err(unsafe {
            GetFileInformationByHandle(self.0.as_raw_handle(), info.as_mut_ptr())
        })?;
        let info = unsafe { info.assume_init() };
        let len = u64::from(info.nFileSizeHigh) << 32 | u64::from(info.nFileSizeLow);
        Ok(len)
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
            let fd = self.0.as_raw_handle() as usize;
            asyncify(move || pwrite(fd, pos, len, ptr)).await
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
            let len = data.len();
            let ptr = data.as_ptr() as u64;
            let fd = self.0.as_raw_handle() as usize;
            let len = asyncify(move || pread(fd, pos, len, ptr)).await?;
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
