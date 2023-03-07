use std::convert::TryFrom;
use std::fs::{File, OpenOptions};
use std::io::prelude::*;
use std::io::{Error, Result};
use std::mem::MaybeUninit;
use std::os::windows::fs::OpenOptionsExt;
use std::os::windows::io::AsRawHandle;
use std::path::Path;

use winapi::shared::minwindef::*;
use winapi::um::fileapi::*;
use winapi::um::minwinbase::OVERLAPPED;
use winapi::um::winbase::FILE_FLAG_NO_BUFFERING;

//todo:
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct FileId(u64);

impl FileId {
    pub fn file_size(file: &File) -> Result<(FileId, u64)> {
        let mut info = MaybeUninit::<BY_HANDLE_FILE_INFORMATION>::zeroed();
        check_err(unsafe { GetFileInformationByHandle(file.as_raw_handle(), info.as_mut_ptr()) })?;
        let info = unsafe { info.assume_init() };
        let id = Self(u64::from(info.nFileIndexHigh) << 32 | u64::from(info.nFileIndexLow));
        let len = u64::from(info.nFileSizeHigh) << 32 | u64::from(info.nFileSizeLow);
        Ok((id, len))
    }
}

pub fn open(path: impl AsRef<Path>, options: &OpenOptions) -> Result<File> {
    let mut options = options.clone();
    options.custom_flags(FILE_FLAG_NO_BUFFERING);
    options.open(path)
}

pub fn read_at(file: &File, pos: u64, buf: &mut [u8]) -> Result<usize> {
    let mut bytes: DWORD = 0;
    let mut ov = overlapped(pos);
    check_err(unsafe {
        ReadFile(
            file.as_raw_handle(),
            buf.as_mut_ptr() as LPVOID,
            DWORD::try_from(buf.len()).unwrap(),
            &mut bytes,
            &mut ov,
        )
    })?;
    Ok(usize::try_from(bytes).unwrap())
}

pub fn write_at(file: &File, pos: u64, buf: &[u8]) -> Result<usize> {
    let mut bytes: DWORD = 0;
    let mut ov = overlapped(pos);
    check_err(unsafe {
        ReadFile(
            file.as_raw_handle(),
            buf.as_ptr() as LPVOID,
            DWORD::try_from(buf.len()).unwrap(),
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
