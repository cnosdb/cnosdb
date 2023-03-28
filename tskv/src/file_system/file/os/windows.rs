use std::convert::TryFrom;
use std::fs::File;
use std::io::{Error, Result};
use std::mem::MaybeUninit;
use std::os::windows::io::AsRawHandle;
use std::os::windows::prelude::RawHandle;

use winapi::shared::minwindef::*;
use winapi::um::fileapi::*;
use winapi::um::minwinbase::OVERLAPPED;

pub fn fd(file: &File) -> usize {
    file.as_raw_handle() as usize
}

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

pub fn file_size(raw_handle: usize) -> Result<u64> {
    let mut info = MaybeUninit::<BY_HANDLE_FILE_INFORMATION>::zeroed();
    check_err(unsafe { GetFileInformationByHandle(raw_handle as RawHandle, info.as_mut_ptr()) })?;
    let info = unsafe { info.assume_init() };
    let len = u64::from(info.nFileSizeHigh) << 32 | u64::from(info.nFileSizeLow);
    Ok(len)
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
