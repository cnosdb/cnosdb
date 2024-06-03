#[cfg(unix)]
mod unix;

#[cfg(windows)]
mod windows;

#[cfg(unix)]
pub use unix::*;
#[cfg(windows)]
pub use windows::*;

pub fn pread_exact(raw_fd: usize, pos: usize, len: usize, ptr: u64) -> std::io::Result<()> {
    let mut read = 0;
    while read < len {
        let r = pread(raw_fd, pos + read, len - read, ptr + read as u64)?;
        if r == 0 {
            return Err(std::io::Error::from_raw_os_error(libc::EOF));
        }
        read += r;
    }
    Ok(())
}

pub fn pwrite_all(raw_fd: usize, pos: usize, len: usize, ptr: u64) -> std::io::Result<()> {
    let mut written = 0;
    while written < len {
        let w = pwrite(raw_fd, pos + written, len - written, ptr + written as u64)?;
        written += w;
    }
    Ok(())
}
