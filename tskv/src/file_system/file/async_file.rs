use std::borrow::BorrowMut;
use std::fs::{File as StdFile, OpenOptions};
use std::io::{Bytes, Error, ErrorKind, IoSlice, Result, SeekFrom};
use std::mem::MaybeUninit;
use std::ops::Deref;
use std::os::unix::fs::MetadataExt;
use std::os::unix::io::{FromRawFd, OwnedFd, RawFd};
use std::os::unix::prelude::AsRawFd;
use std::path::Path;
use std::ptr::null;
use std::rc::Rc;
use std::sync::Arc;
use std::{io, slice, thread};

use async_trait::async_trait;
use futures::{AsyncSeekExt, AsyncWriteExt};
#[cfg(feature = "io_uring")]
use rio::Rio;

use libc::{send, time};
#[cfg(feature = "io_uring")]
use snafu::ResultExt;
use tokio::fs::File;
use tokio::spawn;
use tokio::task::spawn_blocking;

use trace::{error, info};

use crate::file_system::file::os::{check_err, open, pread, pwrite, FileId};
use crate::Options;

#[derive(Debug)]
#[cfg(not(feature = "io_uring"))]
pub struct RawFile(Arc<StdFile>);

#[derive(Debug)]
#[cfg(feature = "io_uring")]
pub struct RawFile(Arc<StdFile>, Arc<Rio>);

impl RawFile {
    #[cfg(not(feature = "io_uring"))]
    fn as_raw_fd(&self) -> RawFd {
        return self.0.as_raw_fd();
    }
    async fn pwrite(&self, pos: u64, data: &[u8]) -> Result<usize> {
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

    async fn pread(&self, pos: u64, data: &mut [u8]) -> Result<usize> {
        #[cfg(feature = "io_uring")]
        {
            let completion = self.1.read_at(&self.0, &data, pos).await?;
            Ok(data.len())
        }
        #[cfg(not(feature = "io_uring"))]
        {
            let contents = data.to_owned();
            let file = self.0.as_raw_fd();
            let len = data.len();
            let ptr = data.as_mut_ptr() as u64;
            let buf = asyncify(move || pread(file, pos, len, ptr)).await?;
            Ok(len)
        }
    }

    async fn sync_data(&self) -> Result<()> {
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

    async fn truncate(&self, size: u64) -> Result<()> {
        #[cfg(feature = "io_uring")]
        unsafe {
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

pub(crate) async fn asyncify<F, T>(f: F) -> Result<T>
where
    F: FnOnce() -> Result<T> + Send + 'static,
    T: Send + 'static,
{
    match spawn_blocking(f).await {
        Ok(res) => res,
        Err(e) => Err(Error::new(
            ErrorKind::Other,
            format!("background task failed: {:?}", e),
        )),
    }
}

unsafe impl Send for FsRuntime {}

pub struct FsRuntime {
    #[cfg(feature = "io_uring")]
    rio: Arc<Rio>,
}

impl FsRuntime {
    #[cfg(feature = "io_uring")]
    pub fn new_runtime() -> Self {
        let rio = Arc::new(rio::new().unwrap());
        FsRuntime { rio }
    }
    #[cfg(not(feature = "io_uring"))]
    pub fn new_runtime() -> Self {
        FsRuntime {}
    }
}

#[async_trait]
pub trait IFile: Send {
    async fn write_vec<'a>(&self, pos: u64, bufs: &'a mut [IoSlice<'a>]) -> Result<usize>;
    async fn write_at(&self, pos: u64, data: &[u8]) -> Result<usize>;
    async fn read_at(&self, pos: u64, data: &mut [u8]) -> Result<usize>;
    async fn sync_data(&self) -> Result<()>;
    async fn truncate(&self, size: u64) -> Result<()>;
    fn len(&self) -> u64;
    fn is_empty(&self) -> bool;
}

pub struct AsyncFile {
    inner: RawFile,
    ctx: Arc<FsRuntime>,
    size: u64,
}

impl AsyncFile {}

#[async_trait::async_trait]
impl IFile for AsyncFile {
    async fn write_vec<'a>(&self, pos: u64, bufs: &'a mut [IoSlice<'a>]) -> Result<usize> {
        let mut p = pos;
        for buf in bufs {
            p += self.write_at(p, buf.deref()).await? as u64;
        }
        Ok((p - pos) as usize)
    }
    async fn write_at(&self, pos: u64, data: &[u8]) -> Result<usize> {
        self.inner.pwrite(pos, data).await
    }
    async fn read_at(&self, pos: u64, data: &mut [u8]) -> Result<usize> {
        self.inner.pread(pos, data).await
    }

    async fn sync_data(&self) -> Result<()> {
        self.inner.sync_data().await
    }

    async fn truncate(&self, size: u64) -> Result<()> {
        self.inner.truncate(size).await
    }

    fn len(&self) -> u64 {
        self.size
    }

    fn is_empty(&self) -> bool {
        self.size == 0
    }
}

impl AsyncFile {
    pub async fn open<P: AsRef<Path>>(
        path: P,
        ctx: Arc<FsRuntime>,
        options: OpenOptions,
    ) -> Result<AsyncFile> {
        let path = path.as_ref().to_owned();
        #[cfg(feature = "io_uring")]
        {
            let file = asyncify(move || options.open(path)).await?;
            let mut stat = MaybeUninit::<libc::stat>::zeroed();
            check_err(unsafe { libc::fstat(file.as_raw_fd(), stat.as_mut_ptr()) })?;
            let stat = unsafe { stat.assume_init() };
            let size = stat.st_size as u64;
            let inner = Arc::new(RawFile(file, ctx.rio.clone()));
            Ok(AsyncFile { inner, ctx, size })
        }
        #[cfg(not(feature = "io_uring"))]
        {
            let file = asyncify(move || options.open(path)).await?;
            let inner = RawFile(Arc::new(file));
            let mut stat = MaybeUninit::<libc::stat>::zeroed();
            // asyncify(move || check_err(unsafe { libc::fstat(file.as_raw_fd(), stat.as_mut_ptr()) })).await?;
            check_err(unsafe { libc::fstat(inner.as_raw_fd(), stat.as_mut_ptr()) })?;
            let stat = unsafe { stat.assume_init() };
            let size = stat.st_size as u64;
            // let size = metadata.size() as u64;
            Ok(AsyncFile { inner, ctx, size })
        }
    }
}
