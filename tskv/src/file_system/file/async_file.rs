use std::fs::OpenOptions;
use std::io::{Error, ErrorKind, IoSlice, Result};
use std::ops::Deref;
use std::path::Path;
use std::sync::Arc;

use tokio::task::spawn_blocking;

use crate::file_system::file::os::RawFile;
use crate::file_system::file::IFile;

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
            let inner = RawFile(Arc::new(file), ctx.rio.clone());
            let size = inner.file_size()?;
            Ok(AsyncFile { inner, ctx, size })
        }
        #[cfg(not(feature = "io_uring"))]
        {
            let file = asyncify(move || options.open(path)).await?;
            let inner = RawFile(Arc::new(file));
            let size = inner.file_size()?;
            Ok(AsyncFile { inner, ctx, size })
        }
    }
}
