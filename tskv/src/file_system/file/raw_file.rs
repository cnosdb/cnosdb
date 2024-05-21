use std::fs::File;
use std::io;
use std::sync::Arc;

use crate::file_system::file;
use crate::file_system::file::os;

#[derive(Debug, Clone)]
#[cfg(not(feature = "io_uring"))]
pub struct RawFile(pub(crate) Arc<File>);

#[derive(Debug, Clone)]
#[cfg(feature = "io_uring")]
struct RawFile(Arc<File>, Arc<rio::Rio>);

impl RawFile {
    pub(crate) fn file_size(&self) -> io::Result<usize> {
        os::file_size(os::fd(self.0.as_ref()))
    }

    pub(crate) async fn pwrite(&self, pos: usize, data: &[u8]) -> io::Result<usize> {
        #[cfg(feature = "io_uring")]
        {
            let completion = self.1.write_at(&self.0, &data, pos).await?;
            Ok(data.len())
        }

        #[cfg(not(feature = "io_uring"))]
        {
            let len = data.len();
            let ptr = data.as_ptr() as u64;
            let fd = os::fd(self.0.as_ref());
            file::asyncify(move || os::pwrite(fd, pos, len, ptr)).await
        }
    }

    pub(crate) async fn pread(&self, pos: usize, data: &mut [u8]) -> io::Result<usize> {
        #[cfg(feature = "io_uring")]
        {
            let completion = self.1.read_at(&self.0, &data, pos).await?;
            Ok(data.len())
        }
        #[cfg(not(feature = "io_uring"))]
        {
            let len = data.len();
            let ptr = data.as_ptr() as u64;
            let fd = os::fd(self.0.as_ref());
            let len = file::asyncify(move || os::pread(fd, pos, len, ptr)).await?;
            Ok(len)
        }
    }

    pub(crate) async fn sync_data(&self) -> io::Result<()> {
        #[cfg(feature = "io_uring")]
        {
            self.1.fsync(&self.0).await?;
            Ok(())
        }
        #[cfg(not(feature = "io_uring"))]
        {
            let file = self.0.clone();
            file::asyncify(move || file.sync_data()).await
        }
    }

    pub(crate) async fn sync_all(&self) -> io::Result<()> {
        #[cfg(feature = "io_uring")]
        {
            self.1.fsync(&self.0).await?;
            Ok(())
        }
        #[cfg(not(feature = "io_uring"))]
        {
            let file = self.0.clone();
            file::asyncify(move || file.sync_all()).await
        }
    }

    pub(crate) async fn truncate(&self, size: u64) -> io::Result<()> {
        #[cfg(feature = "io_uring")]
        {
            let file = self.0.clone();
            asyncify(move || file.set_len(size)).await
        }
        #[cfg(not(feature = "io_uring"))]
        {
            let file = self.0.clone();
            file::asyncify(move || file.set_len(size)).await
        }
    }
}

pub struct FsRuntime {
    #[cfg(feature = "io_uring")]
    rio: Arc<Rio>,
}

unsafe impl Send for FsRuntime {}

impl FsRuntime {
    #[allow(dead_code)]
    pub fn new_runtime() -> Self {
        #[cfg(feature = "io_uring")]
        {
            let rio = Arc::new(rio::new().unwrap());
            FsRuntime { rio }
        }
        #[cfg(not(feature = "io_uring"))]
        {
            FsRuntime {}
        }
    }
}
