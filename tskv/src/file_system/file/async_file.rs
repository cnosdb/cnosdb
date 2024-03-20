use std::fs::OpenOptions;
use std::io::Result;
use std::path::Path;
use std::sync::Arc;

use super::os;
use crate::file_system::file;
use crate::file_system::file::raw_file::RawFile;
use crate::file_system::file::WritableFile;

pub struct AsyncFile {
    inner: RawFile,
    size: usize,
}

#[async_trait::async_trait]
impl WritableFile for AsyncFile {
    async fn write_at(&mut self, pos: usize, data: &[u8]) -> Result<usize> {
        let len = self.inner.pwrite(pos, data).await?;
        self.size = self.size.max(pos + len);
        Ok(len)
    }

    async fn sync_data(&self) -> Result<()> {
        self.inner.sync_data().await
    }

    async fn sync_all(&self) -> Result<()> {
        self.inner.sync_all().await
    }

    async fn truncate(&self, size: u64) -> Result<()> {
        self.inner.truncate(size).await
    }

    fn file_size(&self) -> usize {
        self.size
    }

    fn is_empty(&self) -> bool {
        self.size == 0
    }
}

#[async_trait::async_trait]
impl file::ReadableFile for AsyncFile {
    async fn read_at(&self, pos: u64, data: &mut [u8]) -> Result<usize> {
        self.inner.pread(pos as usize, data).await
    }

    fn file_size(&self) -> usize {
        self.size
    }
}

impl AsyncFile {
    pub async fn open<P: AsRef<Path>>(path: P, options: OpenOptions) -> Result<AsyncFile> {
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
            let res = file::asyncify(move || {
                let file = options.open(path)?;
                let inner = RawFile(Arc::new(file));
                let size = inner.file_size()?;
                Ok(AsyncFile { inner, size })
            })
            .await?;
            Ok(res)
        }
    }

    pub fn fd(&self) -> usize {
        os::fd(&self.inner.0)
    }
}

#[cfg(test)]
mod test {
    use std::fs::OpenOptions;
    use std::sync::Arc;
    use std::time::Duration;

    use crate::file_system::file::async_file::AsyncFile;
    use crate::file_system::file::raw_file::FsRuntime;
    use crate::file_system::file::WritableFile;

    #[tokio::test]
    #[ignore]
    async fn test() {
        let runtime = Arc::new(FsRuntime::new_runtime());
        let mut opt = OpenOptions::new();
        opt.read(true).write(true).create(true).append(true);
        let file = AsyncFile::open("test.txt", runtime, opt).await.unwrap();
        let start_time = std::time::Instant::now();
        let target_duration = Duration::from_secs(5);
        let mut pos = 0;
        loop {
            let elapsed = start_time.elapsed();
            if elapsed > target_duration {
                break;
            }
            pos += file.write_at(pos, b"hello worldhello worldhello worldhello worldhello worldhello worldhello worldhello worldhello worldh").await.unwrap() as u64;
        }
        println!("write {} bytes", pos);
    }
}
