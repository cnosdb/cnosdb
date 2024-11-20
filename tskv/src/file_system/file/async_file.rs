use std::any::Any;
use std::fs::OpenOptions;
use std::io::Result;
use std::path::Path;
use std::sync::Arc;

use crate::file_system::file;
use crate::file_system::file::raw_file::RawFile;
use crate::file_system::file::WritableFile;

#[derive(Clone)]
pub struct AsyncFile {
    inner: RawFile,
    size: usize,
}

#[async_trait::async_trait]
impl WritableFile for AsyncFile {
    async fn write_at(&mut self, pos: usize, data: &[u8]) -> Result<usize> {
        let len = self.inner.write_all_at(pos, data).await?;
        self.size = self.size.max(pos + len);
        Ok(len)
    }

    async fn sync_data(&self) -> Result<()> {
        self.inner.sync_data().await
    }

    async fn sync_all(&self) -> Result<()> {
        self.inner.sync_all().await
    }

    async fn truncate(&mut self, size: u64) -> Result<()> {
        self.size = size as usize;
        self.inner.truncate(size).await
    }

    fn file_size(&self) -> usize {
        self.size
    }

    fn is_empty(&self) -> bool {
        self.size == 0
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[async_trait::async_trait]
impl file::ReadableFile for AsyncFile {
    async fn read_at(&self, pos: usize, data: &mut [u8]) -> Result<usize> {
        self.inner.read_all_at(pos, data).await
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
        unsafe {
            let res = file::asyncify(|| {
                let file = options.open(path)?;
                let inner = RawFile(Arc::new(file));
                let size = inner.file_size()?;
                Ok(AsyncFile { inner, size })
            })
            .await?;
            Ok(res)
        }
    }
}

#[cfg(test)]
mod test {
    use std::fs::OpenOptions;
    use std::sync::Arc;
    use std::time::Duration;

    use crate::file_system::async_filesystem::LocalFileType;
    use crate::file_system::file::raw_file::FsRuntime;
    use crate::file_system::FileSystem;

    #[tokio::test]
    #[ignore]
    async fn test() {
        let _runtime = Arc::new(FsRuntime::new_runtime());
        let mut opt = OpenOptions::new();
        opt.read(true).write(true).create(true).append(true);
        let file_system =
            crate::file_system::async_filesystem::LocalFileSystem::new(LocalFileType::ThreadPool);
        let mut file = file_system
            .open_file_writer("test.txt", 1024)
            .await
            .unwrap();
        let start_time = std::time::Instant::now();
        let target_duration = Duration::from_secs(5);
        let mut pos = 0;
        loop {
            let elapsed = start_time.elapsed();
            if elapsed > target_duration {
                break;
            }
            pos += file.write(b"hello worldhello worldhello worldhello worldhello worldhello worldhello worldhello worldhello worldh").await.unwrap();
        }
        println!("write {} bytes", pos);
    }
}
