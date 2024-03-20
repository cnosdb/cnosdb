use std::fs::{File, OpenOptions};
use std::io::Result;
use std::path::Path;
use std::sync::Arc;
use std::{mem, ptr};

use crate::file_system::file::raw_file::RawFile;
use crate::file_system::file::{asyncify, ReadableFile, WritableFile};

pub struct MmapFile {
    file: RawFile,
    mmap: Arc<memmap2::MmapRaw>,
    size: usize,
}

impl MmapFile {
    pub async fn open<P: AsRef<Path>>(path: P, options: OpenOptions) -> Result<MmapFile> {
        let path = path.as_ref().to_owned();
        let res = asyncify(move || {
            let file = RawFile(Arc::new(options.open(path)?));
            let mmap = Arc::new(memmap2::MmapRaw::map_raw(&file.0)?);
            let size = mmap.len();
            Ok(MmapFile { file, mmap, size })
        })
        .await?;
        Ok(res)
    }
}

#[async_trait::async_trait]
impl WritableFile for MmapFile {
    async fn write_at(&mut self, pos: usize, data: &[u8]) -> Result<usize> {
        let mmap = self.mmap.clone();
        let size = self.size;
        let len = data.len();
        let src = data.as_ptr() as usize;
        let size = asyncify(move || {
            unsafe {
                let memory = std::slice::from_raw_parts_mut(mmap.as_mut_ptr(), size);
                let dst = memory.as_mut_ptr().add(pos);
                ptr::copy_nonoverlapping(mem::transmute(src), dst, len);
            }
            Ok(len)
        })
        .await?;
        self.size = self.size.max(pos + size);
        Ok(size)
    }

    async fn sync_data(&self) -> Result<()> {
        self.file.sync_data().await
    }

    async fn sync_all(&self) -> Result<()> {
        self.file.sync_all().await
    }

    async fn truncate(&self, size: u64) -> Result<()> {
        self.file.truncate(size).await
    }

    fn file_size(&self) -> usize {
        self.size
    }

    fn is_empty(&self) -> bool {
        self.size == 0
    }
}

#[async_trait::async_trait]
impl ReadableFile for MmapFile {
    async fn read_at(&self, pos: u64, data: &mut [u8]) -> Result<usize> {
        let mmap = self.mmap.clone();
        let size = self.size;
        let len = data.len();
        let dst = data.as_ptr() as usize;
        let size = asyncify(move || {
            unsafe {
                let memory = std::slice::from_raw_parts(mmap.as_ptr(), size);
                let src = memory.as_ptr().add(pos as usize);
                ptr::copy_nonoverlapping(src, mem::transmute(dst), len);
            }
            Ok(len)
        })
        .await?;
        Ok(size)
    }

    fn file_size(&self) -> usize {
        self.size
    }
}
