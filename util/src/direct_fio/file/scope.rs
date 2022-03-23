use std::cmp;
use std::fs::File as StdFile;
use std::io::{ErrorKind, Result};
use std::sync::atomic::*;
use std::sync::{Arc, Weak};

use crate::direct_fio::file::*;

pub struct FileScope {
    scope_map: Weak<ScopeMap>,
    id: FileId,
    file: Option<Arc<StdFile>>,
    len: AtomicU64,
}

impl FileScope {
    pub fn new(
        cache: &CacheHandle,
        scope_map: &Arc<ScopeMap>,
        file: StdFile,
        id: FileId,
        len: u64,
    ) -> Result<ScopeHandle> {
        let scope = Self {
            scope_map: Arc::downgrade(scope_map),
            id,
            file: Some(Arc::new(file)),
            len: len.into(),
        };

        let scope = cache.new_scope(scope);

        let v = scope_map.insert(id, scope.downgrade());
        debug_assert!(v.is_none());

        Ok(scope)
    }

    pub fn page_pos(id: PageId, page_len: usize) -> u64 {
        (id as u64).checked_mul(page_len as u64).unwrap()
    }

    pub fn page_span(&self, id: PageId, page_len: usize) -> (u64, usize) {
        let file_len = self.len();
        let pos = Self::page_pos(id, page_len);
        let len = if pos < file_len {
            cmp::min(file_len - pos, page_len as u64) as usize
        } else {
            0
        };
        (pos, len)
    }

    pub fn len(&self) -> u64 {
        self.len.load(Ordering::SeqCst)
    }

    pub fn set_len(&self, len: u64) {
        self.len.store(len, Ordering::SeqCst);
    }

    pub fn sync_data(&self) -> Result<()> {
        self.file().sync_data()
    }

    pub fn sync_all(&self) -> Result<()> {
        self.file().sync_all()
    }

    pub fn sync_len(&self) -> Result<()> {
        let file = self.file().clone();
        let len = self.len();
        Self::sync_len0(&file, len)
    }

    pub(in crate) fn id(&self) -> FileId {
        self.id
    }

    fn sync_len0(file: &StdFile, len: u64) -> Result<()> {
        file.set_len(len)
    }

    fn file(&self) -> &Arc<StdFile> {
        self.file.as_ref().unwrap()
    }
}

impl Drop for FileScope {
    fn drop(&mut self) {
        let file = self.file.take().unwrap();
        let len = self.len();
        // TODO log errors
        let _ = Self::sync_len0(&file, len);

        if let Some(scope_map) = self.scope_map.upgrade() {
            scope_map.remove(&self.id).unwrap();
        }
    }
}

impl Scope for FileScope {
    fn read(&self, id: PageId, buf: &mut [u8]) -> Result<()> {
        let (pos, len) = self.page_span(id, buf.len());
        if len > 0 {
            let read = read_all_at(pos, len, buf, |pos, buf| read_at(&self.file(), pos, buf))?;
            if read != buf.len() {
                debug_assert!(read < buf.len());
                let new_len = pos + read as u64;
                self.set_len(new_len);
            }
        }
        Ok(())
    }

    fn write(&self, id: PageId, buf: &[u8]) -> Result<()> {
        let pos = Self::page_pos(id, buf.len());
        write_all_at(pos, &buf, |pos, buf| write_at(&self.file(), pos, buf))?;
        Ok(())
    }
}

pub fn sync_all(scope: &ScopeHandle, sync: FileSync) -> Result<()> {
    scope.flush()?;
    scope.get().sync_len()?;
    if sync == FileSync::Hard {
        scope.get().sync_all()?;
    }
    Ok(())
}

pub fn sync_data(scope: &ScopeHandle, sync: FileSync) -> Result<()> {
    if scope.flush()? > 0 && sync == FileSync::Hard {
        scope.get().sync_data()?;
    }
    Ok(())
}

const BLOCK_ALIGN: usize = 512;

fn read_all_at<F>(
    mut pos: u64,
    expected_len: usize,
    mut buf: &mut [u8],
    read_at: F,
) -> Result<usize>
where
    F: Fn(u64, &mut [u8]) -> Result<usize>,
{
    assert!(expected_len <= buf.len());
    assert_eq!(pos % BLOCK_ALIGN as u64, 0);
    assert_eq!(buf.len() % BLOCK_ALIGN, 0);
    let mut read = 0_usize;
    while !buf.is_empty() && read != expected_len {
        match read_at(pos, buf) {
            Ok(0) => break,
            Ok(n) => {
                let tmp = buf;
                buf = &mut tmp[n..];
                pos = pos.checked_add(n as u64).unwrap();
                read += read.checked_add(n).unwrap();

                // We assume we hit EOF if read ended not on BLOCK_ALIGN.
                if n % BLOCK_ALIGN != 0 {
                    break;
                }
            }
            Err(e) if e.kind() == ErrorKind::Interrupted => {}
            Err(e) => return Err(e),
        }
    }
    Ok(read)
}

fn write_all_at<F>(mut pos: u64, mut buf: &[u8], write_at: F) -> Result<()>
where
    F: Fn(u64, &[u8]) -> Result<usize>,
{
    assert_eq!(pos % BLOCK_ALIGN as u64, 0);
    assert_eq!(buf.len() % BLOCK_ALIGN, 0);
    while !buf.is_empty() {
        match write_at(pos, buf) {
            Ok(0) => break,
            Ok(n) => {
                assert_eq!(n % BLOCK_ALIGN, 0);
                let tmp = buf;
                buf = &tmp[n..];
                pos = pos.checked_add(n as u64).unwrap();
            }
            Err(e) if e.kind() == ErrorKind::Interrupted => {}
            Err(e) => return Err(e),
        }
    }
    Ok(())
}
