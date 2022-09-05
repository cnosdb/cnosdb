pub mod cursor;
mod os;
mod scope;
pub mod system;

use std::{
    cmp,
    convert::TryFrom,
    fs::OpenOptions,
    io::Result,
    ops::{Deref, DerefMut},
    path::Path,
    sync::Arc,
};

use cursor::FileCursor;
use dashmap::DashMap;
use os::*;
use scope::FileScope;
use static_assertions::*;

use crate::direct_io::cache::{self, PageId, Scope};

type CacheHandle = cache::CacheHandle<FileScope>;
type ScopeHandle = cache::ScopeHandle<FileScope>;
type WeakScopeHandle = cache::WeakScopeHandle<FileScope>;

type ScopeMap = DashMap<FileId, WeakScopeHandle>;

/// Desired file synchronization level.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum FileSync {
    /// File data (and possibly metadata) is written to OS. This doesn't guarantee any physical
    /// writes to the underlying storage immediately.
    Soft,

    /// File data (and possibly metadata) is written to OS and is OS is asked to sync the data to
    /// storage (for example, with `fsync`).
    Hard,
}

#[derive(Clone)]
pub struct File {
    scope: ScopeHandle,
}

assert_impl_all!(File: Send);
assert_not_impl_any!(File: Sync);

impl File {
    fn new(scope: ScopeHandle) -> Self {
        Self { scope }
    }

    pub fn max_page_len(&self) -> usize {
        self.scope.cache().page_len()
    }

    pub fn page_id_at(&self, pos: u64) -> (PageId, usize) {
        let l = self.max_page_len() as u64;
        let id = cache::PageId::try_from(pos / l).unwrap();
        let offset = usize::try_from(pos % l).unwrap();
        (id, offset)
    }

    pub fn page(&self, id: PageId) -> Result<PageRef> {
        self.scope.page(id).map(PageRef)
    }

    pub fn len(&self) -> u64 {
        self.scope.get().len()
    }

    pub fn set_len(&self, len: u64) {
        self.scope.get().set_len(len);
    }

    pub fn discard(&self) {
        self.scope.discard();
    }

    pub fn sync_all(&self, sync: FileSync) -> Result<()> {
        scope::sync_all(&self.scope, sync)
    }

    pub fn sync_data(&self, sync: FileSync) -> Result<()> {
        scope::sync_data(&self.scope, sync)
    }

    pub fn read_at(&self, mut pos: u64, mut buf: &mut [u8]) -> Result<usize> {
        let mut read = 0_usize;
        while !buf.is_empty() {
            let (id, offset) = self.page_id_at(pos);
            let page = self.page(id)?;
            let page = page.read();
            let page = &page[offset..];
            if page.is_empty() {
                break;
            }
            let len = cmp::min(page.len(), buf.len());
            buf[..len].copy_from_slice(&page[..len]);
            read = read.checked_add(len).unwrap();

            buf = &mut buf[len..];
            pos = pos.checked_add(len as u64).unwrap();
        }
        Ok(read)
    }

    pub fn write_at(&self, mut pos: u64, mut buf: &[u8]) -> Result<usize> {
        let len = buf.len();
        while !buf.is_empty() {
            let (id, offset) = self.page_id_at(pos);
            let page = self.page(id)?;
            let mut page = page.write();
            let len = cmp::min(buf.len(), self.max_page_len() - offset);
            page.set_len(offset + len);
            let page = &mut page[offset..];
            page[..len].copy_from_slice(&buf[..len]);

            buf = &buf[len..];
            pos = pos.checked_add(len as u64).unwrap();
        }
        Ok(len)
    }

    pub fn into_cursor(self) -> FileCursor {
        self.into()
    }
}

impl PartialEq for File {
    fn eq(&self, other: &Self) -> bool {
        self.scope.get().id() == other.scope.get().id()
    }
}

impl Eq for File {}

impl From<FileCursor> for File {
    fn from(v: FileCursor) -> Self {
        v.into_file()
    }
}

pub struct PageReadGuard<'a> {
    page: &'a PageRef,
    inner: cache::PageReadGuard<'a>,
    len: usize,
}

assert_not_impl_any!(PageReadGuard: Send, Sync);

impl PageReadGuard<'_> {
    pub fn max_len(&self) -> usize {
        self.page.0.len()
    }

    pub fn is_dirty(&self) -> bool {
        self.inner.is_dirty()
    }
}

impl Deref for PageReadGuard<'_> {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &self.inner[..self.len]
    }
}

pub struct PageWriteGuard<'a> {
    page: &'a PageRef,
    inner: cache::PageWriteGuard<'a>,
    old_len: usize,
    len: usize,
}

assert_not_impl_any!(PageWriteGuard: Send, Sync);

impl PageWriteGuard<'_> {
    pub fn max_len(&self) -> usize {
        self.page.0.len()
    }

    pub fn is_dirty(&self) -> bool {
        self.inner.is_dirty()
    }

    pub fn set_dirty(&mut self, dirty: bool) {
        self.inner.set_dirty(dirty);
    }

    pub fn set_len(&mut self, len: usize) {
        assert!(len <= self.max_len());
        if self.len < len {
            self.len = len;
        }
    }
}

impl Deref for PageWriteGuard<'_> {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &self.inner[..self.len]
    }
}

impl DerefMut for PageWriteGuard<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner[..self.len]
    }
}

impl Drop for PageWriteGuard<'_> {
    fn drop(&mut self) {
        if self.len == self.old_len {
            return;
        }

        let pos = FileScope::page_pos(self.page.0.id(), self.page.0.len());
        let file_len = self.page.0.scope().len();
        let new_file_len = pos + self.len as u64;

        let update = if self.len > self.old_len {
            file_len < new_file_len
        } else {
            file_len > new_file_len
        };
        if update {
            self.page.0.scope().set_len(new_file_len);
        }
    }
}

#[derive(Clone)]
pub struct PageRef(cache::PageRef<FileScope>);

assert_impl_all!(PageRef: Send, Sync);

impl PageRef {
    pub fn read(&self) -> PageReadGuard<'_> {
        let inner = self.0.read();
        let (_, len) = self.span();
        PageReadGuard {
            page: self,
            inner,
            len,
        }
    }

    pub fn try_read(&self) -> Option<PageReadGuard> {
        let inner = self.0.try_read()?;
        let (_, len) = self.span();
        Some(PageReadGuard {
            page: self,
            inner,
            len,
        })
    }

    pub fn write(&self) -> PageWriteGuard<'_> {
        let inner = self.0.write();
        let (_, len) = self.span();
        PageWriteGuard {
            page: self,
            inner,
            old_len: len,
            len,
        }
    }

    pub fn try_write(&self) -> Option<PageWriteGuard> {
        let inner = self.0.try_write()?;
        let (_, len) = self.span();
        Some(PageWriteGuard {
            page: self,
            inner,
            old_len: len,
            len,
        })
    }

    fn span(&self) -> (u64, usize) {
        self.0.scope().page_span(self.0.id(), self.0.len())
    }
}

#[cfg(test)]
mod test {
    use std::{
        fs::File as StdFile,
        io::{prelude::*, SeekFrom},
    };

    use tempfile::NamedTempFile;

    use crate::direct_io::*;

    fn new(max_resident: usize, max_non_resident: usize, page_len_scale: usize) -> FileSystem {
        FileSystem::new(
            Options::default()
                .max_resident(max_resident)
                .max_non_resident(max_non_resident)
                .page_len_scale(page_len_scale),
        )
    }

    fn file_len(file: &StdFile) -> u64 {
        file.metadata().unwrap().len()
    }

    fn read_all(file: &mut StdFile) -> Vec<u8> {
        let mut r = Vec::new();
        file.read_to_end(&mut r).unwrap();
        r
    }

    fn read_vec(file: &mut StdFile, len: usize) -> Vec<u8> {
        let mut r = Vec::new();
        r.resize(len, 0);
        file.read_exact(&mut r).unwrap();
        r
    }

    fn read_vec_at(file: &File, pos: u64, len: usize) -> Vec<u8> {
        let mut r = Vec::new();
        r.resize(len, 0);
        let read = file.read_at(pos, &mut r).unwrap();
        r.truncate(read);
        r
    }

    #[test]
    fn write_far_page() {
        let fs = new(1, 0, 1);
        let page_len = fs.max_page_len() as u64;

        let mut tmpf = NamedTempFile::new().unwrap();
        {
            let f = fs.open(tmpf.path()).unwrap();
            f.write_at(page_len, &[1, 2, 3]).unwrap();

            assert_eq!(&read_vec_at(&f, page_len, 3), &[1, 2, 3]);
        }

        fs.sync_data(FileSync::Soft).unwrap();
        assert_eq!(file_len(tmpf.as_file()), page_len * 2);

        fs.sync_all(FileSync::Soft).unwrap();
        assert_eq!(file_len(tmpf.as_file()), page_len + 3);
        {
            let f = tmpf.as_file_mut();
            f.seek(SeekFrom::End(-3)).unwrap();
            assert_eq!(read_vec(f, 3), &[1, 2, 3]);
        }
    }

    #[test]
    fn read_write_on_non_boundary() {
        let mut tmpf = {
            let fs = new(1, 0, 1);
            assert!(fs.max_page_len() > 100);

            let tmpf = NamedTempFile::new().unwrap();
            let tmpf2 = NamedTempFile::new().unwrap();

            {
                let f = fs.open(tmpf.path()).unwrap();

                f.write_at(0, &[1]).unwrap();
                assert_eq!(f.len(), 1);
                assert_eq!(file_len(tmpf.as_file()), 0);

                f.sync_data(FileSync::Soft).unwrap();
                assert_eq!(f.len(), 1);
                assert_eq!(file_len(tmpf.as_file()), fs.max_page_len() as u64);

                f.sync_all(FileSync::Soft).unwrap();
                assert_eq!(f.len(), 1);
                assert_eq!(file_len(tmpf.as_file()), 1);
            }

            {
                let f = fs.open(tmpf.path()).unwrap();
                f.write_at(1, &[2]).unwrap();

                f.sync_data(FileSync::Soft).unwrap();
                assert_eq!(file_len(tmpf.as_file()), fs.max_page_len() as u64);
            }

            {
                let f = fs.open(tmpf2.path()).unwrap();
                // This replaces the only page and drops the file scope which syncs the physical
                // length in separate task.
                f.write_at(0, &[1]).unwrap();
                f.discard();
            }

            tmpf
        };
        std::thread::sleep(std::time::Duration::from_millis(500));
        assert_eq!(file_len(tmpf.as_file()), 2);
        assert_eq!(read_all(tmpf.as_file_mut()), vec![1, 2]);
    }
}
