pub(crate) mod async_file;
pub(crate) mod cursor;
mod os;

use std::io;
use std::io::IoSlice;

use async_trait::async_trait;

#[async_trait]
pub trait IFile: Send {
    async fn write_vec<'a>(&self, pos: u64, bufs: &'a mut [IoSlice<'a>]) -> io::Result<usize>;
    async fn write_at(&self, pos: u64, data: &[u8]) -> io::Result<usize>;
    async fn read_at(&self, pos: u64, data: &mut [u8]) -> io::Result<usize>;
    async fn sync_data(&self) -> io::Result<()>;
    async fn truncate(&self, size: u64) -> io::Result<()>;
    fn len(&self) -> u64;
    fn is_empty(&self) -> bool;
}
