// #![deny(dead_code)]
// #![deny(non_snake_case)]
// #![deny(unused_imports)]
// #![deny(unused_must_use)]

use async_trait::async_trait;
use tokio::fs::File;

pub(crate) mod file;
pub mod file_manager;
pub mod queue;

#[async_trait]
pub trait DataBlock {
    async fn write(&self, file: &mut File) -> crate::Result<usize>;
    async fn read(&mut self, file: &mut File) -> crate::Result<usize>;
}
