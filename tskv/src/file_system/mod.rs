// #![deny(dead_code)]
// #![deny(non_snake_case)]
// #![deny(unused_imports)]
// #![deny(unused_must_use)]

mod async_file;
mod cache;
mod file;
pub mod file_manager;

pub use cache::PageId;
pub use file::{
    cursor::FileCursor,
    system::{FileSystemCache, Options},
    DmaFile, FileSync,
};
