// #![deny(dead_code)]
// #![deny(non_snake_case)]
// #![deny(unused_imports)]
// #![deny(unused_must_use)]

mod cache;
mod file;

pub use cache::PageId;
pub use file::cursor::FileCursor;
pub use file::system::{FileSystem, Options};
pub use file::{File, FileSync};
pub use std::fs::OpenOptions;
