// #![deny(dead_code)]
// #![deny(non_snake_case)]
// #![deny(unused_imports)]
// #![deny(unused_must_use)]

mod async_rt;
mod cache;
mod file;

pub use std::fs::OpenOptions;

pub use async_rt::*;
pub use cache::PageId;
pub use file::{
    cursor::FileCursor,
    system::{FileSystem, Options},
    File, FileSync,
};
