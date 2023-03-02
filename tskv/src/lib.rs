#![allow(dead_code)]
#![allow(dead_code)]
#![allow(unreachable_patterns)]
#![allow(unused_imports, unused_variables)]

pub use error::{Error, Result};
pub use kv_option::Options;
pub use kvcore::TsKv;
pub use summary::{print_summary_statistics, Summary, VersionEdit};
pub use tseries_family::TimeRange;
pub use tsm::print_tsm_statistics;
pub use wal::print_wal_statistics;

pub mod byte_utils;
mod compaction;
mod compute;
mod context;
pub mod database;
pub mod engine;
pub mod error;
pub mod file_system;
pub mod file_utils;
pub mod index;
pub mod iterator;
pub mod kv_option;
mod kvcore;
mod memcache;
mod record_file;
mod schema;
mod summary;
mod tseries_family;
mod tsm;
mod version_set;
mod wal;

pub type ColumnFileId = u64;
type TseriesFamilyId = u32;
type LevelId = u32;
