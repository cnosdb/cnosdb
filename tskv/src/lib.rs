#![allow(dead_code)]
#![allow(dead_code)]
#![allow(unreachable_patterns)]
#![allow(unused_imports, unused_variables)]

mod byte_utils;
mod compaction;
mod context;
mod direct_io;
mod error;
mod file_manager;
mod file_utils;
mod forward_index;
pub mod kv_option;
mod kvcore;
mod lru_cache;
mod memcache;
mod reader;
mod record_file;
mod runtime;
pub mod schema;
mod summary;
mod tseries_family;
mod tsm;
mod version_set;
mod wal;

pub use error::{Error, Result};
pub use kv_option::Options;
pub use kvcore::TsKv;
use protos::kv_service::WritePointsRpcResponse;
use tokio::sync::oneshot;
use utils::BloomFilter;

type ColumnFileId = u64;
type TseriesFamilyId = u32;
type LevelId = u32;
type VersionId = u32;

#[derive(Debug)]
pub enum Task {
    AddSeries {
        req: protos::kv_service::AddSeriesRpcRequest,
        tx: oneshot::Sender<Result<()>>,
    },
    GetSeriesInfo {
        req: protos::kv_service::GetSeriesInfoRpcRequest,
        tx: oneshot::Sender<Result<()>>,
    },
    WritePoints {
        req: protos::kv_service::WritePointsRpcRequest,
        tx: oneshot::Sender<std::result::Result<WritePointsRpcResponse, Error>>,
    },
}
