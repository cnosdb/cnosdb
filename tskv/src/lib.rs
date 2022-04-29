#![allow(dead_code)]
#![allow(dead_code)]
#![allow(unreachable_patterns)]
#![allow(unused_imports, unused_variables)]

mod compute;
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
mod record_file;
mod runtime;
mod summary;
mod tseries_family;
mod tsm;
mod version_set;
mod wal;

pub use direct_io::*;
pub use error::*;
pub use file_manager::*;
pub use file_utils::*;
pub use kv_option::Options;
pub use kvcore::*;
pub use lru_cache::*;
pub use memcache::*;
use protos::kv_service::WritePointsRpcResponse;
pub use record_file::*;
pub use runtime::*;
pub use summary::*;
use tokio::sync::oneshot;
pub use tseries_family::*;
pub use tsm::*;
pub use version_set::*;

#[derive(Debug)]
pub enum Task {
    AddSeries {
        req: protos::kv_service::AddSeriesRpcRequest,
        tx: oneshot::Sender<wal::WalResult<()>>,
    },
    GetSeriesInfo {
        req: protos::kv_service::GetSeriesInfoRpcRequest,
        tx: oneshot::Sender<wal::WalResult<()>>,
    },
    WritePoints {
        req: protos::kv_service::WritePointsRpcRequest,
        tx: oneshot::Sender<std::result::Result<WritePointsRpcResponse, Error>>,
    },
}
