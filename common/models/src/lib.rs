pub mod codec;
mod consistency_level;
mod errors;
mod field_info;
mod node_info;
mod points;
pub mod schema;
mod series_info;
mod tag;
pub mod utils;
#[macro_use]
pub mod error_code;

use parking_lot::RwLock;
use std::sync::Arc;

pub use errors::{Error, Result};
pub use field_info::{FieldInfo, ValueType};
pub use points::*;
pub use series_info::SeriesKey;
pub use tag::Tag;

pub type ShardId = u64;
pub type CatalogId = u64;
pub type SchemaId = u64;
pub type SeriesId = u64;
pub type TableId = u64;

pub type TagKey = Vec<u8>;
pub type TagValue = Vec<u8>;

pub type FieldId = u64;
pub type FieldName = Vec<u8>;

pub type Timestamp = i64;

pub type RwLockRef<T> = Arc<RwLock<T>>;
