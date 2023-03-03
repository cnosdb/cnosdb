pub mod codec;
pub mod consistency_level;
pub mod errors;
mod field_info;
pub mod meta_data;
mod node_info;
mod points;
pub mod schema;
mod series_info;
pub mod tag;
pub mod utils;
#[macro_use]
// pub mod error_code;
pub mod arrow_array;
pub mod auth;
pub mod duration;
pub mod object_reference;
pub mod oid;
pub mod predicate;
use std::sync::Arc;

pub use error_code;
pub use errors::{Error, Result};
pub use field_info::{FieldInfo, ValueType};
use parking_lot::RwLock;
pub use points::*;
pub use series_info::SeriesKey;
pub use tag::Tag;

pub type ShardId = u64;
pub type CatalogId = u64;
pub type SchemaId = u64;
pub type SeriesId = u32;
pub type TableId = u64;
pub type ColumnId = u32;

pub type TagKey = Vec<u8>;
pub type TagValue = Vec<u8>;

pub type FieldId = u64;
pub type FieldName = Vec<u8>;

pub type Timestamp = i64;

pub type RwLockRef<T> = Arc<RwLock<T>>;
