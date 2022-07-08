mod errors;
mod field_info;
mod points;
mod series_info;
mod tag;

pub use errors::{Error, Result};
pub use field_info::{generate_field_id, FieldInfo, ValueType};
pub use points::*;
pub use series_info::{generate_series_id, SeriesInfo};
pub use tag::Tag;

pub type ShardID = u64;
pub type CatalogID = u64;
pub type SchemaID = u64;
pub type SeriesID = u64;
pub type TableID = u64;

pub type TagKey = Vec<u8>;
pub type TagValue = Vec<u8>;

pub type FieldID = u64;
pub type FieldName = Vec<u8>;

pub type Timestamp = i64;
