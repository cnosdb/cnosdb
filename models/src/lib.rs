mod consistency_level;
mod errors;
mod field_info;
mod node_info;
mod points;
mod series_info;
mod tag;

pub use errors::{Error, Result};
pub use field_info::{generate_field_id, FieldInfo, ValueType};
pub use points::*;
pub use series_info::{generate_series_id, SeriesInfo, SeriesKey};
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
