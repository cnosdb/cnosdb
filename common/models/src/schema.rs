//! CatalogProvider:            ---> namespace
//! - SchemeProvider #1         ---> db
//!     - dyn tableProvider #1  ---> table
//!         - field #1
//!         - Column #2
//!     - dyn TableProvider #2
//!         - Column #3
//!         - Column #4

use std::{collections::BTreeMap, sync::Arc};

use datafusion::arrow::datatypes::{DataType as ArrowDataType, Field, Schema, SchemaRef, TimeUnit};
use serde::{Deserialize, Serialize};
use crate::ValueType;

pub type TableSchemaRef = Arc<TableSchema>;

pub const TIME_FIELD_NAME: &str = "time";

pub const FIELD_ID: &str = "_field_id";
pub const TAG: &str = "_tag";
pub const TIME_FIELD: &str = "time";

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TableSchema {
    pub db: String,
    pub name: String,
    pub schema_id: u32,
    pub location:
    pub fields: BTreeMap<String, TableFiled>,
}

impl TableSchema {
    pub fn to_arrow_schema(&self) -> SchemaRef {
        let fields: Vec<Field> = self
            .fields
            .iter()
            .map(|(name, schema)| {
                let mut f = Field::new(name, schema.column_type.into(), true);
                let mut map = BTreeMap::new();
                map.insert(FIELD_ID.to_string(), schema.id.to_string());
                map.insert(TAG.to_string(), schema.column_type.is_tag().to_string());
                f.set_metadata(Some(map));
                f
            })
            .collect();

        Arc::new(Schema::new(fields))
    }

    pub fn new(db: String, name: String, fields: BTreeMap<String, TableFiled>) -> Self {
        Self { db, name, schema_id: 0, fields }
    }
    pub fn fields(&self) -> &BTreeMap<String, TableFiled>{
        &self.fields
    }
}

pub fn is_time_column(field: &Field) -> bool {
    TIME_FIELD_NAME == field.name()
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Hash)]
pub struct TableFiled {
    pub id: u64,
    pub name: String,
    pub column_type: ColumnType,
}

impl TableFiled {
    pub fn new(id: u64, name: String, column_type: ColumnType) -> Self {
        Self {
            id,
            name,
            column_type,
        }
    }

    pub fn time_field() -> TableFiled {
        TableFiled {
            id: 0,
            name: TIME_FIELD_NAME.to_string(),
            column_type: ColumnType::Time,
        }
    }
}

// impl From<&FieldInfo> for TableFiled {
//     fn from(info: &FieldInfo) -> Self {
//         let mut column = ColumnType::Field(info.value_type());
//         if info.is_tag() {
//             column = ColumnType::Tag;
//         }
//
//         TableFiled::new(
//             info.field_id(),
//             String::from_utf8(info.name().to_vec()).unwrap(),
//             column,
//         )
//     }
// }

impl From<ColumnType> for ArrowDataType {
    fn from(t: ColumnType) -> Self {
        match t {
            ColumnType::Tag => Self::Utf8,
            ColumnType::Time => Self::Timestamp(TimeUnit::Nanosecond, None),
            ColumnType::Field(ValueType::Float) => Self::Float64,
            ColumnType::Field(ValueType::Integer) => Self::Int64,
            ColumnType::Field(ValueType::Unsigned) => Self::UInt64,
            ColumnType::Field(ValueType::String) => Self::Utf8,
            ColumnType::Field(ValueType::Boolean) => Self::Boolean,
            _ => Self::Null,
        }
    }
}

impl TryFrom<ArrowDataType> for ColumnType {
    type Error = &'static str;

    fn try_from(value: ArrowDataType) -> Result<Self, Self::Error> {
        match value {
            ArrowDataType::Float64 => Ok(Self::Field(ValueType::Float)),
            ArrowDataType::Int64 => Ok(Self::Field(ValueType::Integer)),
            ArrowDataType::UInt64 => Ok(Self::Field(ValueType::Unsigned)),
            ArrowDataType::Utf8 => Ok(Self::Field(ValueType::String)),
            ArrowDataType::Boolean => Ok(Self::Field(ValueType::Boolean)),
            _ => Err("Error field type not supported"),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub enum ColumnType {
    Tag,
    Time,
    Field(ValueType),
}

impl ColumnType {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Tag => "tag",
            Self::Time => "time",
            Self::Field(ValueType::Integer) => "i64",
            Self::Field(ValueType::Unsigned) => "u64",
            Self::Field(ValueType::Float) => "f64",
            Self::Field(ValueType::Boolean) => "bool",
            Self::Field(ValueType::String) => "string",
            _ => "Error filed type not supported",
        }
    }
    pub fn field_type(&self)-> u8{
        match self {
            Self::Field(ValueType::Integer) => ,
            Self::Field(ValueType::Unsigned) => ,
            Self::Field(ValueType::Float) => ,
            Self::Field(ValueType::Boolean) => ,
            Self::Field(ValueType::String) => ,
            _ => {
                0
            }
        }
    }
}

impl std::fmt::Display for ColumnType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = self.as_str();
        write!(f, "{}", s)
    }
}

impl ColumnType {
    pub fn is_tag(&self) -> bool {
        matches!(self, ColumnType::Tag)
    }
}
