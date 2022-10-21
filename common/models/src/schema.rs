//! CatalogProvider:            ---> namespace
//! - SchemeProvider #1         ---> db
//!     - dyn tableProvider #1  ---> table
//!         - field #1
//!         - Column #2
//!     - dyn TableProvider #2
//!         - Column #3
//!         - Column #4

use std::any::Any;
use std::collections::HashMap;
use std::fmt;
use std::{collections::BTreeMap, sync::Arc};

use std::mem::size_of_val;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use datafusion::arrow::datatypes::{
    DataType as ArrowDataType, Field as ArrowField, Schema, SchemaRef, TimeUnit,
};
use datafusion::datasource::{TableProvider, TableType};
use datafusion::execution::context::SessionState;
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::ExecutionPlan;

use crate::{ColumnId, ValueType};

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

    columns: Vec<TableColumn>,
    columns_index: HashMap<String, usize>,
}

impl Default for TableSchema {
    fn default() -> Self {
        Self {
            db: "public".to_string(),
            name: "".to_string(),
            schema_id: 0,
            columns: Default::default(),
            columns_index: Default::default(),
        }
    }
}

impl TableSchema {
    pub fn to_arrow_schema(&self) -> SchemaRef {
        let fields: Vec<ArrowField> = self.columns.iter().map(|field| field.into()).collect();

        Arc::new(Schema::new(fields))
    }

    pub fn new(db: String, name: String, columns: Vec<TableColumn>) -> Self {
        let columns_index = columns
            .iter()
            .enumerate()
            .map(|(idx, e)| (e.name.clone(), idx))
            .collect();

        Self {
            db,
            name,
            schema_id: 0,
            columns,
            columns_index,
        }
    }

    /// add column
    /// not add if exists
    pub fn add_column(&mut self, col: TableColumn) {
        self.columns_index
            .entry(col.name.clone())
            .or_insert_with(|| {
                self.columns.push(col);
                self.columns.len() - 1
            });
    }

    /// Get the metadata of the column according to the column name
    pub fn column(&self, name: &str) -> Option<&TableColumn> {
        self.columns_index
            .get(name)
            .map(|idx| unsafe { self.columns.get_unchecked(*idx) })
    }

    pub fn columns(&self) -> &Vec<TableColumn> {
        &self.columns
    }

    /// Number of columns of ColumnType is Field
    pub fn field_num(&self) -> usize {
        let mut ans = 0;
        for i in self.columns.iter() {
            if i.column_type != ColumnType::Tag && i.column_type != ColumnType::Time {
                ans += 1;
            }
        }
        ans
    }

    // return (table_field_id, index), index mean field location which column
    pub fn fields_id(&self) -> HashMap<ColumnId, usize> {
        let mut ans = vec![];
        for i in self.columns.iter() {
            if i.column_type != ColumnType::Tag && i.column_type != ColumnType::Time {
                ans.push(i.id);
            }
        }
        ans.sort();
        let mut map = HashMap::new();
        for (i, id) in ans.iter().enumerate() {
            map.insert(*id, i);
        }
        map
    }

    pub fn size(&self) -> usize {
        let mut size = 0;
        for i in self.columns.iter() {
            size += size_of_val(&i);
        }
        size += size_of_val(&self);
        size
    }
}

#[async_trait]
impl TableProvider for TableSchema {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        todo!()
    }

    fn table_type(&self) -> TableType {
        todo!()
    }

    async fn scan(
        &self,
        _ctx: &SessionState,
        _projection: &Option<Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        todo!()
    }
}

pub fn is_time_column(field: &ArrowField) -> bool {
    TIME_FIELD_NAME == field.name()
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Hash)]
pub struct TableColumn {
    pub id: ColumnId,
    pub name: String,
    pub column_type: ColumnType,
    pub codec: u8,
}

impl From<&TableColumn> for ArrowField {
    fn from(column: &TableColumn) -> Self {
        let mut f = ArrowField::new(&column.name, column.column_type.into(), column.nullable());
        let mut map = BTreeMap::new();
        map.insert(FIELD_ID.to_string(), column.id.to_string());
        map.insert(TAG.to_string(), column.column_type.is_tag().to_string());
        f.set_metadata(Some(map));
        f
    }
}

impl From<TableColumn> for ArrowField {
    fn from(field: TableColumn) -> Self {
        (&field).into()
    }
}

impl TableColumn {
    pub fn new(id: ColumnId, name: String, column_type: ColumnType, codec: u8) -> Self {
        Self {
            id,
            name,
            column_type,
            codec,
        }
    }
    pub fn new_with_default(name: String, column_type: ColumnType) -> Self {
        Self {
            id: 0,
            name,
            column_type,
            codec: 0,
        }
    }

    pub fn new_time_column(id: ColumnId) -> TableColumn {
        TableColumn {
            id,
            name: TIME_FIELD_NAME.to_string(),
            column_type: ColumnType::Time,
            codec: 0,
        }
    }

    pub fn new_tag_column(id: ColumnId, name: String) -> TableColumn {
        TableColumn {
            id,
            name,
            column_type: ColumnType::Tag,
            codec: 0,
        }
    }

    pub fn nullable(&self) -> bool {
        // The time column cannot be empty
        !matches!(self.column_type, ColumnType::Time)
    }
}

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
    pub fn field_type(&self) -> u8 {
        match self {
            Self::Field(ValueType::Float) => 0,
            Self::Field(ValueType::Integer) => 1,
            Self::Field(ValueType::Unsigned) => 2,
            Self::Field(ValueType::Boolean) => 3,
            Self::Field(ValueType::String) => 4,
            _ => 0,
        }
    }

    pub fn from_i32(field_type: i32) -> Self {
        match field_type {
            0 => Self::Field(ValueType::Float),
            1 => Self::Field(ValueType::Integer),
            2 => Self::Field(ValueType::Unsigned),
            3 => Self::Field(ValueType::Boolean),
            4 => Self::Field(ValueType::String),
            5 => Self::Time,
            _ => Self::Field(ValueType::Unknown),
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

    pub fn is_time(&self) -> bool {
        matches!(self, ColumnType::Time)
    }

    pub fn is_field(&self) -> bool {
        matches!(self, ColumnType::Field(_))
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Hash)]
pub struct DatabaseSchema {
    pub name: String,
    pub config: DatabaseOptions,
}

impl DatabaseSchema {
    pub fn new(name: &str) -> Self {
        DatabaseSchema {
            name: name.to_string(),
            config: DatabaseOptions::default(),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Hash)]
pub struct DatabaseOptions {
    // data keep time
    pub ttl: Duration,

    pub shard_num: u64,
    // shard coverage time range
    pub vnode_duration: Duration,

    pub replica: u64,
    // timestamp percision
    pub precision: Precision,
}

impl Default for DatabaseOptions {
    fn default() -> Self {
        Self {
            ttl: Duration {
                time_num: 365,
                unit: DurationUnit::Day,
            },
            shard_num: 1,
            vnode_duration: Duration {
                time_num: 365,
                unit: DurationUnit::Day,
            },
            replica: 1,
            precision: Precision::NS,
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Hash)]
pub enum Precision {
    MS,
    US,
    NS,
}

impl Precision {
    pub fn new(text: &str) -> Option<Self> {
        match text.to_uppercase().as_str() {
            "MS" => Some(Precision::MS),
            "US" => Some(Precision::US),
            "NS" => Some(Precision::NS),
            _ => None,
        }
    }
}

impl fmt::Display for Precision {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Precision::MS => f.write_str("MS"),
            Precision::US => f.write_str("US"),
            Precision::NS => f.write_str("NS"),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Hash)]
pub enum DurationUnit {
    Minutes,
    Hour,
    Day,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Hash)]
pub struct Duration {
    pub time_num: u64,
    pub unit: DurationUnit,
}

impl fmt::Display for Duration {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match &self.unit {
            DurationUnit::Minutes => write!(f, "{} Minutes", self.time_num),
            DurationUnit::Hour => write!(f, "{} Hours", self.time_num),
            DurationUnit::Day => write!(f, "{} Days", self.time_num),
        }
    }
}

impl Duration {
    // with default DurationUnit day
    pub fn new(text: &str) -> Option<Self> {
        if text.is_empty() {
            return None;
        }
        let len = text.len();
        if let Ok(v) = text.parse::<u64>() {
            return Some(Duration {
                time_num: v,
                unit: DurationUnit::Day,
            });
        };

        let time = &text[..len - 1];
        let unit = &text[len - 1..];
        let time_num = match time.parse::<u64>() {
            Ok(v) => v,
            Err(_) => {
                return None;
            }
        };
        let time_unit = match unit.to_uppercase().as_str() {
            "D" => DurationUnit::Day,
            "H" => DurationUnit::Hour,
            "M" => DurationUnit::Minutes,
            _ => return None,
        };
        Some(Duration {
            time_num,
            unit: time_unit,
        })
    }
}
