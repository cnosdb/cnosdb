//! CatalogProvider:            ---> namespace
//! - SchemeProvider #1         ---> db
//!     - dyn tableProvider #1  ---> table
//!         - field #1
//!         - Column #2
//!     - dyn TableProvider #2
//!         - Column #3
//!         - Column #4

use std::borrow::Cow;
use std::collections::HashMap;
use std::fmt::{self, Display};
use std::mem::size_of_val;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration as StdDuration;

use arrow_schema::{DataType, Field as DFField};
use config::{RequestLimiterConfig, TenantLimiterConfig, TenantObjectLimiterConfig};
use datafusion::arrow::datatypes::{
    DataType as ArrowDataType, Field as ArrowField, Schema, SchemaRef, TimeUnit,
};
use datafusion::datasource::file_format::avro::AvroFormat;
use datafusion::datasource::file_format::csv::CsvFormat;
use datafusion::datasource::file_format::file_type::{FileCompressionType, FileType};
use datafusion::datasource::file_format::json::JsonFormat;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::file_format::FileFormat;
use datafusion::datasource::listing::ListingOptions;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::prelude::Column;
use datafusion::scalar::ScalarValue;
use derive_builder::Builder;
use serde::{Deserialize, Serialize};

use crate::codec::Encoding;
use crate::gis::data_type::Geometry;
use crate::oid::{Identifier, Oid};
use crate::utils::{
    DAY_MICROS, DAY_MILLS, DAY_NANOS, HOUR_MICROS, HOUR_MILLS, HOUR_NANOS, MINUTES_MICROS,
    MINUTES_MILLS, MINUTES_NANOS,
};
use crate::value_type::ValueType;
use crate::{ColumnId, Error, PhysicalDType, SchemaId, Timestamp};

pub type TskvTableSchemaRef = Arc<TskvTableSchema>;

pub const TIME_FIELD_NAME: &str = "time";

pub const FIELD_ID: &str = "_field_id";
pub const TAG: &str = "_tag";
pub const TIME_FIELD: &str = "time";

pub const DEFAULT_DATABASE: &str = "public";
pub const USAGE_SCHEMA: &str = "usage_schema";
pub const DEFAULT_CATALOG: &str = "cnosdb";
pub const DEFAULT_PRECISION: &str = "NS";

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum TableSchema {
    TsKvTableSchema(TskvTableSchemaRef),
    ExternalTableSchema(Arc<ExternalTableSchema>),
    StreamTableSchema(Arc<StreamTable>),
}

impl TableSchema {
    pub fn name(&self) -> &str {
        match self {
            TableSchema::TsKvTableSchema(schema) => schema.name.as_str(),
            TableSchema::ExternalTableSchema(schema) => schema.name.as_str(),
            TableSchema::StreamTableSchema(schema) => schema.name(),
        }
    }

    pub fn db(&self) -> &str {
        match self {
            TableSchema::TsKvTableSchema(schema) => schema.db.as_str(),
            TableSchema::ExternalTableSchema(schema) => schema.db.as_str(),
            TableSchema::StreamTableSchema(schema) => schema.db(),
        }
    }

    pub fn engine_name(&self) -> &str {
        match self {
            TableSchema::TsKvTableSchema(_) => "TSKV",
            TableSchema::ExternalTableSchema(_) => "EXTERNAL",
            TableSchema::StreamTableSchema(_) => "STREAM",
        }
    }

    pub fn to_arrow_schema(&self) -> SchemaRef {
        match self {
            Self::ExternalTableSchema(e) => Arc::new(e.schema.clone()),
            Self::TsKvTableSchema(e) => e.to_arrow_schema(),
            Self::StreamTableSchema(e) => e.schema(),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct ExternalTableSchema {
    pub tenant: String,
    pub db: String,
    pub name: String,
    pub file_compression_type: String,
    pub file_type: String,
    pub location: String,
    pub target_partitions: usize,
    pub table_partition_cols: Vec<(String, DataType)>,
    pub has_header: bool,
    pub delimiter: u8,
    pub schema: Schema,
}

impl ExternalTableSchema {
    pub fn table_options(&self) -> DataFusionResult<ListingOptions> {
        let file_compression_type = FileCompressionType::from_str(&self.file_compression_type)?;
        let file_type = FileType::from_str(&self.file_type)?;
        let file_extension =
            file_type.get_ext_with_compression(self.file_compression_type.to_owned().parse()?)?;
        let file_format: Arc<dyn FileFormat> = match file_type {
            FileType::CSV => Arc::new(
                CsvFormat::default()
                    .with_has_header(self.has_header)
                    .with_delimiter(self.delimiter)
                    .with_file_compression_type(file_compression_type),
            ),
            FileType::PARQUET => Arc::new(ParquetFormat::default()),
            FileType::AVRO => Arc::new(AvroFormat),
            FileType::JSON => {
                Arc::new(JsonFormat::default().with_file_compression_type(file_compression_type))
            }
            FileType::ARROW => {
                return Err(DataFusionError::NotImplemented(
                    "Arrow external table.".to_string(),
                ))
            }
        };

        let options = ListingOptions::new(file_format)
            .with_file_extension(file_extension)
            .with_target_partitions(self.target_partitions);

        Ok(options)
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct TskvTableSchema {
    pub tenant: String,
    pub db: String,
    pub name: String,
    pub schema_id: SchemaId,
    next_column_id: ColumnId,

    columns: Vec<TableColumn>,
    //ColumnName -> ColumnsIndex
    columns_index: HashMap<String, usize>,
}

impl Default for TskvTableSchema {
    fn default() -> Self {
        Self {
            tenant: "cnosdb".to_string(),
            db: "public".to_string(),
            name: "template".to_string(),
            schema_id: 0,
            next_column_id: 0,
            columns: Default::default(),
            columns_index: Default::default(),
        }
    }
}

impl TskvTableSchema {
    pub fn to_arrow_schema(&self) -> SchemaRef {
        let fields: Vec<ArrowField> = self.columns.iter().map(|field| field.into()).collect();
        Arc::new(Schema::new(fields))
    }

    pub fn new(tenant: String, db: String, name: String, columns: Vec<TableColumn>) -> Self {
        let columns_index = columns
            .iter()
            .enumerate()
            .map(|(idx, e)| (e.name.clone(), idx))
            .collect();

        Self {
            tenant,
            db,
            name,
            schema_id: 0,
            next_column_id: columns.len() as ColumnId,
            columns,
            columns_index,
        }
    }

    /// only for mock!!!
    pub fn new_test() -> Self {
        TskvTableSchema::new(
            "cnosdb".into(),
            "public".into(),
            "test".into(),
            vec![TableColumn::new_time_column(0, TimeUnit::Second)],
        )
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
        self.next_column_id += 1;
    }

    /// drop column if exists
    pub fn drop_column(&mut self, col_name: &str) {
        if let Some(id) = self.columns_index.get(col_name) {
            self.columns.remove(*id);
        }
        let columns_index = self
            .columns
            .iter()
            .enumerate()
            .map(|(idx, e)| (e.name.clone(), idx))
            .collect();
        self.columns_index = columns_index;
    }

    pub fn change_column(&mut self, col_name: &str, new_column: TableColumn) {
        let id = match self.columns_index.get(col_name) {
            None => return,
            Some(id) => *id,
        };
        self.columns_index.remove(col_name);
        self.columns_index.insert(new_column.name.clone(), id);
        self.columns[id] = new_column;
    }

    /// Get the metadata of the column according to the column name
    pub fn column(&self, name: &str) -> Option<&TableColumn> {
        self.columns_index
            .get(name)
            .map(|idx| unsafe { self.columns.get_unchecked(*idx) })
    }

    pub fn time_column_precision(&self) -> Precision {
        self.columns
            .iter()
            .find(|column| column.column_type.is_time())
            .map(|column| column.column_type.precision().unwrap_or(Precision::NS))
            .unwrap_or(Precision::NS)
    }

    /// Get the index of the column
    pub fn column_index(&self, name: &str) -> Option<usize> {
        self.columns_index.get(name).cloned()
    }

    pub fn column_name(&self, id: ColumnId) -> Option<&str> {
        for column in self.columns.iter() {
            if column.id == id {
                return Some(&column.name);
            }
        }
        None
    }

    /// Get the metadata of the column according to the column index
    pub fn column_by_index(&self, idx: usize) -> Option<&TableColumn> {
        self.columns.get(idx)
    }

    pub fn columns(&self) -> &[TableColumn] {
        &self.columns
    }

    pub fn fields(&self) -> Vec<TableColumn> {
        self.columns
            .iter()
            .filter(|column| column.column_type.is_field())
            .cloned()
            .collect()
    }

    /// Traverse and return the time column of the table
    ///
    /// Do not call frequently
    pub fn time_column(&self) -> TableColumn {
        // There is one and only one time column
        unsafe {
            self.columns
                .iter()
                .filter(|column| column.column_type.is_time())
                .last()
                .cloned()
                .unwrap_unchecked()
        }
    }

    /// Number of columns of ColumnType is Field
    pub fn field_num(&self) -> usize {
        self.columns
            .iter()
            .filter(|column| column.column_type.is_field())
            .count()
    }

    pub fn tag_num(&self) -> usize {
        self.columns
            .iter()
            .filter(|column| column.column_type.is_tag())
            .count()
    }

    pub fn tag_indices(&self) -> Vec<usize> {
        self.columns
            .iter()
            .enumerate()
            .filter(|(_, column)| column.column_type.is_tag())
            .map(|(idx, _)| idx)
            .collect()
    }

    // return (table_field_id, index), index mean field location which column
    pub fn fields_id(&self) -> HashMap<ColumnId, usize> {
        let mut ans = vec![];
        for i in self.columns.iter() {
            if matches!(i.column_type, ColumnType::Field(_)) {
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

    pub fn next_column_id(&self) -> ColumnId {
        self.next_column_id
    }

    pub fn size(&self) -> usize {
        let mut size = 0;
        for i in self.columns.iter() {
            size += size_of_val(i);
        }
        size += size_of_val(self);
        size
    }

    pub fn contains_column(&self, column_name: &str) -> bool {
        self.columns_index.contains_key(column_name)
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
    pub encoding: Encoding,
}

pub const GIS_SRID_META_KEY: &str = "gis.srid";
pub const GIS_SUB_TYPE_META_KEY: &str = "gis.sub_type";

impl From<&TableColumn> for ArrowField {
    fn from(column: &TableColumn) -> Self {
        let mut map = HashMap::new();
        map.insert(FIELD_ID.to_string(), column.id.to_string());
        map.insert(TAG.to_string(), column.column_type.is_tag().to_string());

        // 通过 SRID_META_KEY 标记 Geometry 类型的列
        if let ColumnType::Field(ValueType::Geometry(Geometry { srid, sub_type })) =
            column.column_type
        {
            map.insert(GIS_SUB_TYPE_META_KEY.to_string(), sub_type.to_string());
            map.insert(GIS_SRID_META_KEY.to_string(), srid.to_string());
        }

        let nullable = column.nullable();
        let mut f = ArrowField::new(&column.name, column.column_type.clone().into(), nullable);
        f.set_metadata(map);
        f
    }
}

impl From<TableColumn> for ArrowField {
    fn from(column: TableColumn) -> Self {
        Self::from(&column)
    }
}

impl From<TableColumn> for Column {
    fn from(field: TableColumn) -> Self {
        Column::from_name(field.name)
    }
}

impl TableColumn {
    pub fn new(id: ColumnId, name: String, column_type: ColumnType, encoding: Encoding) -> Self {
        Self {
            id,
            name,
            column_type,
            encoding,
        }
    }
    pub fn new_with_default(name: String, column_type: ColumnType) -> Self {
        Self {
            id: 0,
            name,
            column_type,
            encoding: Encoding::Default,
        }
    }

    pub fn new_time_column(id: ColumnId, time_unit: TimeUnit) -> TableColumn {
        TableColumn {
            id,
            name: TIME_FIELD_NAME.to_string(),
            column_type: ColumnType::Time(time_unit),
            encoding: Encoding::Default,
        }
    }

    pub fn new_tag_column(id: ColumnId, name: String) -> TableColumn {
        TableColumn {
            id,
            name,
            column_type: ColumnType::Tag,
            encoding: Encoding::Default,
        }
    }

    pub fn nullable(&self) -> bool {
        // The time column cannot be empty
        !matches!(self.column_type, ColumnType::Time(_))
    }

    pub fn encode(&self) -> crate::errors::Result<Vec<u8>> {
        let buf = bincode::serialize(&self)
            .map_err(|e| Error::InvalidSerdeMessage { err: e.to_string() })?;

        Ok(buf)
    }

    pub fn decode(buf: &[u8]) -> crate::errors::Result<Self> {
        let column = bincode::deserialize::<TableColumn>(buf)
            .map_err(|e| Error::InvalidSerdeMessage { err: e.to_string() })?;

        Ok(column)
    }

    pub fn encoding_valid(&self) -> bool {
        if let ColumnType::Field(ValueType::Float) = self.column_type {
            return self.encoding.is_double_encoding();
        } else if let ColumnType::Field(ValueType::Boolean) = self.column_type {
            return self.encoding.is_bool_encoding();
        } else if let ColumnType::Field(ValueType::Integer) = self.column_type {
            return self.encoding.is_bigint_encoding();
        } else if let ColumnType::Field(ValueType::Unsigned) = self.column_type {
            return self.encoding.is_unsigned_encoding();
        } else if let ColumnType::Field(ValueType::String) = self.column_type {
            return self.encoding.is_string_encoding();
        } else if let ColumnType::Time(_) = self.column_type {
            return self.encoding.is_timestamp_encoding();
        } else if let ColumnType::Tag = self.column_type {
            return self.encoding.is_string_encoding();
        }

        true
    }
}

impl From<ColumnType> for ArrowDataType {
    fn from(t: ColumnType) -> Self {
        match t {
            ColumnType::Tag => ArrowDataType::Utf8,
            ColumnType::Time(unit) => ArrowDataType::Timestamp(unit, None),
            ColumnType::Field(ValueType::Float) => ArrowDataType::Float64,
            ColumnType::Field(ValueType::Integer) => ArrowDataType::Int64,
            ColumnType::Field(ValueType::Unsigned) => ArrowDataType::UInt64,
            ColumnType::Field(ValueType::String) => ArrowDataType::Utf8,
            ColumnType::Field(ValueType::Boolean) => ArrowDataType::Boolean,
            ColumnType::Field(ValueType::Geometry(_)) => ArrowDataType::Utf8,
            _ => ArrowDataType::Null,
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Hash)]
pub enum ColumnType {
    Tag,
    Time(TimeUnit),
    Field(ValueType),
}

impl ColumnType {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Tag => "TAG",
            Self::Time(unit) => match unit {
                TimeUnit::Second => "TimestampSecond",
                TimeUnit::Millisecond => "TimestampMillisecond",
                TimeUnit::Microsecond => "TimestampMicrosecond",
                TimeUnit::Nanosecond => "TimestampNanosecond",
            },
            Self::Field(ValueType::Integer) => "I64",
            Self::Field(ValueType::Unsigned) => "U64",
            Self::Field(ValueType::Float) => "F64",
            Self::Field(ValueType::Boolean) => "BOOL",
            Self::Field(ValueType::String) => "STRING",
            _ => "Error filed type not supported",
        }
    }

    pub fn as_column_type_str(&self) -> &'static str {
        match self {
            Self::Tag => "TAG",
            Self::Field(_) => "FIELD",
            Self::Time(_) => "TIME",
        }
    }

    pub fn field_type(&self) -> u8 {
        match self {
            Self::Field(ValueType::Float) => 0,
            Self::Field(ValueType::Integer) => 1,
            Self::Field(ValueType::Unsigned) => 2,
            Self::Field(ValueType::Boolean) => 3,
            Self::Field(ValueType::String) | Self::Field(ValueType::Geometry(_)) => 4,
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
            _ => Self::Field(ValueType::Unknown),
        }
    }

    pub fn to_sql_type_str(&self) -> Cow<'static, str> {
        match self {
            Self::Tag => "STRING".into(),
            Self::Time(unit) => match unit {
                TimeUnit::Second => "TIMESTAMP(SECOND)".into(),
                TimeUnit::Millisecond => "TIMESTAMP(MILLISECOND)".into(),
                TimeUnit::Microsecond => "TIMESTAMP(MICROSECOND)".into(),
                TimeUnit::Nanosecond => "TIMESTAMP(NANOSECOND)".into(),
            },
            Self::Field(value_type) => match value_type {
                ValueType::String => "STRING".into(),
                ValueType::Integer => "BIGINT".into(),
                ValueType::Unsigned => "BIGINT UNSIGNED".into(),
                ValueType::Float => "DOUBLE".into(),
                ValueType::Boolean => "BOOLEAN".into(),
                ValueType::Unknown => "UNKNOWN".into(),
                ValueType::Geometry(geo) => geo.to_string().into(),
            },
        }
    }
}

impl Display for ColumnType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> std::fmt::Result {
        let s = self.as_str();
        write!(f, "{}", s)
    }
}

impl ColumnType {
    pub fn is_tag(&self) -> bool {
        matches!(self, ColumnType::Tag)
    }

    pub fn is_time(&self) -> bool {
        matches!(self, ColumnType::Time(_))
    }

    pub fn precision(&self) -> Option<Precision> {
        match self {
            ColumnType::Time(unit) => match unit {
                TimeUnit::Millisecond => Some(Precision::MS),
                TimeUnit::Microsecond => Some(Precision::US),
                TimeUnit::Nanosecond => Some(Precision::NS),
                _ => None,
            },
            _ => None,
        }
    }

    pub fn is_field(&self) -> bool {
        matches!(self, ColumnType::Field(_))
    }
}

impl From<ValueType> for ColumnType {
    fn from(value: ValueType) -> Self {
        Self::Field(value)
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, Default, PartialEq, Eq, Hash)]
pub struct DatabaseSchema {
    tenant: String,
    database: String,
    pub config: DatabaseOptions,
}

impl DatabaseSchema {
    pub fn new(tenant_name: &str, database_name: &str) -> Self {
        DatabaseSchema {
            tenant: tenant_name.to_string(),
            database: database_name.to_string(),
            config: DatabaseOptions::default(),
        }
    }

    pub fn new_with_options(
        tenant_name: &str,
        database_name: &str,
        options: DatabaseOptions,
    ) -> Self {
        DatabaseSchema {
            tenant: tenant_name.to_string(),
            database: database_name.to_string(),
            config: options,
        }
    }

    pub fn database_name(&self) -> &str {
        &self.database
    }

    pub fn tenant_name(&self) -> &str {
        &self.tenant
    }

    pub fn owner(&self) -> String {
        make_owner(&self.tenant, &self.database)
    }

    pub fn is_empty(&self) -> bool {
        if self.tenant.is_empty() && self.database.is_empty() {
            return true;
        }

        false
    }

    pub fn options(&self) -> &DatabaseOptions {
        &self.config
    }

    // return the min timestamp value database allowed to store
    pub fn time_to_expired(&self) -> i64 {
        let (ttl, now) = match self.config.precision_or_default() {
            Precision::MS => (
                self.config.ttl_or_default().to_millisecond(),
                crate::utils::now_timestamp_millis(),
            ),
            Precision::US => (
                self.config.ttl_or_default().to_microseconds(),
                crate::utils::now_timestamp_micros(),
            ),
            Precision::NS => (
                self.config.ttl_or_default().to_nanoseconds(),
                crate::utils::now_timestamp_nanos(),
            ),
        };
        now - ttl
    }
}

pub fn make_owner(tenant_name: &str, database_name: &str) -> String {
    format!("{}.{}", tenant_name, database_name)
}

pub fn split_owner(owner: &str) -> (&str, &str) {
    owner
        .find('.')
        .map(|index| {
            (index < owner.len())
                .then(|| (&owner[..index], &owner[(index + 1)..]))
                .unwrap_or((owner, ""))
        })
        .unwrap_or_default()
}

#[derive(Serialize, Deserialize, Debug, Default, Clone, PartialEq, Eq, Hash)]
pub struct DatabaseOptions {
    // data keep time
    ttl: Option<Duration>,

    shard_num: Option<u64>,
    // shard coverage time range
    vnode_duration: Option<Duration>,

    replica: Option<u64>,
    // timestamp precision
    precision: Option<Precision>,
}

impl DatabaseOptions {
    pub const DEFAULT_TTL: Duration = Duration {
        time_num: 0,
        unit: DurationUnit::Inf,
    };
    pub const DEFAULT_SHARD_NUM: u64 = 1;
    pub const DEFAULT_REPLICA: u64 = 1;
    pub const DEFAULT_VNODE_DURATION: Duration = Duration {
        time_num: 365,
        unit: DurationUnit::Day,
    };
    pub const DEFAULT_PRECISION: Precision = Precision::NS;

    pub fn new(
        ttl: Option<Duration>,
        shard_num: Option<u64>,
        vnode_duration: Option<Duration>,
        replica: Option<u64>,
        precision: Option<Precision>,
    ) -> Self {
        DatabaseOptions {
            ttl,
            shard_num,
            vnode_duration,
            replica,
            precision,
        }
    }

    pub fn ttl(&self) -> &Option<Duration> {
        &self.ttl
    }

    pub fn ttl_or_default(&self) -> &Duration {
        self.ttl.as_ref().unwrap_or(&DatabaseOptions::DEFAULT_TTL)
    }

    pub fn shard_num(&self) -> &Option<u64> {
        &self.shard_num
    }

    pub fn shard_num_or_default(&self) -> u64 {
        self.shard_num.unwrap_or(DatabaseOptions::DEFAULT_SHARD_NUM)
    }

    pub fn vnode_duration(&self) -> &Option<Duration> {
        &self.vnode_duration
    }

    pub fn vnode_duration_or_default(&self) -> &Duration {
        self.vnode_duration
            .as_ref()
            .unwrap_or(&DatabaseOptions::DEFAULT_VNODE_DURATION)
    }

    pub fn replica(&self) -> &Option<u64> {
        &self.replica
    }

    pub fn replica_or_default(&self) -> u64 {
        self.replica.unwrap_or(DatabaseOptions::DEFAULT_REPLICA)
    }

    pub fn precision(&self) -> &Option<Precision> {
        &self.precision
    }

    pub fn precision_or_default(&self) -> &Precision {
        self.precision
            .as_ref()
            .unwrap_or(&DatabaseOptions::DEFAULT_PRECISION)
    }

    pub fn with_ttl(&mut self, ttl: Duration) {
        self.ttl = Some(ttl);
    }

    pub fn with_shard_num(&mut self, shard_num: u64) {
        self.shard_num = Some(shard_num);
    }

    pub fn with_vnode_duration(&mut self, vnode_duration: Duration) {
        self.vnode_duration = Some(vnode_duration);
    }

    pub fn with_replica(&mut self, replica: u64) {
        self.replica = Some(replica);
    }

    pub fn with_precision(&mut self, precision: Precision) {
        self.precision = Some(precision)
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum Precision {
    MS = 0,
    US,
    NS,
}

impl From<u8> for Precision {
    fn from(value: u8) -> Self {
        match value {
            0 => Precision::MS,
            1 => Precision::US,
            2 => Precision::NS,
            _ => Precision::NS,
        }
    }
}

impl Default for Precision {
    fn default() -> Self {
        Self::NS
    }
}

impl From<TimeUnit> for Precision {
    fn from(value: TimeUnit) -> Self {
        match value {
            TimeUnit::Millisecond => Precision::MS,
            TimeUnit::Microsecond => Precision::US,
            TimeUnit::Nanosecond => Precision::NS,
            _ => Precision::NS,
        }
    }
}

impl From<Precision> for TimeUnit {
    fn from(value: Precision) -> Self {
        match value {
            Precision::MS => TimeUnit::Millisecond,
            Precision::US => TimeUnit::Microsecond,
            Precision::NS => TimeUnit::Nanosecond,
        }
    }
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

pub fn timestamp_convert(from: Precision, to: Precision, ts: Timestamp) -> Option<Timestamp> {
    match (from, to) {
        (Precision::NS, Precision::US) | (Precision::US, Precision::MS) => Some(ts / 1_000),
        (Precision::MS, Precision::US) | (Precision::US, Precision::NS) => ts.checked_mul(1_000),
        (Precision::NS, Precision::MS) => Some(ts / 1_000_000),
        (Precision::MS, Precision::NS) => ts.checked_mul(1_000_000),
        _ => Some(ts),
    }
}

impl Display for Precision {
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
    Inf,
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
            DurationUnit::Inf => write!(f, "INF"),
        }
    }
}

impl Duration {
    pub fn new_with_day(day: u64) -> Self {
        Self {
            time_num: day,
            unit: DurationUnit::Day,
        }
    }

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

    pub fn new_inf() -> Self {
        Self {
            time_num: 100000,
            unit: DurationUnit::Day,
        }
    }

    pub fn to_nanoseconds(&self) -> i64 {
        match self.unit {
            DurationUnit::Minutes => (self.time_num as i64).saturating_mul(MINUTES_NANOS),
            DurationUnit::Hour => (self.time_num as i64).saturating_mul(HOUR_NANOS),
            DurationUnit::Day => (self.time_num as i64).saturating_mul(DAY_NANOS),
            DurationUnit::Inf => i64::MAX,
        }
    }

    pub fn to_microseconds(&self) -> i64 {
        match self.unit {
            DurationUnit::Minutes => (self.time_num as i64).saturating_mul(MINUTES_MICROS),
            DurationUnit::Hour => (self.time_num as i64).saturating_mul(HOUR_MICROS),
            DurationUnit::Day => (self.time_num as i64).saturating_mul(DAY_MICROS),
            DurationUnit::Inf => i64::MAX,
        }
    }

    pub fn to_millisecond(&self) -> i64 {
        match self.unit {
            DurationUnit::Minutes => (self.time_num as i64).saturating_mul(MINUTES_MILLS),
            DurationUnit::Hour => (self.time_num as i64).saturating_mul(HOUR_MILLS),
            DurationUnit::Day => (self.time_num as i64).saturating_mul(DAY_MILLS),
            DurationUnit::Inf => i64::MAX,
        }
    }

    pub fn to_precision(&self, pre: Precision) -> i64 {
        match pre {
            Precision::MS => self.to_millisecond(),
            Precision::US => self.to_microseconds(),
            Precision::NS => self.to_nanoseconds(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Tenant {
    id: Oid,
    name: String,
    options: TenantOptions,
}

impl Identifier<Oid> for Tenant {
    fn id(&self) -> &Oid {
        &self.id
    }

    fn name(&self) -> &str {
        &self.name
    }
}

impl Tenant {
    pub fn new(id: Oid, name: String, options: TenantOptions) -> Self {
        Self { id, name, options }
    }

    pub fn options(&self) -> &TenantOptions {
        &self.options
    }

    pub fn to_own_options(self) -> TenantOptions {
        self.options
    }
}

#[derive(Debug, Default, Clone, Builder, Serialize, Deserialize)]
#[builder(setter(into, strip_option), default)]
pub struct TenantOptions {
    pub comment: Option<String>,
    pub limiter_config: Option<TenantLimiterConfig>,
}

impl From<TenantOptions> for TenantOptionsBuilder {
    fn from(value: TenantOptions) -> Self {
        let mut builder = TenantOptionsBuilder::default();
        if let Some(comment) = value.comment {
            builder.comment(comment);
        }
        if let Some(config) = value.limiter_config {
            builder.limiter_config(config);
        }
        builder
    }
}

impl TenantOptionsBuilder {
    pub fn unset_comment(&mut self) {
        self.comment = None
    }
    pub fn unset_limiter_config(&mut self) {
        self.limiter_config = None
    }
}

impl TenantOptions {
    pub fn object_config(&self) -> Option<&TenantObjectLimiterConfig> {
        match self.limiter_config {
            Some(ref limit_config) => limit_config.object_config.as_ref(),
            None => None,
        }
    }

    pub fn request_config(&self) -> Option<&RequestLimiterConfig> {
        match self.limiter_config {
            Some(ref limit_config) => limit_config.request_config.as_ref(),
            None => None,
        }
    }
}

impl Display for TenantOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(ref e) = self.comment {
            write!(f, "comment={},", e)?;
        }

        if let Some(ref e) = self.limiter_config {
            write!(f, "limiter={e:?},")?;
        } else {
            write!(f, "limiter=None,")?;
        }

        Ok(())
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Hash)]
pub struct Watermark {
    pub column: String,
    pub delay: StdDuration,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct StreamTable {
    tenant: String,
    db: String,
    name: String,
    schema: SchemaRef,
    stream_type: String,
    watermark: Watermark,
    extra_options: HashMap<String, String>,
}

impl StreamTable {
    pub fn new(
        tenant: impl Into<String>,
        db: impl Into<String>,
        name: impl Into<String>,
        schema: SchemaRef,
        stream_type: impl Into<String>,
        watermark: Watermark,
        extra_options: HashMap<String, String>,
    ) -> Self {
        Self {
            tenant: tenant.into(),
            db: db.into(),
            name: name.into(),
            schema,
            stream_type: stream_type.into(),
            watermark,
            extra_options,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn tenant(&self) -> &str {
        &self.tenant
    }

    pub fn db(&self) -> &str {
        &self.db
    }

    pub fn stream_type(&self) -> &str {
        &self.stream_type
    }

    pub fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    pub fn watermark(&self) -> &Watermark {
        &self.watermark
    }

    pub fn extra_options(&self) -> &HashMap<String, String> {
        &self.extra_options
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub enum ScalarValueForkDF {
    /// represents `DataType::Null` (castable to/from any other type)
    Null,
    /// true or false value
    Boolean(Option<bool>),
    /// 32bit float
    Float32(Option<f32>),
    /// 64bit float
    Float64(Option<f64>),
    /// 128bit decimal, using the i128 to represent the decimal, precision scale
    Decimal128(Option<i128>, u8, u8),
    /// signed 8bit int
    Int8(Option<i8>),
    /// signed 16bit int
    Int16(Option<i16>),
    /// signed 32bit int
    Int32(Option<i32>),
    /// signed 64bit int
    Int64(Option<i64>),
    /// unsigned 8bit int
    UInt8(Option<u8>),
    /// unsigned 16bit int
    UInt16(Option<u16>),
    /// unsigned 32bit int
    UInt32(Option<u32>),
    /// unsigned 64bit int
    UInt64(Option<u64>),
    /// utf-8 encoded string.
    Utf8(Option<String>),
    /// utf-8 encoded string representing a LargeString's arrow type.
    LargeUtf8(Option<String>),
    /// binary
    Binary(Option<Vec<u8>>),
    /// fixed size binary
    FixedSizeBinary(i32, Option<Vec<u8>>),
    /// large binary
    LargeBinary(Option<Vec<u8>>),
    /// list of nested ScalarValue
    List(Option<Vec<ScalarValueForkDF>>, Box<DFField>),
    /// Date stored as a signed 32bit int days since UNIX epoch 1970-01-01
    Date32(Option<i32>),
    /// Date stored as a signed 64bit int milliseconds since UNIX epoch 1970-01-01
    Date64(Option<i64>),
    /// Time stored as a signed 64bit int as nanoseconds since midnight
    Time64(Option<i64>),
    /// Timestamp Second
    TimestampSecond(Option<i64>, Option<String>),
    /// Timestamp Milliseconds
    TimestampMillisecond(Option<i64>, Option<String>),
    /// Timestamp Microseconds
    TimestampMicrosecond(Option<i64>, Option<String>),
    /// Timestamp Nanoseconds
    TimestampNanosecond(Option<i64>, Option<String>),
    /// Number of elapsed whole months
    IntervalYearMonth(Option<i32>),
    /// Number of elapsed days and milliseconds (no leap seconds)
    /// stored as 2 contiguous 32-bit signed integers
    IntervalDayTime(Option<i64>),
    /// A triple of the number of elapsed months, days, and nanoseconds.
    /// Months and days are encoded as 32-bit signed integers.
    /// Nanoseconds is encoded as a 64-bit signed integer (no leap seconds).
    IntervalMonthDayNano(Option<i128>),
    /// struct of nested ScalarValue
    Struct(Option<Vec<ScalarValueForkDF>>, Box<Vec<DFField>>),
    /// Dictionary type: index type and value
    Dictionary(Box<ArrowDataType>, Box<ScalarValueForkDF>),
}

impl From<ScalarValue> for ScalarValueForkDF {
    fn from(value: ScalarValue) -> Self {
        unsafe { std::mem::transmute::<ScalarValue, Self>(value) }
    }
}

impl From<ScalarValueForkDF> for ScalarValue {
    fn from(value: ScalarValueForkDF) -> Self {
        unsafe { std::mem::transmute::<ScalarValueForkDF, Self>(value) }
    }
}

/// column type for tskv
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Hash)]
pub enum PhysicalCType {
    Tag,
    Time(TimeUnit),
    Field(PhysicalDType),
}

impl PhysicalCType {
    pub fn default_time() -> Self {
        Self::Time(TimeUnit::Nanosecond)
    }
}

impl ColumnType {
    pub fn to_physical_type(&self) -> PhysicalCType {
        match self {
            Self::Tag => PhysicalCType::Tag,
            Self::Time(unit) => PhysicalCType::Time(unit.clone()),
            Self::Field(value_type) => PhysicalCType::Field(value_type.to_physical_type()),
        }
    }
}
