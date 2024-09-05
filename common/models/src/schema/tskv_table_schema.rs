use std::borrow::Cow;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::fmt::{self, Display};
use std::mem::size_of_val;
use std::str::FromStr;
use std::sync::Arc;

use arrow_schema::FieldRef;
use datafusion::arrow::datatypes::{
    DataType as ArrowDataType, Field as ArrowField, Schema, SchemaRef, TimeUnit,
};
use datafusion::common::{DFField, DFSchema, DFSchemaRef};
use datafusion::error::DataFusionError;
use datafusion::prelude::Column;
use serde::de::{MapAccess, SeqAccess, Visitor};
use serde::ser::SerializeStruct;
use serde::{Deserialize, Serialize, Serializer};
use snafu::ResultExt;
use utils::precision::Precision;

use crate::codec::Encoding;
use crate::errors::{InternalSnafu, InvalidSerdeMessageSnafu};
use crate::gis::data_type::{Geometry, GeometryType};
use crate::schema::{
    COLUMN_ENCODING_META_KEY, COLUMN_ID_META_KEY, DATABASE_NAME, DEFAULT_CATALOG, DEFAULT_DATABASE,
    GIS_SRID_META_KEY, GIS_SUB_TYPE_META_KEY, IS_TAG, NEXT_COLUMN_ID, SCHEMA_VERSION, TABLE_NAME,
    TENANT, TIME_FIELD_NAME,
};
use crate::value_type::ValueType;
use crate::{ColumnId, ModelError, ModelResult, PhysicalDType, SchemaVersion};

pub type TskvTableSchemaRef = Arc<TskvTableSchema>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TskvTableSchema {
    pub tenant: String,
    pub db: String,
    pub name: String,
    pub schema_version: SchemaVersion,
    next_column_id: ColumnId,

    columns: Vec<TableColumn>,
    //ColumnName -> ColumnsIndex
    columns_index: HashMap<String, usize>,
    fields_ids: HashMap<ColumnId, usize>,
}

impl Serialize for TskvTableSchema {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("TskvTableSchema", 6)?;
        state.serialize_field("tenant", &self.tenant)?;
        state.serialize_field("db", &self.db)?;
        state.serialize_field("name", &self.name)?;
        state.serialize_field("schema_version", &self.schema_version)?;
        state.serialize_field("next_column_id", &self.next_column_id)?;
        state.serialize_field("columns", &self.columns)?;
        state.serialize_field("columns_index", &self.columns_index)?;
        state.end()
    }
}

impl<'de> Deserialize<'de> for TskvTableSchema {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct TskvTableSchemaVisitor;
        impl<'de> Visitor<'de> for TskvTableSchemaVisitor {
            type Value = TskvTableSchema;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("struct TskvTableSchema")
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: SeqAccess<'de>,
            {
                let tenant = seq
                    .next_element::<String>()?
                    .ok_or_else(|| serde::de::Error::invalid_length(0, &self))?;
                let db = seq
                    .next_element::<String>()?
                    .ok_or_else(|| serde::de::Error::invalid_length(1, &self))?;
                let name = seq
                    .next_element::<String>()?
                    .ok_or_else(|| serde::de::Error::invalid_length(2, &self))?;
                let schema_version = seq
                    .next_element::<SchemaVersion>()?
                    .ok_or_else(|| serde::de::Error::invalid_length(3, &self))?;
                let next_column_id = seq
                    .next_element::<ColumnId>()?
                    .ok_or_else(|| serde::de::Error::invalid_length(4, &self))?;
                let columns = seq
                    .next_element::<Vec<TableColumn>>()?
                    .ok_or_else(|| serde::de::Error::invalid_length(5, &self))?;
                let columns_index = seq
                    .next_element::<HashMap<String, usize>>()?
                    .ok_or_else(|| serde::de::Error::invalid_length(6, &self))?;
                let fields_ids = TskvTableSchema::build_fields_ids(&columns);
                Ok(TskvTableSchema {
                    tenant,
                    db,
                    name,
                    schema_version,
                    next_column_id,
                    columns,
                    columns_index,
                    fields_ids,
                })
            }

            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: MapAccess<'de>,
            {
                let mut tenant = None;
                let mut db = None;
                let mut name = None;
                let mut schema_version = None;
                let mut next_column_id = None;
                let mut columns = None;
                let mut columns_index = None;
                while let Some(key) = map.next_key()? {
                    match key {
                        "tenant" => {
                            if tenant.is_some() {
                                return Err(serde::de::Error::duplicate_field("tenant"));
                            }
                            tenant = Some(map.next_value::<String>()?);
                        }
                        "db" => {
                            if db.is_some() {
                                return Err(serde::de::Error::duplicate_field("db"));
                            }
                            db = Some(map.next_value::<String>()?);
                        }
                        "name" => {
                            if name.is_some() {
                                return Err(serde::de::Error::duplicate_field("name"));
                            }
                            name = Some(map.next_value::<String>()?);
                        }
                        "schema_version" => {
                            if schema_version.is_some() {
                                return Err(serde::de::Error::duplicate_field("schema_version"));
                            }
                            schema_version = Some(map.next_value::<SchemaVersion>()?);
                        }
                        "next_column_id" => {
                            if next_column_id.is_some() {
                                return Err(serde::de::Error::duplicate_field("next_column_id"));
                            }
                            next_column_id = Some(map.next_value::<ColumnId>()?);
                        }
                        "columns" => {
                            if columns.is_some() {
                                return Err(serde::de::Error::duplicate_field("columns"));
                            }
                            columns = Some(map.next_value::<Vec<TableColumn>>()?);
                        }
                        "columns_index" => {
                            if columns_index.is_some() {
                                return Err(serde::de::Error::duplicate_field("columns_index"));
                            }
                            columns_index = Some(map.next_value::<HashMap<String, usize>>()?);
                        }
                        _ => {
                            return Err(serde::de::Error::unknown_field(
                                key,
                                &[
                                    "tenant",
                                    "db",
                                    "name",
                                    "schema_version",
                                    "next_column_id",
                                    "columns",
                                    "columns_index",
                                ],
                            ))?;
                        }
                    }
                }
                let tenant = tenant.ok_or_else(|| serde::de::Error::missing_field("tenant"))?;
                let db = db.ok_or_else(|| serde::de::Error::missing_field("db"))?;
                let name = name.ok_or_else(|| serde::de::Error::missing_field("name"))?;
                let schema_version = schema_version
                    .ok_or_else(|| serde::de::Error::missing_field("schema_version"))?;
                let next_column_id = next_column_id
                    .ok_or_else(|| serde::de::Error::missing_field("next_column_id"))?;
                let columns = columns.ok_or_else(|| serde::de::Error::missing_field("columns"))?;
                let columns_index = columns
                    .iter()
                    .enumerate()
                    .map(|(idx, e)| (e.name.clone(), idx))
                    .collect();
                let fields_ids = TskvTableSchema::build_fields_ids(&columns);
                Ok(TskvTableSchema {
                    tenant,
                    db,
                    name,
                    schema_version,
                    next_column_id,
                    columns,
                    columns_index,
                    fields_ids,
                })
            }
        }
        deserializer.deserialize_struct(
            "TskvTableSchema",
            &[
                "tenant",
                "db",
                "name",
                "schema_version",
                "next_column_id",
                "columns",
                "columns_index",
            ],
            TskvTableSchemaVisitor,
        )
    }
}

impl PartialOrd for TskvTableSchema {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.schema_version.cmp(&other.schema_version))
    }
}

impl Ord for TskvTableSchema {
    fn cmp(&self, other: &Self) -> Ordering {
        self.tenant
            .cmp(&other.tenant)
            .then_with(|| self.db.cmp(&other.db))
            .then_with(|| self.name.cmp(&other.name))
            .then_with(|| self.schema_version.cmp(&other.schema_version))
    }
}

impl Default for TskvTableSchema {
    fn default() -> Self {
        Self {
            tenant: DEFAULT_CATALOG.to_string(),
            db: DEFAULT_DATABASE.to_string(),
            name: "template".to_string(),
            schema_version: 0,
            next_column_id: 0,
            columns: Default::default(),
            columns_index: Default::default(),
            fields_ids: Default::default(),
        }
    }
}

impl TskvTableSchema {
    pub fn meta(&self) -> HashMap<String, String> {
        let mut meta = HashMap::new();
        meta.insert(TENANT.to_string(), self.tenant.clone());
        meta.insert(DATABASE_NAME.to_string(), self.db.clone());
        meta.insert(TABLE_NAME.to_string(), self.name.clone());
        meta.insert(SCHEMA_VERSION.to_string(), self.schema_version.to_string());
        meta.insert(NEXT_COLUMN_ID.to_string(), self.next_column_id.to_string());
        meta
    }

    pub fn to_record_data_schema(&self) -> SchemaRef {
        let fields: Vec<ArrowField> = self
            .columns
            .iter()
            .filter(|column| !column.column_type.is_tag())
            .map(|field| field.into())
            .collect();
        Arc::new(Schema::new_with_metadata(fields, self.meta()))
    }

    pub fn schema_version_from_arrow_array_schema(schema: SchemaRef) -> ModelResult<SchemaVersion> {
        let schema_version =
            SchemaVersion::from_str(schema.metadata().get(SCHEMA_VERSION).ok_or_else(|| {
                InternalSnafu {
                    err: format!("schema version not found in schema: {:?}", schema),
                }
                .build()
            })?)
            .map_err(|e| {
                InternalSnafu {
                    err: format!("parse schema version error: {}", e),
                }
                .build()
            })?;
        Ok(schema_version)
    }

    pub fn to_arrow_schema(&self) -> SchemaRef {
        let fields: Vec<ArrowField> = self.columns.iter().map(|field| field.into()).collect();
        Arc::new(Schema::new_with_metadata(fields, self.meta()))
    }

    pub fn to_df_schema(&self) -> Result<DFSchemaRef, DataFusionError> {
        let fields: Vec<DFField> = self
            .columns
            .iter()
            .map(ArrowField::from)
            .map(|f| DFField::from_qualified(self.name.as_str(), f))
            .collect();
        Ok(Arc::new(DFSchema::new_with_metadata(fields, self.meta())?))
    }

    pub fn new(tenant: String, db: String, name: String, columns: Vec<TableColumn>) -> Self {
        let columns_index = columns
            .iter()
            .enumerate()
            .map(|(idx, e)| (e.name.clone(), idx))
            .collect();

        let fields_ids = Self::build_fields_ids(&columns);
        Self {
            tenant,
            db,
            name,
            schema_version: 0,
            next_column_id: columns.len() as ColumnId,
            columns,
            columns_index,
            fields_ids,
        }
    }

    pub fn build_fields_ids(columns: &[TableColumn]) -> HashMap<ColumnId, usize> {
        let mut ans = vec![];
        for i in columns.iter() {
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
        self.fields_ids = Self::build_fields_ids(&self.columns);
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
        self.fields_ids = Self::build_fields_ids(&self.columns);
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
        self.fields_ids = Self::build_fields_ids(&self.columns);
    }

    /// Get the metadata of the column according to the column name
    pub fn column(&self, name: &str) -> Option<&TableColumn> {
        self.columns_index
            .get(name)
            .map(|idx| unsafe { self.columns.get_unchecked(*idx) })
    }

    pub fn column_id_column_map(&self) -> HashMap<ColumnId, &TableColumn> {
        self.columns.iter().map(|c| (c.id, c)).collect()
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

    pub fn column_ids(&self) -> Vec<ColumnId> {
        self.columns.iter().map(|c| c.id).collect()
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
    pub fn fields_id(&self) -> &HashMap<ColumnId, usize> {
        &self.fields_ids
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

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Hash, Ord, PartialOrd)]
pub struct TableColumn {
    pub id: ColumnId,
    pub name: String,
    pub column_type: ColumnType,
    pub encoding: Encoding,
}

impl TryFrom<FieldRef> for TableColumn {
    type Error = ModelError;

    fn try_from(value: FieldRef) -> Result<Self, Self::Error> {
        let column_id = value
            .metadata()
            .get(COLUMN_ID_META_KEY)
            .map(|v| v.parse::<ColumnId>())
            .ok_or_else(|| {
                InternalSnafu {
                    err: format!("Column id not found in metadata: {:?}", value),
                }
                .build()
            })?
            .map_err(|e| {
                InternalSnafu {
                    err: format!("Failed to parse column id: {:?}", e),
                }
                .build()
            })?;

        let name = value.name().clone();

        let encoding = value
            .metadata()
            .get(COLUMN_ENCODING_META_KEY)
            .map(|v| Encoding::from_str(v))
            .unwrap_or_else(|| Ok(Encoding::Default))
            .map_err(|e| {
                InternalSnafu {
                    err: format!("Failed to parse column encoding: {:?}", e),
                }
                .build()
            })?;

        if let (Some(k), Some(v)) = (
            value.metadata().get(GIS_SUB_TYPE_META_KEY),
            value.metadata().get(GIS_SRID_META_KEY),
        ) {
            let sub_type = GeometryType::from_str(k).map_err(|e| {
                InternalSnafu {
                    err: format!("Failed to parse gis sub type: {:?}", e),
                }
                .build()
            })?;
            let srid = v.parse::<i16>().map_err(|e| {
                InternalSnafu {
                    err: format!("Failed to parse gis srid: {:?}", e),
                }
                .build()
            })?;
            return Ok(TableColumn::new(
                column_id,
                name,
                ColumnType::Field(ValueType::Geometry(Geometry { sub_type, srid })),
                Encoding::Default,
            ));
        }
        if value.metadata().contains_key(IS_TAG) {
            Ok(TableColumn::new_tag_column(column_id, name))
        } else {
            let column_type = value.data_type().clone().into();
            Ok(TableColumn::new(column_id, name, column_type, encoding))
        }
    }
}

impl From<&TableColumn> for ArrowField {
    fn from(column: &TableColumn) -> Self {
        let mut map = HashMap::new();
        map.insert(COLUMN_ID_META_KEY.to_string(), column.id.to_string());
        map.insert(
            COLUMN_ENCODING_META_KEY.to_string(),
            column.encoding.as_str().to_string(),
        );

        // 通过 SRID_META_KEY 标记 Geometry 类型的列
        if let ColumnType::Field(ValueType::Geometry(Geometry { srid, sub_type })) =
            column.column_type
        {
            map.insert(GIS_SUB_TYPE_META_KEY.to_string(), sub_type.to_string());
            map.insert(GIS_SRID_META_KEY.to_string(), srid.to_string());
        }

        if let ColumnType::Tag = column.column_type {
            map.insert(IS_TAG.to_string(), "".to_string());
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

    pub fn encode(&self) -> ModelResult<Vec<u8>> {
        let buf = bincode::serialize(&self).context(InvalidSerdeMessageSnafu)?;

        Ok(buf)
    }

    pub fn decode(buf: &[u8]) -> ModelResult<Self> {
        let column = bincode::deserialize::<TableColumn>(buf).context(InvalidSerdeMessageSnafu)?;

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

    pub fn column_id_from_arrow_field(field: FieldRef) -> ModelResult<ColumnId> {
        let column_id = field
            .metadata()
            .get(COLUMN_ID_META_KEY)
            .ok_or_else(|| {
                InternalSnafu {
                    err: format!("Column id not found in metadata: {:?}", field),
                }
                .build()
            })?
            .parse::<ColumnId>()
            .map_err(|e| {
                InternalSnafu {
                    err: format!("Failed to parse column id: {:?}", e),
                }
                .build()
            })?;
        Ok(column_id)
    }

    pub fn encoding(&self) -> Encoding {
        self.encoding
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

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Hash, Ord, PartialOrd)]
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
            Self::Field(ValueType::Geometry(..)) => "GEOMETRY",
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

    pub fn from_proto_field_type(field_type: protos::models::FieldType) -> Self {
        match field_type.0 {
            0 => Self::Field(ValueType::Float),
            1 => Self::Field(ValueType::Integer),
            2 => Self::Field(ValueType::Unsigned),
            3 => Self::Field(ValueType::Boolean),
            4 => Self::Field(ValueType::String),
            _ => Self::Field(ValueType::Unknown),
        }
    }

    pub fn to_sql_type_str_with_unit(&self) -> Cow<'static, str> {
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

    pub fn to_physical_type(&self) -> PhysicalCType {
        match self {
            Self::Tag => PhysicalCType::Tag,
            Self::Time(unit) => PhysicalCType::Time(unit.clone()),
            Self::Field(value_type) => PhysicalCType::Field(value_type.to_physical_type()),
        }
    }

    pub fn to_physical_data_type(&self) -> PhysicalDType {
        match self {
            Self::Tag => PhysicalDType::String,
            Self::Time(_) => PhysicalDType::Integer,
            Self::Field(value_type) => value_type.to_physical_type(),
        }
    }
}

impl From<ArrowDataType> for ColumnType {
    fn from(t: ArrowDataType) -> Self {
        match t {
            ArrowDataType::Timestamp(unit, _) => ColumnType::Time(unit),
            ArrowDataType::Float64 => ColumnType::Field(ValueType::Float),
            ArrowDataType::Int64 => ColumnType::Field(ValueType::Integer),
            ArrowDataType::UInt64 => ColumnType::Field(ValueType::Unsigned),
            ArrowDataType::Boolean => ColumnType::Field(ValueType::Boolean),
            ArrowDataType::Utf8 => ColumnType::Field(ValueType::String),
            _ => ColumnType::Field(ValueType::Unknown),
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

    pub fn matches_type(&self, other: &ColumnType) -> bool {
        self.eq(other)
            || (matches!(self, ColumnType::Field(ValueType::Geometry(..)))
                && matches!(other, ColumnType::Field(ValueType::String)))
    }
}

impl From<ValueType> for ColumnType {
    fn from(value: ValueType) -> Self {
        Self::Field(value)
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

    pub fn to_physical_data_type(&self) -> PhysicalDType {
        match self {
            Self::Tag => PhysicalDType::String,
            Self::Time(_) => PhysicalDType::Integer,
            Self::Field(value_type) => *value_type,
        }
    }
}
