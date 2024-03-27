use std::cmp::{max, min};
use std::collections::{BTreeMap, HashMap};
use std::io::IoSlice;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use minivec::MiniVec;
use models::codec::Encoding;
use models::field_value::FieldVal;
use models::predicate::domain::TimeRange;
use models::schema::{ColumnType, TableColumn, TskvTableSchemaRef};
use models::{SeriesId, SeriesKey, ValueType};
use snafu::ResultExt;
use utils::bitset::BitSet;
use utils::BloomFilter;

use super::statistics::ValueStatistics;
use crate::compaction::CompactingBlock;
use crate::error::IOSnafu;
use crate::file_system::async_filesystem::{LocalFileSystem, LocalFileType};
use crate::file_system::file::stream_writer::FileStreamWriter;
use crate::file_system::FileSystem;
use crate::file_utils::{make_delta_file, make_tsm_file};
use crate::tsm::codec::{
    get_bool_codec, get_f64_codec, get_i64_codec, get_str_codec, get_u64_codec,
};
use crate::tsm::TsmTombstone;
use crate::tsm2::page::{
    Chunk, ChunkGroup, ChunkGroupMeta, ChunkGroupWriteSpec, ChunkStatics, ChunkWriteSpec,
    ColumnGroup, Footer, Page, PageMeta, PageStatistics, PageWriteSpec, SeriesMeta, TableMeta,
};
use crate::tsm2::{TsmWriteData, BLOOM_FILTER_BITS};
use crate::{Error, Result};

// #[derive(Debug, Clone)]
// pub enum Array {
//     F64(Vec<Option<f64>>),
//     I64(Vec<Option<i64>>),
//     U64(Vec<Option<u64>>),
//     String(Vec<Option<MiniVec<u8>>>),
//     Bool(Vec<Option<bool>>),
// }
//
// impl Array {
//     pub fn push(&mut self, value: Option<FieldVal>) {
//         match (self, value) {
//             (Array::F64(array), Some(FieldVal::Float(v))) => array.push(Some(v)),
//             (Array::F64(array), None) => array.push(None),
//
//             (Array::I64(array), Some(FieldVal::Integer(v))) => array.push(Some(v)),
//             (Array::I64(array), None) => array.push(None),
//
//             (Array::U64(array), Some(FieldVal::Unsigned(v))) => array.push(Some(v)),
//             (Array::U64(array), None) => array.push(None),
//
//             (Array::String(array), Some(FieldVal::Bytes(v))) => array.push(Some(v)),
//             (Array::String(array), None) => array.push(None),
//
//             (Array::Bool(array), Some(FieldVal::Boolean(v))) => array.push(Some(v)),
//             (Array::Bool(array), None) => array.push(None),
//             _ => { panic!("invalid type: array type does not match filed type") }
//         }
//     }
// }
//
//
// #[derive(Debug, Clone)]
// pub enum Array2 {
//     F64(Vec<Option<f64>>),
//     I64(Vec<Option<i64>>),
//     U64(Vec<Option<u64>>),
//     String(Vec<Option<MiniVec<u8>>>),
//     Bool(Vec<Option<bool>>),
// }
//
// impl Array2 {
//     pub fn push(&mut self, value: Option<FieldVal>) {
//         match (self, value) {
//             (Array2::F64(array), Some(FieldVal::Float(v))) => array.push(Some(v)),
//             (Array2::F64(array), None) => array.push(None),
//
//             (Array2::I64(array), Some(FieldVal::Integer(v))) => array.push(Some(v)),
//             (Array2::I64(array), None) => array.push(None),
//
//             (Array2::U64(array), Some(FieldVal::Unsigned(v))) => array.push(Some(v)),
//             (Array2::U64(array), None) => array.push(None),
//
//             (Array2::String(array), Some(FieldVal::Bytes(v))) => array.push(Some(v)),
//             (Array2::String(array), None) => array.push(None),
//
//             (Array2::Bool(array), Some(FieldVal::Boolean(v))) => array.push(Some(v)),
//             (Array2::Bool(array), None) => array.push(None),
//             _ => { panic!("invalid type: array type does not match filed type") }
//         }
//     }
// }

/// max size 1024
#[derive(Debug, Clone, PartialEq)]
pub struct Column {
    column_type: ColumnType,
    valid: BitSet,
    data: ColumnData,
}

impl Column {
    pub fn new(column_type: ColumnType, valid: BitSet, data: ColumnData) -> Column {
        Column {
            column_type,
            valid,
            data,
        }
    }

    pub fn empty(column_type: ColumnType) -> Result<Column> {
        let valid = BitSet::new();
        let column = Self {
            column_type: column_type.clone(),
            valid,
            data: match column_type {
                ColumnType::Tag => ColumnData::String(vec![], String::new(), String::new()),
                ColumnType::Time(_) => ColumnData::I64(vec![], i64::MAX, i64::MIN),
                ColumnType::Field(ref field_type) => match field_type {
                    ValueType::Float => ColumnData::F64(vec![], f64::MAX, f64::MIN),
                    ValueType::Integer => ColumnData::I64(vec![], i64::MAX, i64::MIN),
                    ValueType::Unsigned => ColumnData::U64(vec![], u64::MAX, u64::MIN),
                    ValueType::Boolean => ColumnData::Bool(vec![], false, true),
                    ValueType::Geometry(_) | ValueType::String => {
                        ColumnData::String(vec![], String::new(), String::new())
                    }
                    ValueType::Unknown => {
                        return Err(Error::UnsupportedDataType {
                            dt: "unknown".to_string(),
                        })
                    }
                },
            },
        };
        Ok(column)
    }

    pub fn empty_with_cap(column_type: ColumnType, cap: usize) -> Result<Column> {
        let valid = BitSet::with_size(cap);
        let column = Self {
            column_type: column_type.clone(),
            valid,
            data: match column_type {
                ColumnType::Tag => {
                    ColumnData::String(Vec::with_capacity(cap), String::new(), String::new())
                }
                ColumnType::Time(_) => ColumnData::I64(Vec::with_capacity(cap), i64::MAX, i64::MIN),
                ColumnType::Field(ref field_type) => match field_type {
                    ValueType::Float => {
                        ColumnData::F64(Vec::with_capacity(cap), f64::MAX, f64::MIN)
                    }
                    ValueType::Integer => {
                        ColumnData::I64(Vec::with_capacity(cap), i64::MAX, i64::MIN)
                    }
                    ValueType::Unsigned => {
                        ColumnData::U64(Vec::with_capacity(cap), u64::MAX, u64::MIN)
                    }
                    ValueType::Boolean => ColumnData::Bool(Vec::with_capacity(cap), false, true),
                    ValueType::Geometry(_) | ValueType::String => {
                        ColumnData::String(Vec::with_capacity(cap), String::new(), String::new())
                    }
                    ValueType::Unknown => {
                        return Err(Error::UnsupportedDataType {
                            dt: "unknown".to_string(),
                        })
                    }
                },
            },
        };
        Ok(column)
    }

    pub fn push(&mut self, value: Option<FieldVal>) {
        match (&mut self.data, value) {
            (ColumnData::F64(ref mut value, min, max), Some(FieldVal::Float(val))) => {
                if *max < val {
                    *max = val;
                }
                if *min > val {
                    *min = val;
                }
                value.push(val);
                let idx = value.len() - 1;
                self.valid.append_unset_and_set(idx);
            }
            (ColumnData::F64(ref mut value, ..), None) => {
                value.push(0.0);
                if self.valid.len() < value.len() {
                    self.valid.append_unset(1);
                }
            }
            (ColumnData::I64(ref mut value, min, max), Some(FieldVal::Integer(val))) => {
                if *max < val {
                    *max = val;
                }
                if *min > val {
                    *min = val;
                }
                value.push(val);
                let idx = value.len() - 1;
                self.valid.append_unset_and_set(idx);
            }
            (ColumnData::I64(ref mut value, ..), None) => {
                value.push(0);
                if self.valid.len() < value.len() {
                    self.valid.append_unset(1);
                }
            }
            (ColumnData::U64(ref mut value, min, max), Some(FieldVal::Unsigned(val))) => {
                if *max < val {
                    *max = val;
                }
                if *min > val {
                    *min = val;
                }
                value.push(val);
                let idx = value.len() - 1;
                self.valid.append_unset_and_set(idx);
            }
            (ColumnData::U64(ref mut value, ..), None) => {
                value.push(0);
                if self.valid.len() < value.len() {
                    self.valid.append_unset(1);
                }
            }
            //todo: need to change string to Bytes type in ColumnData
            (ColumnData::String(ref mut value, min, max), Some(FieldVal::Bytes(val))) => {
                let val = String::from_utf8(val.to_vec()).unwrap();
                if *max < val {
                    *max = val.clone();
                }
                if *min > val {
                    *min = val.clone();
                }
                value.push(val);
                let idx = value.len() - 1;
                self.valid.append_unset_and_set(idx);
            }
            (ColumnData::String(ref mut value, ..), None) => {
                value.push(String::new());
                if self.valid.len() < value.len() {
                    self.valid.append_unset(1);
                }
            }
            (ColumnData::Bool(ref mut value, min, max), Some(FieldVal::Boolean(val))) => {
                if !(*max) & val {
                    *max = val;
                }
                if *min & !val {
                    *min = val;
                }
                value.push(val);
                let idx = value.len() - 1;
                self.valid.append_unset_and_set(idx);
            }
            (ColumnData::Bool(ref mut value, ..), None) => {
                value.push(false);
                if self.valid.len() < value.len() {
                    self.valid.append_unset(1);
                }
            }
            _ => {
                panic!("Column type mismatch")
            }
        }
    }
    pub fn col_to_page(&self, desc: &TableColumn) -> Result<Page> {
        let null_count = 1;
        let len_bitset = self.valid.byte_len() as u32;
        let data_len = self.len() as u64;
        let mut buf = vec![];
        let statistics = match &self.data {
            ColumnData::F64(array, min, max) => {
                let encoder = get_f64_codec(desc.encoding);
                encoder
                    .encode(array, &mut buf)
                    .map_err(|e| Error::Encode { source: e })?;
                PageStatistics::F64(ValueStatistics::new(
                    Some(*min),
                    Some(*max),
                    None,
                    null_count,
                ))
            }
            ColumnData::I64(array, min, max) => {
                let encoder = get_i64_codec(desc.encoding);
                encoder
                    .encode(array, &mut buf)
                    .map_err(|e| Error::Encode { source: e })?;
                PageStatistics::I64(ValueStatistics::new(
                    Some(*min),
                    Some(*max),
                    None,
                    null_count,
                ))
            }
            ColumnData::U64(array, min, max) => {
                let encoder = get_u64_codec(desc.encoding);
                encoder
                    .encode(array, &mut buf)
                    .map_err(|e| Error::Encode { source: e })?;
                PageStatistics::U64(ValueStatistics::new(
                    Some(*min),
                    Some(*max),
                    None,
                    null_count,
                ))
            }
            ColumnData::String(array, min, max) => {
                let encoder = get_str_codec(desc.encoding);
                encoder
                    .encode(
                        &array.iter().map(|it| it.as_bytes()).collect::<Vec<_>>(),
                        &mut buf,
                    )
                    .map_err(|e| Error::Encode { source: e })?;
                PageStatistics::Bytes(ValueStatistics::new(
                    Some(min.as_bytes().to_vec()),
                    Some(max.as_bytes().to_vec()),
                    None,
                    null_count,
                ))
            }
            ColumnData::Bool(array, min, max) => {
                let encoder = get_bool_codec(desc.encoding);
                encoder
                    .encode(array, &mut buf)
                    .map_err(|e| Error::Encode { source: e })?;
                PageStatistics::Bool(ValueStatistics::new(
                    Some(*min),
                    Some(*max),
                    None,
                    null_count,
                ))
            }
        };
        let mut data = vec![];
        data.extend_from_slice(&len_bitset.to_be_bytes());
        data.extend_from_slice(&data_len.to_be_bytes());
        data.extend_from_slice(self.valid.bytes());
        data.extend_from_slice(&buf);
        let bytes = bytes::Bytes::from(data);
        let meta = PageMeta {
            num_values: self.data.len() as u32,
            column: desc.clone(),
            statistics,
        };
        Ok(Page { bytes, meta })
    }

    pub fn get(&self, index: usize) -> Option<FieldVal> {
        if self.valid.len() <= index || self.data.len() <= index {
            return None;
        }
        if self.valid.get(index) {
            self.data.get(index)
        } else {
            None
        }
    }

    pub fn chunk(&self, start: usize, end: usize) -> Result<Column> {
        let mut column = Column::empty_with_cap(self.column_type.clone(), end - start)?;
        for index in start..end {
            column.push(self.get(index));
        }
        Ok(column)
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    pub fn is_all_set(&self) -> bool {
        self.valid.is_all_set()
    }

    /// only use for Timastamp column, other will return Err(0)
    pub fn binary_search_for_i64_col(&self, value: i64) -> Result<usize, usize> {
        self.data.binary_search_for_i64_col(value)
    }

    pub fn valid(&self) -> &BitSet {
        &self.valid
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum ColumnData {
    ///   array   min, max
    F64(Vec<f64>, f64, f64),
    I64(Vec<i64>, i64, i64),
    U64(Vec<u64>, u64, u64),
    String(Vec<String>, String, String),
    Bool(Vec<bool>, bool, bool),
}

impl ColumnData {
    pub fn get(&self, index: usize) -> Option<FieldVal> {
        return match self {
            ColumnData::F64(data, _, _) => data.get(index).map(|val| FieldVal::Float(*val)),
            ColumnData::I64(data, _, _) => data.get(index).map(|val| FieldVal::Integer(*val)),
            ColumnData::U64(data, _, _) => data.get(index).map(|val| FieldVal::Unsigned(*val)),
            ColumnData::String(data, _, _) => data
                .get(index)
                .map(|val| FieldVal::Bytes(MiniVec::from(val.as_str()))),
            ColumnData::Bool(data, _, _) => data.get(index).map(|val| FieldVal::Boolean(*val)),
        };
    }

    pub fn len(&self) -> usize {
        match self {
            ColumnData::F64(data, _, _) => data.len(),
            ColumnData::I64(data, _, _) => data.len(),
            ColumnData::U64(data, _, _) => data.len(),
            ColumnData::String(data, _, _) => data.len(),
            ColumnData::Bool(data, _, _) => data.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        match self {
            ColumnData::F64(data, _, _) => data.is_empty(),
            ColumnData::I64(data, _, _) => data.is_empty(),
            ColumnData::U64(data, _, _) => data.is_empty(),
            ColumnData::String(data, _, _) => data.is_empty(),
            ColumnData::Bool(data, _, _) => data.is_empty(),
        }
    }

    /// only use for Timastamp column, other will return Err(0)
    pub fn binary_search_for_i64_col(&self, value: i64) -> Result<usize, usize> {
        match self {
            ColumnData::I64(data, ..) => data.binary_search(&value),
            _ => Err(0),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct DataBlock2 {
    schema: TskvTableSchemaRef,
    ts: Column,
    ts_desc: TableColumn,
    cols: Vec<Column>,
    cols_desc: Vec<TableColumn>,
}

enum Merge {
    SelfTs(usize),
    OtherTs(usize),
    Equal(usize, usize),
}

impl DataBlock2 {
    const BLOCK_SIZE: usize = 1000;

    pub fn new(
        schema: TskvTableSchemaRef,
        ts: Column,
        ts_desc: TableColumn,
        cols: Vec<Column>,
        cols_desc: Vec<TableColumn>,
    ) -> Self {
        DataBlock2 {
            schema,
            ts,
            ts_desc,
            cols,
            cols_desc,
        }
    }

    pub fn schema(&self) -> TskvTableSchemaRef {
        self.schema.clone()
    }

    pub fn block_to_page(&self) -> Result<Vec<Page>> {
        let mut pages = Vec::with_capacity(self.cols.len() + 1);
        pages.push(self.ts.col_to_page(&self.ts_desc)?);
        for (col, desc) in self.cols.iter().zip(self.cols_desc.iter()) {
            pages.push(col.col_to_page(desc)?);
        }
        Ok(pages)
    }

    pub fn merge(&mut self, other: DataBlock2) -> Result<DataBlock2> {
        self.schema_check(&other)?;

        let schema = if self.schema.schema_id > other.schema.schema_id {
            self.schema.clone()
        } else {
            other.schema.clone()
        };
        let (sort_index, time_array) = self.sort_index_and_time_col(&other)?;
        let mut columns = Vec::new();
        let mut columns_des = Vec::new();
        for field in schema.fields() {
            let mut merge_column =
                Column::empty_with_cap(field.column_type.clone(), time_array.len())?;
            let column_self = self.column(&field.name);
            let column_other = other.column(&field.name);
            for idx in sort_index.iter() {
                match idx {
                    Merge::SelfTs(index) => {
                        if let Some(column_self) = column_self {
                            merge_column.push(column_self.get(*index));
                        } else {
                            merge_column.push(None);
                        }
                    }
                    Merge::OtherTs(index) => {
                        if let Some(column_other) = column_other {
                            merge_column.push(column_other.get(*index));
                        } else {
                            merge_column.push(None);
                        }
                    }
                    Merge::Equal(index_self, index_other) => {
                        let field_self = if let Some(column_self) = column_self {
                            column_self.get(*index_self)
                        } else {
                            None
                        };
                        let field_other = if let Some(column_other) = column_other {
                            column_other.get(*index_other)
                        } else {
                            None
                        };
                        if field_self.is_some() {
                            merge_column.push(field_self);
                        } else {
                            merge_column.push(field_other);
                        }
                    }
                }
            }
            columns.push(merge_column);
            columns_des.push(field.clone());
        }

        let mut ts_col =
            Column::empty_with_cap(self.ts_desc.column_type.clone(), time_array.len())?;

        time_array
            .iter()
            .for_each(|it| ts_col.push(Some(FieldVal::Integer(*it))));
        let datablock = DataBlock2::new(schema, ts_col, self.ts_desc.clone(), columns, columns_des);

        // todo: split datablock to blocks
        // let mut blocks = Vec::with_capacity(time_array.len() / Self::BLOCK_SIZE + 1);
        // blocks.push(datablock);

        Ok(datablock)
    }

    fn sort_index_and_time_col(&self, other: &DataBlock2) -> Result<(Vec<Merge>, Vec<i64>)> {
        let mut sort_index = Vec::with_capacity(self.len() + other.len());
        let mut time_array = Vec::new();
        let (mut index_self, mut index_other) = (0_usize, 0_usize);
        let (self_len, other_len) = (self.len(), other.len());
        while index_self < self_len && index_other < other_len {
            match (&self.ts.data, &other.ts.data) {
                (ColumnData::I64(ref data1, ..), ColumnData::I64(ref data2, ..)) => {
                    match data1[index_self].cmp(&data2[index_other]) {
                        std::cmp::Ordering::Less => {
                            sort_index.push(Merge::SelfTs(index_self));
                            time_array.push(data1[index_self]);
                            index_self += 1;
                        }
                        std::cmp::Ordering::Greater => {
                            sort_index.push(Merge::OtherTs(index_other));
                            time_array.push(data2[index_other]);
                            index_other += 1;
                        }
                        std::cmp::Ordering::Equal => {
                            sort_index.push(Merge::Equal(index_self, index_other));
                            time_array.push(data1[index_self]);
                            index_self += 1;
                            index_other += 1;
                        }
                    }
                }
                _ => {
                    return Err(Error::DataBlockError {
                        reason: "Time column does not support except i64 physical data type"
                            .to_string(),
                    })
                }
            }
        }

        match (&self.ts.data, &other.ts.data) {
            (ColumnData::I64(ref data1, ..), ColumnData::I64(ref data2, ..)) => {
                while index_self < self_len {
                    sort_index.push(Merge::SelfTs(index_self));
                    time_array.push(data1[index_self]);
                    index_self += 1;
                }
                while index_other < other_len {
                    sort_index.push(Merge::OtherTs(index_other));
                    time_array.push(data2[index_other]);
                    index_other += 1;
                }
            }
            _ => {
                return Err(Error::DataBlockError {
                    reason: "Time column does not support except i64 physical data type"
                        .to_string(),
                })
            }
        }
        Ok((sort_index, time_array))
    }

    pub fn len(&self) -> usize {
        self.ts.data.len()
    }

    pub fn is_empty(&self) -> bool {
        self.ts.data.is_empty()
    }

    pub fn schema_check(&self, other: &DataBlock2) -> Result<()> {
        if self.schema.name != other.schema.name
            || self.schema.db != other.schema.db
            || self.schema.tenant != other.schema.tenant
        {
            return Err(Error::CommonError {
                reason: format!(
                    "schema name not match in datablock merge, self: {}.{}.{}, other: {}.{}.{}",
                    self.schema.tenant,
                    self.schema.db,
                    self.schema.name,
                    other.schema.tenant,
                    other.schema.db,
                    other.schema.name
                ),
            });
        }
        Ok(())
    }

    pub fn column(&self, name: &str) -> Option<&Column> {
        for (index, col_des) in self.cols_desc.iter().enumerate() {
            if col_des.name == name {
                return Some(&self.cols[index]);
            }
        }
        None
    }

    pub fn chunk(&self, start: usize, end: usize) -> Result<DataBlock2> {
        if start > end || end > self.len() {
            return Err(Error::CommonError {
                reason: "start or end index out of range".to_string(),
            });
        }

        let ts_column = self.ts.chunk(start, end)?;
        let other_colums = self
            .cols
            .iter()
            .map(|column| column.chunk(start, end))
            .collect::<Result<Vec<_>>>()?;
        let datablock = DataBlock2::new(
            self.schema.clone(),
            ts_column,
            self.ts_desc.clone(),
            other_colums,
            self.cols_desc.clone(),
        );

        Ok(datablock)
    }

    pub fn filter_by_tomb(
        &mut self,
        tombstone: Arc<TsmTombstone>,
        series_id: SeriesId,
    ) -> Result<()> {
        let time_range = self.time_range()?;
        for (column, col_desc) in self.cols.iter_mut().zip(self.cols_desc.iter()) {
            let time_ranges =
                tombstone.get_overlapped_time_ranges(series_id, col_desc.id, &time_range);
            for time_range in time_ranges {
                let index_begin = self
                    .ts
                    .data
                    .binary_search_for_i64_col(time_range.min_ts)
                    .unwrap_or_else(|index| index);
                let index_end = self
                    .ts
                    .data
                    .binary_search_for_i64_col(time_range.max_ts)
                    .map(|index| index + 1)
                    .unwrap_or_else(|index| index);
                if index_begin == index_end {
                    continue;
                }
                column.valid.clear_bits(index_begin, index_end);
            }
        }
        Ok(())
    }

    pub fn time_range(&self) -> Result<TimeRange> {
        match self.ts {
            Column {
                data: ColumnData::I64(_, min, max),
                ..
            } => Ok(TimeRange {
                min_ts: min,
                max_ts: max,
            }),
            _ => Err(Error::DataBlockError {
                reason: "Time column does not support except i64 physical data type".to_string(),
            }),
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum State {
    Initialised,
    Started,
    Finished,
}

pub enum Version {
    V1,
}

pub struct WriteOptions {
    version: Version,
    write_statistics: bool,
    encode: Encoding,
}

impl Default for WriteOptions {
    fn default() -> Self {
        Self {
            version: Version::V1,
            write_statistics: true,
            encode: Encoding::Null,
        }
    }
}

const HEADER_LEN: u64 = 5;
const TSM_MAGIC: [u8; 4] = 0x12CDA16_u32.to_be_bytes();
const VERSION: [u8; 1] = [1];

pub struct Tsm2Writer {
    file_id: u64,
    min_ts: i64,
    max_ts: i64,
    size: usize,
    max_size: u64,
    path: PathBuf,

    series_bloom_filter: BloomFilter,
    // todo: table object id bloom filter
    // table_bloom_filter: BloomFilter,
    writer: Box<FileStreamWriter>,
    options: WriteOptions,
    table_schemas: HashMap<String, TskvTableSchemaRef>,

    /// <table < series, Chunk>>
    page_specs: BTreeMap<String, BTreeMap<SeriesId, Chunk>>,
    /// <table, ChunkGroup>
    chunk_specs: BTreeMap<String, ChunkGroup>,
    /// [ChunkGroupWriteSpec]
    chunk_group_specs: ChunkGroupMeta,
    footer: Footer,
    state: State,
}

//MutableRecordBatch
impl Tsm2Writer {
    pub async fn open(
        path_buf: &impl AsRef<Path>,
        file_id: u64,
        max_size: u64,
        is_delta: bool,
    ) -> Result<Self> {
        let file_path = if is_delta {
            make_delta_file(path_buf, file_id)
        } else {
            make_tsm_file(path_buf, file_id)
        };
        let file_system = LocalFileSystem::new(LocalFileType::ThreadPool);
        let file = file_system
            .open_file_writer(&file_path)
            .await
            .map_err(|e| Error::FileSystemError { source: e })?;
        let writer = Self::new(file_path, file, file_id, max_size);
        Ok(writer)
    }
    fn new(path: PathBuf, writer: Box<FileStreamWriter>, file_id: u64, max_size: u64) -> Self {
        Self {
            file_id,
            max_ts: i64::MIN,
            min_ts: i64::MAX,
            size: 0,
            max_size,
            path,
            series_bloom_filter: BloomFilter::new(BLOOM_FILTER_BITS),
            writer,
            options: Default::default(),
            table_schemas: Default::default(),
            page_specs: Default::default(),
            chunk_specs: Default::default(),
            chunk_group_specs: Default::default(),
            footer: Default::default(),
            state: State::Initialised,
        }
    }

    pub fn file_id(&self) -> u64 {
        self.file_id
    }

    pub fn min_ts(&self) -> i64 {
        self.min_ts
    }

    pub fn max_ts(&self) -> i64 {
        self.max_ts
    }

    pub fn size(&self) -> usize {
        self.size
    }

    pub fn path(&self) -> &Path {
        self.path.as_path()
    }

    pub fn series_bloom_filter(&self) -> &BloomFilter {
        &self.series_bloom_filter
    }

    pub fn is_finished(&self) -> bool {
        self.state == State::Finished
    }

    pub async fn write_header(&mut self) -> Result<usize> {
        let size = self
            .writer
            .write_vec(
                [
                    IoSlice::new(TSM_MAGIC.as_slice()),
                    IoSlice::new(VERSION.as_slice()),
                ]
                .as_mut_slice(),
            )
            .await
            .context(IOSnafu)?;
        self.state = State::Started;
        self.size += size;
        Ok(size)
    }

    /// todo: write footer
    pub async fn write_footer(&mut self) -> Result<usize> {
        let buf = self.footer.serialize()?;
        let size = self.writer.write(&buf).await.context(IOSnafu)?;
        self.size += size;
        Ok(size)
    }

    pub async fn write_chunk_group(&mut self) -> Result<()> {
        for (table, group) in &self.chunk_specs {
            let chunk_group_offset = self.writer.len() as u64;
            let buf = group.serialize()?;
            let chunk_group_size = self.writer.write(&buf).await?;
            self.size += chunk_group_size;
            let chunk_group_spec = ChunkGroupWriteSpec {
                table_schema: self.table_schemas.get(table).unwrap().clone(),
                chunk_group_offset,
                chunk_group_size,
                time_range: group.time_range(),
                // The number of chunks in the group.
                count: 0,
            };
            self.chunk_group_specs.push(chunk_group_spec);
        }
        Ok(())
    }

    pub async fn write_chunk_group_specs(&mut self, series: SeriesMeta) -> Result<()> {
        let chunk_group_specs_offset = self.writer.len() as u64;
        let buf = self.chunk_group_specs.serialize()?;
        let chunk_group_specs_size = self.writer.write(&buf).await?;
        self.size += chunk_group_specs_size;
        let time_range = self.chunk_group_specs.time_range();
        let footer = Footer {
            version: 2_u8,
            time_range,
            table: TableMeta::new(chunk_group_specs_offset, chunk_group_specs_size),
            series,
        };
        self.footer = footer;
        Ok(())
    }

    pub async fn write_chunk(&mut self) -> Result<SeriesMeta> {
        let chunk_offset = self.writer.len() as u64;
        for (table, group) in &self.page_specs {
            for (series, chunk) in group {
                let chunk_offset = self.writer.len() as u64;
                let buf = chunk.serialize()?;
                let chunk_size = self.writer.write(&buf).await?;
                self.size += chunk_size;
                let time_range = chunk.time_range();
                self.min_ts = min(self.min_ts, time_range.min_ts);
                self.max_ts = max(self.max_ts, time_range.max_ts);
                let chunk_spec = ChunkWriteSpec {
                    series_id: *series,
                    chunk_offset,
                    chunk_size,
                    statics: ChunkStatics {
                        time_range: *time_range,
                    },
                };
                self.chunk_specs
                    .entry(table.clone())
                    .or_default()
                    .push(chunk_spec);
                self.series_bloom_filter.insert(&series.to_be_bytes());
            }
        }
        let chunk_size = self.writer.len() as u64 - chunk_offset;
        let series = SeriesMeta::new(
            self.series_bloom_filter.bytes().to_vec(),
            chunk_offset,
            chunk_size,
        );
        Ok(series)
    }
    pub async fn write_data(&mut self, groups: TsmWriteData) -> Result<()> {
        // write page data
        for (_, group) in groups {
            for (series, (series_buf, datablock)) in group {
                self.write_datablock(series, series_buf, datablock).await?;
            }
        }
        Ok(())
    }

    fn create_column_group(
        &mut self,
        schema: TskvTableSchemaRef,
        series_id: SeriesId,
        series_key: &SeriesKey,
    ) -> ColumnGroup {
        let chunks = self.page_specs.entry(schema.name.clone()).or_default();
        let chunk = chunks.entry(series_id).or_insert(Chunk::new(
            schema.name.clone(),
            series_id,
            series_key.clone(),
        ));

        ColumnGroup::new(chunk.next_column_group_id())
    }

    pub async fn write_datablock(
        &mut self,
        series_id: SeriesId,
        series_key: SeriesKey,
        datablock: DataBlock2,
    ) -> Result<()> {
        if self.state == State::Finished {
            return Err(Error::CommonError {
                reason: "Tsm2Writer has been finished".to_string(),
            });
        }

        let time_range = datablock.time_range()?;
        let schema = datablock.schema.clone();
        let pages = datablock.block_to_page()?;

        self.write_pages(schema, series_id, series_key, pages, time_range)
            .await?;
        Ok(())
    }

    pub async fn write_pages(
        &mut self,
        schema: TskvTableSchemaRef,
        series_id: SeriesId,
        series_key: SeriesKey,
        pages: Vec<Page>,
        time_range: TimeRange,
    ) -> Result<()> {
        if self.state == State::Initialised {
            self.write_header().await?;
        }

        let mut column_group = self.create_column_group(schema.clone(), series_id, &series_key);

        let table = schema.name.clone();
        for page in pages {
            let offset = self.writer.len() as u64;
            let size = self.writer.write(&page.bytes).await?;
            self.size += size;
            let spec = PageWriteSpec {
                offset,
                size,
                meta: page.meta,
            };
            column_group.push(spec);
            self.table_schemas.insert(table.clone(), schema.clone());
        }
        column_group.time_range_merge(&time_range);
        self.page_specs
            .entry(table.clone())
            .or_default()
            .entry(series_id)
            .or_insert(Chunk::new(schema.name.clone(), series_id, series_key))
            .push(column_group.into())?;
        Ok(())
    }

    pub async fn write_raw(
        &mut self,
        schema: TskvTableSchemaRef,
        meta: Arc<Chunk>,
        column_group_id: usize,
        raw: Vec<u8>,
    ) -> Result<()> {
        if self.state == State::Initialised {
            self.write_header().await?;
        }

        let mut new_column_group =
            self.create_column_group(schema.clone(), meta.series_id(), meta.series_key());

        let mut offset = self.writer.len() as u64;
        let size = self.writer.write(&raw).await?;
        self.size += size;

        let table = schema.name.to_string();
        let column_group = meta
            .column_group()
            .get(&column_group_id)
            .ok_or(Error::CommonError {
                reason: format!("column group not found: {}", column_group_id),
            })?;
        for spec in column_group.pages() {
            let spec = PageWriteSpec {
                offset,
                size: spec.size,
                meta: spec.meta.clone(),
            };
            offset += spec.size as u64;
            new_column_group.push(spec);
            self.table_schemas.insert(table.clone(), schema.clone());
        }
        new_column_group.time_range_merge(column_group.time_range());
        let series_id = meta.series_id();
        let series_key = meta.series_key().clone();
        self.page_specs
            .entry(table.clone())
            .or_default()
            .entry(series_id)
            .or_insert(Chunk::new(table, series_id, series_key))
            .push(new_column_group.into())?;

        Ok(())
    }

    pub async fn write_compacting_block(
        &mut self,
        compacting_block: CompactingBlock,
    ) -> Result<()> {
        match compacting_block {
            CompactingBlock::Decoded {
                data_block,
                series_id,
                series_key,
                ..
            } => {
                self.write_datablock(series_id, series_key, data_block)
                    .await?
            }
            CompactingBlock::Encoded {
                table_schema,
                series_id,
                series_key,
                time_range,
                data_block,
                ..
            } => {
                self.write_pages(table_schema, series_id, series_key, data_block, time_range)
                    .await?
            }
            CompactingBlock::Raw {
                table_schema,
                meta,
                column_group_id,
                raw,
                ..
            } => {
                self.write_raw(table_schema, meta, column_group_id, raw)
                    .await?
            }
        }

        if self.max_size != 0 && self.size > self.max_size as usize {
            self.finish().await?;
        }

        Ok(())
    }

    pub async fn finish(&mut self) -> Result<()> {
        let series_meta = self.write_chunk().await?;
        self.write_chunk_group().await?;
        self.write_chunk_group_specs(series_meta).await?;
        self.write_footer().await?;
        self.state = State::Finished;
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use std::path::PathBuf;
    use std::sync::Arc;

    use arrow::datatypes::TimeUnit;
    use models::codec::Encoding;
    use models::field_value::FieldVal;
    use models::predicate::domain::TimeRange;
    use models::schema::{ColumnType, TableColumn, TskvTableSchema};
    use models::{SeriesKey, ValueType};

    use crate::tsm2::reader::TSM2Reader;
    use crate::tsm2::writer::{Column, DataBlock2, Tsm2Writer};

    fn i64_column(data: Vec<i64>) -> Column {
        let mut col = Column::empty(ColumnType::Field(ValueType::Integer)).unwrap();
        for datum in data {
            col.push(Some(FieldVal::Integer(datum)))
        }
        col
    }

    fn ts_column(data: Vec<i64>) -> Column {
        let mut col = Column::empty(ColumnType::Time(TimeUnit::Nanosecond)).unwrap();
        for datum in data {
            col.push(Some(FieldVal::Integer(datum)))
        }
        col
    }

    #[tokio::test]
    async fn test_write_and_read() {
        let schema = TskvTableSchema::new(
            "cnosdb".to_string(),
            "public".to_string(),
            "test0".to_string(),
            vec![
                TableColumn::new(
                    0,
                    "time".to_string(),
                    ColumnType::Time(TimeUnit::Nanosecond),
                    Encoding::default(),
                ),
                TableColumn::new(
                    1,
                    "f1".to_string(),
                    ColumnType::Field(ValueType::Integer),
                    Encoding::default(),
                ),
                TableColumn::new(
                    2,
                    "f2".to_string(),
                    ColumnType::Field(ValueType::Integer),
                    Encoding::default(),
                ),
                TableColumn::new(
                    3,
                    "f3".to_string(),
                    ColumnType::Field(ValueType::Integer),
                    Encoding::default(),
                ),
            ],
        );
        let schema = Arc::new(schema);
        let data1 = DataBlock2::new(
            schema.clone(),
            ts_column(vec![1, 2, 3]),
            schema.time_column(),
            vec![
                i64_column(vec![1, 2, 3]),
                i64_column(vec![1, 2, 3]),
                i64_column(vec![1, 2, 3]),
            ],
            vec![
                schema.column("f1").cloned().unwrap(),
                schema.column("f2").cloned().unwrap(),
                schema.column("f3").cloned().unwrap(),
            ],
        );

        let path = "/tmp/test/tsm2";
        let mut tsm_writer = Tsm2Writer::open(&PathBuf::from(path), 1, 0, false)
            .await
            .unwrap();
        tsm_writer
            .write_datablock(1, SeriesKey::default(), data1.clone())
            .await
            .unwrap();
        tsm_writer.finish().await.unwrap();
        let tsm_reader = TSM2Reader::open(tsm_writer.path).await.unwrap();
        let data2 = tsm_reader.read_datablock(1, 0).await.unwrap();
        assert_eq!(data1, data2);
        let time_range = data2.time_range().unwrap();
        assert_eq!(time_range, TimeRange::new(1, 3));
        println!("time range: {:?}", time_range);
    }
}
