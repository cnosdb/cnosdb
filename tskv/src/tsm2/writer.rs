use std::cmp::{max, min};
use std::collections::{BTreeMap, HashMap};
use std::io::IoSlice;
use std::path::PathBuf;
use std::sync::Arc;

use minivec::MiniVec;
use models::codec::Encoding;
use models::field_value::FieldVal;
use models::predicate::domain::TimeRange;
use models::schema::{ColumnType, TableColumn, TskvTableSchemaRef};
use models::{PhysicalDType, SeriesId, ValueType};
use snafu::ResultExt;
use utils::bitset::BitSet;
use utils::BloomFilter;

use crate::byte_utils::decode_be_i64;
use crate::compaction::CompactingBlock;
use crate::error::IOSnafu;
use crate::file_system::file::cursor::FileCursor;
use crate::file_system::file_manager;
use crate::file_utils::make_tsm_file_name;
use crate::tsm::codec::{
    get_bool_codec, get_f64_codec, get_i64_codec, get_str_codec, get_u64_codec,
};
use crate::tsm2::page::{
    Chunk, ChunkGroup, ChunkGroupMeta, ChunkGroupWriteSpec, ChunkStatics, ChunkWriteSpec, Footer,
    Page, PageMeta, PageStatistics, PageWriteSpec, SeriesMeta, TableMeta,
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
#[derive(Debug, Clone)]
pub struct Column {
    pub column_type: ColumnType,
    pub valid: BitSet,
    pub data: ColumnData,
}

impl Column {
    pub fn empty(column_type: ColumnType) -> Column {
        let valid = BitSet::new();
        Self {
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
                    ValueType::String => ColumnData::String(vec![], String::new(), String::new()),
                    _ => {
                        panic!("invalid type: field type does not match filed type")
                    }
                },
            },
        }
    }

    pub fn new(row_count: usize, column_type: ColumnType) -> Column {
        let mut valid = BitSet::new();
        valid.append_unset(row_count);

        let data = match column_type {
            ColumnType::Tag => {
                ColumnData::String(vec![String::new(); row_count], String::new(), String::new())
            }
            ColumnType::Time(_) => ColumnData::I64(vec![0; row_count], i64::MAX, i64::MIN),
            ColumnType::Field(field_type) => match field_type {
                ValueType::Float => ColumnData::F64(vec![0.0; row_count], f64::MAX, f64::MIN),
                ValueType::Integer => ColumnData::I64(vec![0; row_count], i64::MAX, i64::MIN),
                ValueType::Unsigned => ColumnData::U64(vec![0; row_count], u64::MAX, u64::MIN),
                ValueType::Boolean => ColumnData::Bool(vec![false; row_count], false, true),
                ValueType::String => {
                    ColumnData::String(vec![String::new(); row_count], String::new(), String::new())
                }
                _ => {
                    panic!("invalid type: field type does not match filed type")
                }
            },
        };
        Self {
            column_type,
            valid,
            data,
        }
    }
    pub fn push(&mut self, value: Option<FieldVal>) {
        match (&mut self.data, value) {
            (ColumnData::F64(ref mut value, min, max), Some(FieldVal::Float(val))) => {
                value.push(val);
                if *max < val {
                    *max = val;
                }
                if *min > val {
                    *min = val;
                }
                self.valid.append_set(1);
            }
            (ColumnData::F64(ref mut value, ..), None) => {
                value.push(0.0);
                self.valid.append_unset(1);
            }
            (ColumnData::I64(ref mut value, min, max), Some(FieldVal::Integer(val))) => {
                if *max < val {
                    *max = val;
                }
                if *min > val {
                    *min = val;
                }
                value.push(val);
                self.valid.append_set(1);
            }
            (ColumnData::I64(ref mut value, ..), None) => {
                value.push(0);
                self.valid.append_unset(1);
            }
            (ColumnData::U64(ref mut value, min, max), Some(FieldVal::Unsigned(val))) => {
                if *max < val {
                    *max = val;
                }
                if *min > val {
                    *min = val;
                }
                value.push(val);
                self.valid.append_set(1);
            }
            (ColumnData::U64(ref mut value, ..), None) => {
                value.push(0);
                self.valid.append_unset(1);
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
                self.valid.append_set(1);
            }
            (ColumnData::String(ref mut value, ..), None) => {
                value.push(String::new());
                self.valid.append_unset(1);
            }
            (ColumnData::Bool(ref mut value, min, max), Some(FieldVal::Boolean(val))) => {
                if !(*max) & val {
                    *max = val;
                }
                if *min & !val {
                    *min = val;
                }
                value.push(val);
                self.valid.append_set(1);
            }
            (ColumnData::Bool(ref mut value, ..), None) => {
                value.push(false);
                self.valid.append_unset(1);
            }
            _ => {
                panic!("Column type mismatch")
            }
        }
    }
    pub fn col_to_page(&self, desc: &TableColumn, time_range: Option<TimeRange>) -> Page {
        let len_bitset = self.valid.byte_len() as u32;
        let data_len = self.valid.len() as u64;
        let mut buf = vec![];
        let (min, max, value_type) = match &self.data {
            ColumnData::F64(array, min, max) => {
                let encoder = get_f64_codec(desc.encoding);
                encoder.encode(array, &mut buf).unwrap();
                (
                    Vec::from(min.to_le_bytes()),
                    Vec::from(max.to_le_bytes()),
                    PhysicalDType::Float,
                )
            }
            ColumnData::I64(array, min, max) => {
                let encoder = get_i64_codec(desc.encoding);
                encoder.encode(array, &mut buf).unwrap();
                (
                    Vec::from(min.to_le_bytes()),
                    Vec::from(max.to_le_bytes()),
                    PhysicalDType::Integer,
                )
            }
            ColumnData::U64(array, min, max) => {
                let encoder = get_u64_codec(desc.encoding);
                encoder.encode(array, &mut buf).unwrap();
                (
                    Vec::from(min.to_le_bytes()),
                    Vec::from(max.to_le_bytes()),
                    PhysicalDType::Unsigned,
                )
            }
            ColumnData::String(array, min, max) => {
                let encoder = get_str_codec(desc.encoding);
                encoder
                    .encode(
                        &array.iter().map(|it| it.as_bytes()).collect::<Vec<_>>(),
                        &mut buf,
                    )
                    .unwrap();
                (
                    min.as_bytes().to_vec(),
                    max.as_bytes().to_vec(),
                    PhysicalDType::String,
                )
            }
            ColumnData::Bool(array, min, max) => {
                let encoder = get_bool_codec(desc.encoding);
                encoder.encode(array, &mut buf).unwrap();
                (vec![*min as u8], vec![*max as u8], PhysicalDType::Boolean)
            }
        };
        let mut data = vec![];
        data.extend_from_slice(&len_bitset.to_be_bytes());
        data.extend_from_slice(&data_len.to_be_bytes());
        data.extend_from_slice(self.valid.bytes());
        data.extend_from_slice(&buf);
        let bytes = bytes::Bytes::from(data);
        let time_range = match time_range {
            None => TimeRange {
                max_ts: decode_be_i64(&max),
                min_ts: decode_be_i64(&min),
            },
            Some(time_range) => time_range,
        };
        let meta = PageMeta {
            num_values: self.valid.len() as u32,
            column: desc.clone(),
            time_range,
            statistic: PageStatistics {
                primitive_type: value_type,
                null_count: None,
                distinct_count: None,
                max_value: Some(max),
                min_value: Some(min),
            },
        };
        Page { bytes, meta }
    }

    pub fn get(&self, index: usize) -> Option<FieldVal> {
        if self.valid.get(index) {
            self.data.get(index)
        } else {
            None
        }
    }

    pub fn chunk(&self, start: usize, end: usize) -> Result<Column> {
        let mut column = Column::empty(self.column_type.clone());
        for index in start..end {
            column.push(self.get(index));
        }
        Ok(column)
    }
}

#[derive(Debug, Clone)]
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
}

#[derive(Debug, Clone)]
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

    //todo dont forgot build time column to pages
    pub fn block_to_page(&self) -> Vec<Page> {
        let mut pages = Vec::with_capacity(self.cols.len() + 1);
        let ts_page = self.ts.col_to_page(&self.ts_desc, None);
        let time_range = ts_page.meta.time_range;
        pages.push(self.ts.col_to_page(&self.ts_desc, None));
        for (col, desc) in self.cols.iter().zip(self.cols_desc.iter()) {
            pages.push(col.col_to_page(desc, Some(time_range)));
        }
        pages
    }

    pub fn merge(&mut self, other: DataBlock2) -> Result<DataBlock2> {
        self.schema_check(&other)?;

        let schema = if self.schema.schema_id > other.schema.schema_id {
            self.schema.clone()
        } else {
            other.schema.clone()
        };
        let (sort_index, time_array) = self.sort_index_and_time_col(&other);
        let mut columns = Vec::new();
        let mut columns_des = Vec::new();
        for field in schema.fields() {
            let mut merge_column = Column::empty(field.column_type.clone());
            let column_self = self.column(&field.name);
            let column_other = other.column(&field.name);
            for idx in sort_index.iter() {
                match idx {
                    Merge::SelfTs(index) => {
                        if let Some(column_self) = column_self {
                            merge_column.push(column_self.data.get(*index));
                        }
                    }
                    Merge::OtherTs(index) => {
                        if let Some(column_other) = column_other {
                            merge_column.push(column_other.data.get(*index));
                        }
                    }
                    Merge::Equal(index_self, index_other) => {
                        let field_self = if let Some(column_self) = column_self {
                            column_self.data.get(*index_self)
                        } else {
                            None
                        };
                        let field_other = if let Some(column_other) = column_other {
                            column_other.data.get(*index_other)
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

        let mut ts_col = Column::empty(self.ts_desc.column_type.clone());
        time_array
            .iter()
            .for_each(|it| ts_col.push(Some(FieldVal::Integer(*it))));
        let datablock = DataBlock2::new(schema, ts_col, self.ts_desc.clone(), columns, columns_des);

        // todo: split datablock to blocks
        // let mut blocks = Vec::with_capacity(time_array.len() / Self::BLOCK_SIZE + 1);
        // blocks.push(datablock);

        Ok(datablock)
    }

    fn sort_index_and_time_col(&self, other: &DataBlock2) -> (Vec<Merge>, Vec<i64>) {
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
                    unreachable!();
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
                unreachable!()
            }
        }
        (sort_index, time_array)
    }

    pub fn len(&self) -> usize {
        self.ts.valid.len()
    }

    pub fn schema_check(&self, other: &DataBlock2) -> Result<()> {
        if self.schema.name != other.schema.name
            || self.schema.db != other.schema.db
            || self.schema.tenant != other.schema.tenant
        {
            return Err(Error::CommonError {
                reason: "schema name not match".to_string(),
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

    pub fn time_range(&self) -> TimeRange {
        match self.ts {
            Column {
                data: ColumnData::I64(_, min, max),
                ..
            } => TimeRange {
                min_ts: min,
                max_ts: max,
            },
            _ => {
                unreachable!()
            }
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

    series_bloom_filter: BloomFilter,
    // todo: table object id bloom filter
    // table_bloom_filter: BloomFilter,
    writer: FileCursor,
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
    pub async fn open(path_buf: &PathBuf, file_id: u64, max_size: u64) -> Result<Self> {
        let tsm_path = make_tsm_file_name(path_buf, file_id);
        let file_cursor = file_manager::create_file(tsm_path).await?;
        let writer = Self::new(file_cursor.into(), file_id, max_size);
        Ok(writer)
    }
    pub fn new(writer: FileCursor, file_id: u64, max_size: u64) -> Self {
        Self {
            file_id,
            max_ts: i64::MIN,
            min_ts: i64::MAX,
            size: 0,
            max_size,
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
        let size = self
            .writer
            .write(&self.footer.serialize()?)
            .await
            .context(IOSnafu)?;
        self.size += size;
        Ok(size)
    }

    pub async fn write_chunk_group(&mut self) -> Result<()> {
        for (table, group) in &self.chunk_specs {
            let chunk_group_offset = self.writer.pos();
            let buf = group.serialize()?;
            let chunk_group_size = self.writer.write(&buf).await?;
            self.size += chunk_group_size;
            let chunk_group_spec = ChunkGroupWriteSpec {
                table_schema: self.table_schemas.get(table).unwrap().clone(),
                chunk_group_offset,
                chunk_group_size,
                time_range: group.time_range(),
                /// The number of chunks in the group.
                count: 0,
            };
            self.chunk_group_specs.push(chunk_group_spec);
        }
        Ok(())
    }

    pub async fn write_chunk_group_specs(&mut self, series: SeriesMeta) -> Result<()> {
        let chunk_group_specs_offset = self.writer.pos();
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
        let chunk_offset = self.writer.pos();
        for (table, group) in &self.page_specs {
            for (series, chunk) in group {
                let chunk_offset = self.writer.pos();
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
        let chunk_size = self.writer.pos() - chunk_offset;
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
            for (series, datablock) in group {
                self.write_datablock(series, datablock).await?;
            }
        }
        Ok(())
    }

    pub async fn write_datablock(
        &mut self,
        series_id: SeriesId,
        datablock: DataBlock2,
    ) -> Result<()> {
        if self.state == State::Initialised {
            self.write_header().await?;
        }

        if self.state == State::Finished {
            return Err(Error::CommonError {
                reason: "Tsm2Writer has been finished".to_string(),
            });
        }

        let schema = datablock.schema.clone();
        let pages = datablock.block_to_page();

        self.write_pages(schema, series_id, pages).await?;
        Ok(())
    }

    pub async fn write_pages(
        &mut self,
        schema: TskvTableSchemaRef,
        series_id: SeriesId,
        pages: Vec<Page>,
    ) -> Result<()> {
        let table = schema.name.clone();
        let mut time_range = TimeRange::none();
        for page in pages {
            let offset = self.writer.pos();
            let size = self.writer.write(&page.bytes).await?;
            self.size += size;
            time_range.merge(&page.meta.time_range);
            let spec = PageWriteSpec {
                offset,
                size,
                meta: page.meta,
            };
            self.page_specs
                .entry(table.clone())
                .or_default()
                .entry(series_id)
                .or_insert(Chunk::new(schema.name.clone(), series_id))
                .push(spec);
            self.table_schemas
                .entry(table.clone())
                .or_insert(schema.clone());
        }
        Ok(())
    }

    pub async fn write_raw(
        &mut self,
        schema: TskvTableSchemaRef,
        meta: Arc<Chunk>,
        raw: Vec<u8>,
    ) -> Result<()> {
        let mut offset = self.writer.pos();
        let size = self.writer.write(&raw).await?;
        self.size += size;

        let table = schema.name.to_string();
        let series_id = meta.series_id();
        for spec in meta.pages() {
            let spec = PageWriteSpec {
                offset,
                size: spec.size,
                meta: spec.meta.clone(),
            };
            offset += spec.size as u64;
            self.page_specs
                .entry(table.clone())
                .or_default()
                .entry(series_id)
                .or_insert(Chunk::new(table.clone(), series_id))
                .push(spec.clone());
            self.table_schemas
                .entry(table.clone())
                .or_insert(schema.clone());
        }
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
                ..
            } => self.write_datablock(series_id, data_block).await?,
            CompactingBlock::Encoded {
                table_schema,
                series_id,
                data_block,
                ..
            } => {
                self.write_pages(table_schema, series_id, data_block)
                    .await?
            }
            CompactingBlock::Raw {
                table_schema,
                meta,
                raw,
                ..
            } => self.write_raw(table_schema, meta, raw).await?,
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
