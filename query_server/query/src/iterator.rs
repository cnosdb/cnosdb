use std::collections::HashMap;

use minivec::MiniVec;
use std::sync::Arc;
use tokio::time::Instant;

use datafusion::arrow::array::{ArrayBuilder, TimestampNanosecondBuilder};
use models::utils::{min_num, unite_id};
use models::ValueType;
use snafu::ResultExt;
use trace::debug;

use crate::stream::TskvSourceMetrics;

use tskv::{
    engine::EngineRef,
    memcache::DataType,
    tseries_family::{ColumnFile, SuperVersion, TimeRange},
    tsm::{BlockMetaIterator, DataBlock, TsmReader},
    Error, {error, ColumnFileId},
};

use datafusion::arrow::{
    array::{BooleanBuilder, Float64Builder, Int64Builder, StringBuilder, UInt64Builder},
    datatypes::SchemaRef,
    record_batch::RecordBatch,
};
use models::schema::{ColumnType, TableSchema, TIME_FIELD};

pub type CursorPtr = Box<dyn Cursor>;
pub type ArrayBuilderPtr = Box<dyn ArrayBuilder>;

// 1. Tsm文件遍历： KeyCursor
//  功能：根据输入参数遍历Tsm文件
//  输入参数： SeriesKey、FieldName、StartTime、EndTime、Ascending
//  功能函数：调用Peek()—>(value, timestamp)得到一个值；调用Next()方法游标移到下一个值。
// 2. Field遍历： FiledCursor
//  功能：一个Field特定SeriesKey的遍历
//  输入输出参数同KeyCursor，区别是需要读取缓存数据，并按照特定顺序返回
// 3. Fields->行  转换器
//  一行数据是由同一个时间点的多个Field得到。借助上面的FieldCursor按照时间点对齐多个Field-Value拼接成一行数据。其过程类似于多路归并排序。
// 4. Iterator接口抽象层
//  调用Next接口返回一行数据，并且屏蔽查询是本机节点数据还是其他节点数据
// 5. 行数据到DataFusion的RecordBatch转换器
//  调用Iterator.Next得到行数据，然后转换行数据为RecordBatch结构

pub struct QueryOption {
    pub time_range: TimeRange,
    pub table_schema: TableSchema,
    pub datafusion_schema: SchemaRef,
}

pub struct FieldFileLocation {
    reader: TsmReader,
    block_it: BlockMetaIterator,

    read_index: usize,
    data_block: DataBlock,
}

impl FieldFileLocation {
    pub fn new(reader: TsmReader, block_it: BlockMetaIterator, vtype: ValueType) -> Self {
        Self {
            reader,
            block_it,
            read_index: 0,
            data_block: DataBlock::new(0, vtype),
        }
    }

    pub fn peek(&mut self) -> Result<Option<DataType>, Error> {
        if self.read_index >= self.data_block.len() {
            if let Some(meta) = self.block_it.next() {
                self.read_index = 0;
                self.data_block = self.reader.get_data_block(&meta)?;
            } else {
                return Ok(None);
            }
        }

        Ok(self.data_block.get(self.read_index))
    }

    pub fn next(&mut self) {
        self.read_index += 1;
    }
}

//-----------trait Cursor----------------
pub trait Cursor: Send + Sync {
    fn name(&self) -> &String;
    fn is_field(&self) -> bool;
    fn val_type(&self) -> ValueType;

    fn next(&mut self, ts: i64);
    fn peek(&mut self) -> Result<Option<DataType>, Error>;
}

//-----------Time Cursor----------------
pub struct TimeCursor {
    ts: i64,
    name: String,
}

impl TimeCursor {
    pub fn new(ts: i64, name: String) -> Self {
        Self { ts, name }
    }
}

impl Cursor for TimeCursor {
    fn name(&self) -> &String {
        &self.name
    }

    fn peek(&mut self) -> Result<Option<DataType>, Error> {
        let data = DataType::I64(self.ts, self.ts);

        Ok(Some(data))
    }

    fn next(&mut self, _ts: i64) {}

    fn val_type(&self) -> ValueType {
        ValueType::Integer
    }

    fn is_field(&self) -> bool {
        false
    }
}
//-----------Tag Cursor----------------
pub struct TagCursor {
    name: String,
    value: String,
}

impl TagCursor {
    pub fn new(value: String, name: String) -> Self {
        Self { name, value }
    }
}

impl Cursor for TagCursor {
    fn name(&self) -> &String {
        &self.name
    }

    fn peek(&mut self) -> Result<Option<DataType>, Error> {
        let data = DataType::Str(0, MiniVec::from(self.value.as_bytes()));

        Ok(Some(data))
    }

    fn next(&mut self, _ts: i64) {}

    fn val_type(&self) -> ValueType {
        ValueType::String
    }

    fn is_field(&self) -> bool {
        false
    }
}

//-----------Field Cursor----------------
pub struct FieldCursor {
    name: String,
    value_type: ValueType,

    cache_index: usize,
    cache_block: DataBlock,

    locations: Vec<FieldFileLocation>,
}

impl FieldCursor {
    pub fn new(
        field_id: u64,
        name: String,
        vtype: ValueType,
        iterator: &mut RowIterator,
    ) -> Result<Self, Error> {
        let version = iterator.version.clone();
        let time_range = iterator.option.time_range;

        // get data from im_memcache and memcache
        let mut blocks = vec![];
        for mem_cache in version.caches.immut_cache.iter() {
            if mem_cache.read().flushed {
                continue;
            }
            if let Some(mem_entry) = mem_cache.read().get(&field_id) {
                blocks.append(&mut mem_entry.read().read_cell(&time_range));
            }
        }

        if let Some(mem_entry) = version.caches.mut_cache.read().get(&field_id) {
            blocks.append(&mut mem_entry.read().read_cell(&time_range));
        }

        let cache_block = match DataBlock::merge_blocks(blocks, 0).pop() {
            Some(v) => v,
            None => DataBlock::new(0, vtype),
        };
        debug!(
            "build memcache data block id: {:02X}, len: {}",
            field_id,
            cache_block.len()
        );

        // get data from levelinfo
        let mut locations = vec![];
        for level in version.version.levels_info.iter().rev() {
            for file in level.files.iter() {
                if file.is_deleted() || !file.overlap(&time_range) {
                    continue;
                }

                debug!(
                    "build file data block id: {:02X}, len: {}",
                    field_id,
                    file.file_path().display()
                );

                let tsm_reader = iterator.get_tsm_reader(file.clone())?;
                for idx in tsm_reader.index_iterator_opt(field_id) {
                    let block_it = idx.block_iterator_opt(&time_range);
                    let location = FieldFileLocation::new(tsm_reader.clone(), block_it, vtype);
                    locations.push(location);
                }
            }
        }

        Ok(Self {
            name,
            value_type: vtype,
            cache_index: 0,
            cache_block,
            locations,
        })
    }
}

impl Cursor for FieldCursor {
    fn name(&self) -> &String {
        &self.name
    }

    fn peek(&mut self) -> Result<Option<DataType>, Error> {
        let mut data = DataType::new(self.value_type, i64::MAX);
        for loc in self.locations.iter_mut() {
            if let Some(val) = loc.peek()? {
                if data.timestamp() >= val.timestamp() {
                    data = val;
                }
            }
        }

        if let Some(val) = self.cache_block.get(self.cache_index) {
            if data.timestamp() >= val.timestamp() {
                data = val;
            }
        }

        if data.timestamp() == i64::MAX {
            return Ok(None);
        }
        Ok(Some(data))
    }

    fn next(&mut self, ts: i64) {
        if let Some(val) = self.cache_block.get(self.cache_index) {
            if val.timestamp() == ts {
                self.cache_index += 1;
            }
        }

        for loc in self.locations.iter_mut() {
            if let Some(val) = loc.peek().unwrap() {
                if val.timestamp() == ts {
                    loc.next();
                }
            }
        }
    }

    fn val_type(&self) -> ValueType {
        self.value_type
    }

    fn is_field(&self) -> bool {
        true
    }
}

pub struct RowIterator {
    batch_size: usize,
    series_index: usize,
    series: Vec<u64>,
    engine: EngineRef,
    option: QueryOption,
    columns: Vec<CursorPtr>,
    version: Arc<SuperVersion>,

    open_files: HashMap<ColumnFileId, TsmReader>,

    metrics: TskvSourceMetrics,
}

impl RowIterator {
    pub fn new(
        metrics: TskvSourceMetrics,
        engine: EngineRef,
        option: QueryOption,
        batch_size: usize,
    ) -> Result<Self, Error> {
        let version =
            engine
                .get_db_version(&option.table_schema.db)
                .ok_or(Error::DatabaseNotFound {
                    database: option.table_schema.db.clone(),
                })?;

        let series = engine
            .get_series_id_list(&option.table_schema.db, &option.table_schema.name, &[])
            .context(error::IndexErrSnafu)?;

        Ok(Self {
            series,
            engine,
            option,
            version,
            batch_size,

            columns: vec![],
            series_index: usize::MAX,
            open_files: HashMap::new(),

            metrics,
        })
    }

    pub fn get_tsm_reader(&mut self, file: Arc<ColumnFile>) -> Result<TsmReader, Error> {
        if let Some(val) = self.open_files.get(&file.file_id()) {
            return Ok(val.clone());
        }

        let tsm_reader = TsmReader::open(file.file_path())?;
        self.open_files.insert(file.file_id(), tsm_reader.clone());

        Ok(tsm_reader)
    }

    fn build_series_columns(&mut self, id: u64) -> Result<(), Error> {
        let start = Instant::now();

        if let Some(key) = self
            .engine
            .get_series_key(&self.option.table_schema.db, id)
            .context(error::IndexErrSnafu)?
        {
            self.columns.clear();
            let fields = self.option.table_schema.fields.clone();
            for (_, item) in fields {
                let field_name = item.name.clone();
                debug!("build series columns id:{:02X}, {:?}", id, item);
                let column: CursorPtr = match item.column_type {
                    ColumnType::Time => Box::new(TimeCursor::new(0, field_name)),

                    ColumnType::Tag => Box::new(TagCursor::new(
                        String::from_utf8(key.tag_val(&item.name)).unwrap(),
                        field_name,
                    )),

                    ColumnType::Field(vtype) => match vtype {
                        ValueType::Unknown => todo!(),

                        ValueType::Float
                        | ValueType::Integer
                        | ValueType::Unsigned
                        | ValueType::Boolean
                        | ValueType::String => {
                            let cursor =
                                FieldCursor::new(unite_id(item.id, id), field_name, vtype, self)?;
                            Box::new(cursor)
                        }
                    },
                };

                self.columns.push(column);
            }
        }

        self.metrics
            .elapsed_series_scan()
            .add_duration(Instant::now() - start);

        Ok(())
    }

    fn next_series(&mut self) -> Result<Option<()>, Error> {
        if self.series_index == usize::MAX {
            self.series_index = 0;
        } else {
            self.series_index += 1;
        }

        if self.series_index >= self.series.len() {
            return Ok(None);
        }

        self.build_series_columns(self.series[self.series_index])?;

        Ok(Some(()))
    }

    fn collect_row_data(&mut self, builder: &mut [ArrayBuilderPtr]) -> Result<Option<()>, Error> {
        debug!("======collect_row_data=========");
        let timer = self.metrics.elapsed_field_scan().timer();

        let mut min_time = i64::MAX;
        let mut values = Vec::with_capacity(self.columns.len());
        for column in self.columns.iter_mut() {
            match column.peek() {
                Ok(val) => match val {
                    Some(ref data) => {
                        if column.is_field() {
                            min_time = min_num(min_time, data.timestamp());
                        }

                        values.push(val);
                    }
                    None => values.push(None),
                },

                Err(err) => return Err(err),
            }
        }

        for (column, value) in self.columns.iter_mut().zip(values.iter_mut()) {
            debug!("field: {} value {:?}", column.name(), value);
            if !column.is_field() {
                continue;
            }

            if let Some(data) = value {
                let ts = data.timestamp();
                if ts == min_time {
                    column.next(ts);
                } else {
                    *value = None;
                }
            }
        }

        timer.done();

        debug!("min time {}", min_time);
        if min_time == i64::MAX {
            self.columns.clear();
            return Ok(None);
        }

        let timer = self.metrics.elapsed_point_to_record_batch().timer();

        for i in 0..values.len() {
            if self.columns[i].name() == TIME_FIELD {
                let field_builder = builder[i]
                    .as_any_mut()
                    .downcast_mut::<TimestampNanosecondBuilder>()
                    .unwrap();
                field_builder.append_value(min_time);

                continue;
            }

            match self.columns[i].val_type() {
                ValueType::Unknown => todo!(),
                ValueType::Float => {
                    let field_builder = builder[i]
                        .as_any_mut()
                        .downcast_mut::<Float64Builder>()
                        .unwrap();
                    if let Some(DataType::F64(_, val)) = &values[i] {
                        debug!("append float value");
                        field_builder.append_value(*val);
                    } else {
                        debug!("append float null");
                        field_builder.append_null();
                    }
                }
                ValueType::Integer => {
                    let field_builder = builder[i]
                        .as_any_mut()
                        .downcast_mut::<Int64Builder>()
                        .unwrap();
                    if let Some(DataType::I64(_, val)) = &values[i] {
                        field_builder.append_value(*val);
                    } else {
                        field_builder.append_null();
                    }
                }
                ValueType::Unsigned => {
                    let field_builder = builder[i]
                        .as_any_mut()
                        .downcast_mut::<UInt64Builder>()
                        .unwrap();
                    if let Some(DataType::U64(_, val)) = &values[i] {
                        field_builder.append_value(*val);
                    } else {
                        field_builder.append_null();
                    }
                }
                ValueType::Boolean => {
                    let field_builder = builder[i]
                        .as_any_mut()
                        .downcast_mut::<BooleanBuilder>()
                        .unwrap();
                    if let Some(DataType::Bool(_, val)) = &values[i] {
                        field_builder.append_value(*val);
                    } else {
                        field_builder.append_null();
                    }
                }
                ValueType::String => {
                    let field_builder = builder[i]
                        .as_any_mut()
                        .downcast_mut::<StringBuilder>()
                        .unwrap();
                    if let Some(DataType::Str(_, val)) = &values[i] {
                        debug!("append string value");
                        field_builder.append_value(String::from_utf8(val.to_vec()).unwrap());
                    } else {
                        debug!("append string null");
                        field_builder.append_null();
                    }
                }
            }
        }

        timer.done();

        Ok(Some(()))
    }

    fn next_row(&mut self, builder: &mut [ArrayBuilderPtr]) -> Result<Option<()>, Error> {
        loop {
            if self.columns.is_empty() {
                match self.next_series() {
                    Ok(val) => match val {
                        Some(_) => {}
                        None => return Ok(None),
                    },
                    Err(err) => return Err(err),
                }
            }

            if self.collect_row_data(builder)?.is_some() {
                return Ok(Some(()));
            }
        }
    }

    fn record_builder(&self) -> Vec<ArrayBuilderPtr> {
        let mut builders: Vec<ArrayBuilderPtr> =
            Vec::with_capacity(self.option.table_schema.fields.len());
        for (_, item) in self.option.table_schema.fields.iter() {
            debug!("schema info {:02X} {}", item.id, item.name);

            match item.column_type {
                ColumnType::Tag => builders.push(Box::new(StringBuilder::new(self.batch_size))),
                ColumnType::Time => {
                    builders.push(Box::new(TimestampNanosecondBuilder::new(self.batch_size)))
                }
                ColumnType::Field(t) => match t {
                    ValueType::Unknown => todo!(),
                    ValueType::Float => {
                        builders.push(Box::new(Float64Builder::new(self.batch_size)))
                    }
                    ValueType::Integer => {
                        builders.push(Box::new(Int64Builder::new(self.batch_size)))
                    }
                    ValueType::Unsigned => {
                        builders.push(Box::new(UInt64Builder::new(self.batch_size)))
                    }
                    ValueType::Boolean => {
                        builders.push(Box::new(BooleanBuilder::new(self.batch_size)))
                    }
                    ValueType::String => {
                        builders.push(Box::new(StringBuilder::new(self.batch_size)))
                    }
                },
            }
        }

        builders
    }

    fn is_finish(&self) -> bool {
        if self.series_index == usize::MAX {
            return false;
        }

        self.series_index >= self.series.len()
    }
}

impl Iterator for RowIterator {
    type Item = Result<RecordBatch, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.is_finish() {
            return None;
        }

        let timer = self.metrics.elapsed_point_to_record_batch().timer();
        let mut builder = self.record_builder();
        timer.done();

        for _ in 0..self.batch_size {
            debug!("========next_row");
            match self.next_row(&mut builder) {
                Ok(val) => match val {
                    Some(_) => {}
                    None => break,
                },

                Err(err) => return Some(Err(err)),
            }
        }

        let timer = self.metrics.elapsed_point_to_record_batch().timer();
        let result = {
            let mut cols = vec![];
            for item in builder.iter_mut() {
                cols.push(item.finish())
            }

            match RecordBatch::try_new(self.option.datafusion_schema.clone(), cols) {
                Ok(batch) => Some(Ok(batch)),
                Err(err) => Some(Err(Error::DataFusionNew {
                    reason: err.to_string(),
                })),
            }
        };
        timer.done();

        result
    }
}
