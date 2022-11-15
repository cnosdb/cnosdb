use std::collections::HashMap;
use std::ops::{Bound, RangeBounds};
use std::task::Poll;

use datafusion::arrow::error::ArrowError;
use datafusion::physical_plan::metrics::{
    self, BaselineMetrics, ExecutionPlanMetricsSet, MetricBuilder,
};

use datafusion::scalar::ScalarValue;
use minivec::MiniVec;
use std::sync::Arc;
use tokio::time::Instant;

use datafusion::arrow::array::{ArrayBuilder, TimestampNanosecondBuilder};
use datafusion::arrow::datatypes::DataType as ArrowDataType;
use models::utils::{min_num, unite_id};
use models::{FieldId, SeriesId, ValueType};
use snafu::ResultExt;
use trace::debug;

use super::{
    engine::EngineRef,
    error::IndexErrSnafu,
    memcache::DataType,
    tseries_family::{ColumnFile, SuperVersion, TimeRange},
    tsm::{BlockMetaIterator, DataBlock, TsmReader},
    ColumnFileId, Error,
};

use datafusion::arrow::{
    array::{BooleanBuilder, Float64Builder, Int64Builder, StringBuilder, UInt64Builder},
    datatypes::SchemaRef,
    record_batch::RecordBatch,
};

use models::predicate::domain::{ColumnDomains, Domain, Range, ValueEntry};
use models::schema::{ColumnType, TskvTableSchema, TIME_FIELD, TIME_FIELD_NAME};

pub type CursorPtr = Box<dyn Cursor>;
pub type ArrayBuilderPtr = Box<dyn ArrayBuilder>;

/// Stores metrics about the table writer execution.
#[derive(Debug)]
pub struct TableScanMetrics {
    baseline_metrics: BaselineMetrics,

    partition: usize,
    metrics: ExecutionPlanMetricsSet,
}

impl TableScanMetrics {
    /// Create new metrics
    pub fn new(metrics: &ExecutionPlanMetricsSet, partition: usize) -> Self {
        let baseline_metrics = BaselineMetrics::new(metrics, partition);
        Self {
            baseline_metrics,
            partition,
            metrics: metrics.clone(),
        }
    }

    pub fn tskv_metrics(&self) -> TskvSourceMetrics {
        TskvSourceMetrics::new(&self.metrics.clone(), self.partition)
    }

    /// return the metric for cpu time spend in this operator
    pub fn elapsed_compute(&self) -> &metrics::Time {
        self.baseline_metrics.elapsed_compute()
    }

    /// Process a poll result of a stream producing output for an
    /// operator, recording the output rows and stream done time and
    /// returning the same poll result
    pub fn record_poll(
        &self,
        poll: Poll<Option<std::result::Result<RecordBatch, ArrowError>>>,
    ) -> Poll<Option<std::result::Result<RecordBatch, ArrowError>>> {
        self.baseline_metrics.record_poll(poll)
    }

    /// Records the fact that this operator's execution is complete
    /// (recording the `end_time` metric).
    pub fn done(&self) {
        self.baseline_metrics.done()
    }
}

/// Stores metrics about the table writer execution.
#[derive(Debug)]
pub struct TskvSourceMetrics {
    elapsed_point_to_record_batch: metrics::Time,
    elapsed_field_scan: metrics::Time,
    elapsed_series_scan: metrics::Time,
}

impl TskvSourceMetrics {
    /// Create new metrics
    pub fn new(metrics: &ExecutionPlanMetricsSet, partition: usize) -> Self {
        let elapsed_point_to_record_batch =
            MetricBuilder::new(metrics).subset_time("elapsed_point_to_record_batch", partition);

        let elapsed_field_scan =
            MetricBuilder::new(metrics).subset_time("elapsed_field_scan", partition);

        let elapsed_series_scan =
            MetricBuilder::new(metrics).subset_time("elapsed_series_scan", partition);

        Self {
            elapsed_point_to_record_batch,
            elapsed_field_scan,
            elapsed_series_scan,
        }
    }

    pub fn elapsed_point_to_record_batch(&self) -> &metrics::Time {
        &self.elapsed_point_to_record_batch
    }

    pub fn elapsed_field_scan(&self) -> &metrics::Time {
        &self.elapsed_field_scan
    }

    pub fn elapsed_series_scan(&self) -> &metrics::Time {
        &self.elapsed_series_scan
    }
}

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
    pub table_schema: TskvTableSchema,
    pub datafusion_schema: SchemaRef,
    pub time_filter: ColumnDomains<String>,
    pub tags_filter: ColumnDomains<String>,
    pub fields_filter: ColumnDomains<String>,
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
    cache_data: Vec<DataType>,
    locations: Vec<FieldFileLocation>,
}

impl FieldCursor {
    pub fn empty(value_type: ValueType, name: String) -> Self {
        Self {
            name,
            value_type,
            cache_index: 0,
            cache_data: Vec::new(),
            locations: Vec::new(),
        }
    }

    pub fn new(
        field_id: FieldId,
        name: String,
        vtype: ValueType,
        iterator: &mut RowIterator,
    ) -> Result<Self, Error> {
        let version = match iterator.version.clone() {
            Some(v) => v,
            None => return Ok(Self::empty(vtype, name)),
        };

        let time_ranges: Vec<TimeRange> = filter_to_time_ranges(&iterator.option.time_filter);

        // get data from im_memcache and memcache
        let mut mem_data: Vec<DataType> = Vec::new();

        let time_predicate = |ts| {
            time_ranges
                .iter()
                .any(|time_range| time_range.is_boundless() || time_range.contains(ts))
        };

        version
            .caches
            .immut_cache
            .iter()
            .filter(|m| !m.read().flushed)
            .for_each(|m| {
                mem_data.append(&mut m.read().get_data(field_id, time_predicate, |_| true))
            });

        mem_data.append(&mut version.caches.mut_cache.read().get_data(
            field_id,
            time_predicate,
            |_| true,
        ));

        mem_data.sort_by_key(|data| data.timestamp());

        debug!(
            "build memcache data id: {:02X}, len: {}",
            field_id,
            mem_data.len()
        );

        // get data from levelinfo
        let mut locations = vec![];
        for level in version.version.levels_info.iter().rev() {
            for file in level.files.iter() {
                if file.is_deleted() {
                    continue;
                }

                for time_range in time_ranges.iter() {
                    if !file.overlap(time_range) {
                        continue;
                    }

                    debug!(
                        "build file data block id: {:02X}, len: {}",
                        field_id,
                        file.file_path().display()
                    );

                    let tsm_reader = iterator.get_tsm_reader(file.clone())?;
                    for idx in tsm_reader.index_iterator_opt(field_id) {
                        let block_it = idx.block_iterator_opt(time_range);
                        let location = FieldFileLocation::new(tsm_reader.clone(), block_it, vtype);
                        locations.push(location);
                    }
                }
            }
        }

        Ok(Self {
            name,
            value_type: vtype,
            cache_index: 0,
            cache_data: mem_data,
            locations,
        })
    }

    fn peek_cache(&mut self) -> Option<&DataType> {
        let mut opt_top = self.cache_data.get(self.cache_index);
        let mut opt_next = self.cache_data.get(self.cache_index + 1);

        while let (Some(top), Some(next)) = (opt_top, opt_next) {
            // if timestamp is same, select next data
            // deduplication
            if top.timestamp() == next.timestamp() {
                self.cache_index += 1;
                opt_top = Some(next);
                opt_next = self.cache_data.get(self.cache_index + 1);
            } else {
                break;
            }
        }

        opt_top
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

        if let Some(val) = self.peek_cache() {
            if data.timestamp() >= val.timestamp() {
                data = val.clone();
            }
        }

        if data.timestamp() == i64::MAX {
            return Ok(None);
        }
        Ok(Some(data))
    }

    fn next(&mut self, ts: i64) {
        if let Some(val) = self.peek_cache() {
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

pub fn filter_to_time_ranges(time_domain: &ColumnDomains<String>) -> Vec<TimeRange> {
    if time_domain.is_none() {
        // Does not contain any data, and returns an empty array directly
        return vec![];
    }

    if time_domain.is_all() {
        // Include all data
        return vec![TimeRange::all()];
    }

    let mut time_ranges: Vec<TimeRange> = Vec::new();

    if let Some(time_domain) = time_domain.domains() {
        assert!(time_domain.contains_key(TIME_FIELD_NAME));

        let domain = unsafe { time_domain.get(TIME_FIELD_NAME).unwrap_unchecked() };

        // Convert ScalarValue value to nanosecond timestamp
        let valid_and_generate_index_key = |v: &ScalarValue| {
            // Time can only be of type Timestamp
            assert!(matches!(v.get_datatype(), ArrowDataType::Timestamp(_, _)));
            unsafe { i64::try_from(v.clone()).unwrap_unchecked() }
        };

        match domain {
            Domain::Range(range_set) => {
                for (_, range) in range_set.low_indexed_ranges().into_iter() {
                    let range: &Range = range;

                    let start_bound = range.start_bound();
                    let end_bound = range.end_bound();

                    // Convert the time value in Bound to timestamp
                    let translate_bound = |bound: Bound<&ScalarValue>| match bound {
                        Bound::Unbounded => Bound::Unbounded,
                        Bound::Included(v) => Bound::Included(valid_and_generate_index_key(v)),
                        Bound::Excluded(v) => Bound::Excluded(valid_and_generate_index_key(v)),
                    };

                    let range = (translate_bound(start_bound), translate_bound(end_bound));
                    time_ranges.push(range.into());
                }
            }
            Domain::Equtable(vals) => {
                if !vals.is_white_list() {
                    // eg. time != xxx
                    time_ranges.push(TimeRange::all());
                } else {
                    // Contains the given value
                    for entry in vals.entries().into_iter() {
                        let entry: &ValueEntry = entry;

                        let ts = valid_and_generate_index_key(entry.value());

                        time_ranges.push(TimeRange::new(ts, ts));
                    }
                }
            }
            Domain::All => time_ranges.push(TimeRange::all()),
            Domain::None => return vec![],
        }
    }

    time_ranges
}

pub struct RowIterator {
    batch_size: usize,
    series_index: usize,
    series: Vec<u64>,
    engine: EngineRef,
    option: QueryOption,
    columns: Vec<CursorPtr>,
    version: Option<Arc<SuperVersion>>,

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
        let version = engine.get_db_version(&option.table_schema.db)?;

        let series = engine
            .get_series_id_by_filter(
                &option.table_schema.db,
                &option.table_schema.name,
                &option.tags_filter,
            )
            .context(IndexErrSnafu)?;

        debug!("series number: {}", series.len());

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

    fn build_series_columns(&mut self, id: SeriesId) -> Result<(), Error> {
        let start = Instant::now();

        if let Some(key) = self
            .engine
            .get_series_key(&self.option.table_schema.db, id)
            .context(IndexErrSnafu)?
        {
            self.columns.clear();
            let fields = self.option.table_schema.columns().to_vec();
            for item in fields {
                let field_name = item.name.clone();
                debug!("build series columns id:{:02X}, {:?}", id, item);
                let column: CursorPtr = match item.column_type {
                    ColumnType::Time => Box::new(TimeCursor::new(0, field_name)),

                    ColumnType::Tag => Box::new(TagCursor::new(
                        String::from_utf8(key.tag_val(&item.name))
                            .map_err(|_| Error::ErrCharacterSet)?,
                        field_name,
                    )),

                    ColumnType::Field(vtype) => match vtype {
                        ValueType::Unknown => todo!(),
                        _ => {
                            let cursor = FieldCursor::new(
                                unite_id(item.id as u64, id),
                                field_name,
                                vtype,
                                self,
                            )?;
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
            let val = column.peek()?;
            if let Some(ref data) = val {
                if column.is_field() {
                    min_time = min_num(min_time, data.timestamp());
                }
            }
            values.push(val)
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

        for (i, value) in values.into_iter().enumerate() {
            if self.columns[i].name() == TIME_FIELD {
                let field_builder = builder[i]
                    .as_any_mut()
                    .downcast_mut::<TimestampNanosecondBuilder>()
                    .unwrap();
                field_builder.append_value(min_time);

                continue;
            }

            match self.columns[i].val_type() {
                ValueType::Unknown => {
                    return Err(Error::UnKnowType);
                }
                ValueType::Float => {
                    let field_builder = builder[i]
                        .as_any_mut()
                        .downcast_mut::<Float64Builder>()
                        .unwrap();
                    if let Some(DataType::F64(_, val)) = value {
                        field_builder.append_value(val);
                    } else {
                        field_builder.append_null();
                    }
                }
                ValueType::Integer => {
                    let field_builder = builder[i]
                        .as_any_mut()
                        .downcast_mut::<Int64Builder>()
                        .unwrap();
                    if let Some(DataType::I64(_, val)) = value {
                        field_builder.append_value(val);
                    } else {
                        field_builder.append_null();
                    }
                }
                ValueType::Unsigned => {
                    let field_builder = builder[i]
                        .as_any_mut()
                        .downcast_mut::<UInt64Builder>()
                        .unwrap();
                    if let Some(DataType::U64(_, val)) = value {
                        field_builder.append_value(val);
                    } else {
                        field_builder.append_null();
                    }
                }
                ValueType::Boolean => {
                    let field_builder = builder[i]
                        .as_any_mut()
                        .downcast_mut::<BooleanBuilder>()
                        .unwrap();
                    if let Some(DataType::Bool(_, val)) = value {
                        field_builder.append_value(val);
                    } else {
                        field_builder.append_null();
                    }
                }
                ValueType::String => {
                    let field_builder = builder[i]
                        .as_any_mut()
                        .downcast_mut::<StringBuilder>()
                        .unwrap();
                    if let Some(DataType::Str(_, val)) = value {
                        field_builder.append_value(
                            String::from_utf8(val.to_vec()).map_err(|_| Error::ErrCharacterSet)?,
                        );
                    } else {
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
            if self.columns.is_empty() && self.next_series()?.is_none() {
                return Ok(None);
            }

            if self.collect_row_data(builder)?.is_some() {
                return Ok(Some(()));
            }
        }
    }

    fn record_builder(&self) -> Vec<ArrayBuilderPtr> {
        let mut builders: Vec<ArrayBuilderPtr> =
            Vec::with_capacity(self.option.table_schema.columns().len());
        for item in self.option.table_schema.columns().iter() {
            debug!("schema info {:02X} {}", item.id, item.name);

            match item.column_type {
                ColumnType::Tag => builders.push(Box::new(StringBuilder::with_capacity(
                    self.batch_size,
                    self.batch_size * 32,
                ))),
                ColumnType::Time => builders.push(Box::new(
                    TimestampNanosecondBuilder::with_capacity(self.batch_size),
                )),
                ColumnType::Field(t) => match t {
                    ValueType::Unknown => todo!(),
                    ValueType::Float => {
                        builders.push(Box::new(Float64Builder::with_capacity(self.batch_size)))
                    }
                    ValueType::Integer => {
                        builders.push(Box::new(Int64Builder::with_capacity(self.batch_size)))
                    }
                    ValueType::Unsigned => {
                        builders.push(Box::new(UInt64Builder::with_capacity(self.batch_size)))
                    }
                    ValueType::Boolean => {
                        builders.push(Box::new(BooleanBuilder::with_capacity(self.batch_size)))
                    }
                    ValueType::String => builders.push(Box::new(StringBuilder::with_capacity(
                        self.batch_size,
                        self.batch_size * 32,
                    ))),
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
                Ok(Some(_)) => {}
                Ok(None) => break,
                Err(err) => return Some(Err(err)),
            };
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
