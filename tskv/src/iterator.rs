use std::collections::HashMap;
use std::ops::{Bound, RangeBounds};
use std::sync::Arc;
use std::task::Poll;

use datafusion::arrow::array::{
    ArrayBuilder, BooleanBuilder, Float64Builder, Int64Builder, StringBuilder,
    TimestampNanosecondBuilder, UInt64Builder,
};
use datafusion::arrow::datatypes::{DataType as ArrowDataType, SchemaRef};
use datafusion::arrow::error::ArrowError;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::DataFusionError;
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryPool, MemoryReservation};
use datafusion::physical_plan::metrics::{
    self, BaselineMetrics, ExecutionPlanMetricsSet, MetricBuilder,
};
use datafusion::scalar::ScalarValue;
use minivec::MiniVec;
use models::predicate::domain::{ColumnDomains, Domain, PredicateRef, Range, ValueEntry};
use models::schema::{ColumnType, TableColumn, TskvTableSchema, TIME_FIELD, TIME_FIELD_NAME};
use models::utils::{min_num, unite_id};
use models::{FieldId, SeriesId, ValueType};
use parking_lot::RwLock;
use snafu::ResultExt;
use tokio::time::Instant;
use trace::{debug, error, info};

use super::engine::EngineRef;
use super::error::IndexErrSnafu;
use super::memcache::DataType;
use super::tseries_family::{ColumnFile, SuperVersion, TimeRange};
use super::tsm::{BlockMetaIterator, DataBlock, TsmReader};
use super::{error, ColumnFileId, Error};
use crate::compute::count::count_column_non_null_values;
use crate::schema::error::SchemaError;
use crate::tseries_family::Version;

pub type CursorPtr = Box<dyn Cursor>;
pub type ArrayBuilderPtr = Box<dyn ArrayBuilder>;

/// Stores metrics about the table writer execution.
#[derive(Debug)]
pub struct TableScanMetrics {
    baseline_metrics: BaselineMetrics,
    partition: usize,
    metrics: ExecutionPlanMetricsSet,
    reservation: Option<RwLock<MemoryReservation>>,
}

impl TableScanMetrics {
    /// Create new metrics
    pub fn new(
        metrics: &ExecutionPlanMetricsSet,
        partition: usize,
        pool: Option<&Arc<dyn MemoryPool>>,
    ) -> Self {
        let baseline_metrics = BaselineMetrics::new(metrics, partition);
        let reservation = match pool {
            None => None,
            Some(pool) => {
                let reservation = RwLock::new(
                    MemoryConsumer::new(format!("TableScanMetrics[{partition}]")).register(pool),
                );
                Some(reservation)
            }
        };

        Self {
            baseline_metrics,
            partition,
            metrics: metrics.clone(),
            reservation,
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
        poll: Poll<Option<std::result::Result<RecordBatch, DataFusionError>>>,
    ) -> Poll<Option<std::result::Result<RecordBatch, DataFusionError>>> {
        self.baseline_metrics.record_poll(poll)
    }

    pub fn record_memory(&self, rb: &RecordBatch) -> Result<(), DataFusionError> {
        if let Some(res) = &self.reservation {
            res.write().try_grow(rb.get_array_memory_size())?
        }
        Ok(())
    }

    /// Records the fact that this operator's execution is complete
    /// (recording the `end_time` metric).
    pub fn done(&self) {
        self.baseline_metrics.done()
    }
}

/// Stores metrics about the table writer execution.
#[derive(Debug, Clone)]
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

#[derive(Debug, Clone)]
pub struct QueryOption {
    pub batch_size: usize,
    pub tenant: String,
    pub filter: PredicateRef,
    pub df_schema: SchemaRef,
    pub table_schema: TskvTableSchema,
    pub metrics: TskvSourceMetrics,
    pub aggregates: Option<Vec<TableColumn>>, // TODO: Use PushedAggregateFunction

    pub time_filter: ColumnDomains<String>,
    pub tags_filter: ColumnDomains<String>,
    pub fields_filter: ColumnDomains<String>,
}

impl QueryOption {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        batch_size: usize,
        tenant: String,
        filter: PredicateRef,
        aggregates: Option<Vec<TableColumn>>, // TODO: Use PushedAggregateFunction
        df_schema: SchemaRef,
        table_schema: TskvTableSchema,
        metrics: TskvSourceMetrics,
    ) -> Self {
        let domains_filter = filter
            .filter()
            .translate_column(|c| table_schema.column(&c.name).cloned());

        // 提取过滤条件
        let time_filter = domains_filter.translate_column(|e| match e.column_type {
            ColumnType::Time => Some(e.name.clone()),
            _ => None,
        });

        let tags_filter = domains_filter.translate_column(|e| match e.column_type {
            ColumnType::Tag => Some(e.name.clone()),
            _ => None,
        });

        let fields_filter = domains_filter.translate_column(|e| match e.column_type {
            ColumnType::Field(_) => Some(e.name.clone()),
            _ => None,
        });

        Self {
            batch_size,
            tenant,
            filter,
            table_schema,
            df_schema,
            metrics,
            aggregates,

            time_filter,
            tags_filter,
            fields_filter,
        }
    }

    pub fn parse_time_ranges(
        filter: PredicateRef,
        table_schema: TskvTableSchema,
    ) -> Vec<TimeRange> {
        let filter = filter
            .filter()
            .translate_column(|c| table_schema.column(&c.name).cloned());

        let time_filter = filter.translate_column(|e| match e.column_type {
            ColumnType::Time => Some(e.name.clone()),
            _ => None,
        });

        let time_ranges: Vec<TimeRange> = filter_to_time_ranges(&time_filter);

        time_ranges
    }
}

pub struct FieldFileLocation {
    reader: Arc<TsmReader>,
    block_it: BlockMetaIterator,

    read_index: usize,
    data_block: DataBlock,
}

impl FieldFileLocation {
    pub fn new(reader: Arc<TsmReader>, block_it: BlockMetaIterator, vtype: ValueType) -> Self {
        Self {
            reader,
            block_it,
            read_index: 0,
            data_block: DataBlock::new(0, vtype),
        }
    }

    pub async fn peek(&mut self) -> Result<Option<DataType>, Error> {
        if self.read_index >= self.data_block.len() {
            if let Some(meta) = self.block_it.next() {
                self.read_index = 0;
                self.data_block = self.reader.get_data_block(&meta).await?;
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
#[async_trait::async_trait]
pub trait Cursor: Send + Sync {
    fn name(&self) -> &String;
    fn is_field(&self) -> bool;
    fn val_type(&self) -> ValueType;

    async fn next(&mut self, ts: i64);
    async fn peek(&mut self) -> Result<Option<DataType>, Error>;
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

#[async_trait::async_trait]
impl Cursor for TimeCursor {
    fn name(&self) -> &String {
        &self.name
    }

    fn is_field(&self) -> bool {
        false
    }

    fn val_type(&self) -> ValueType {
        ValueType::Integer
    }

    async fn next(&mut self, _ts: i64) {}

    async fn peek(&mut self) -> Result<Option<DataType>, Error> {
        let data = DataType::I64(self.ts, self.ts);

        Ok(Some(data))
    }
}

//-----------Tag Cursor----------------
pub struct TagCursor {
    name: String,
    value: Option<String>,
}

impl TagCursor {
    pub fn new(name: String, value: Option<String>) -> Self {
        Self { name, value }
    }
}

#[async_trait::async_trait]
impl Cursor for TagCursor {
    fn name(&self) -> &String {
        &self.name
    }

    fn is_field(&self) -> bool {
        false
    }

    fn val_type(&self) -> ValueType {
        ValueType::String
    }

    async fn next(&mut self, _ts: i64) {}

    async fn peek(&mut self) -> Result<Option<DataType>, Error> {
        match &self.value {
            Some(value) => Ok(Some(DataType::Str(0, MiniVec::from(value.as_bytes())))),
            None => Ok(None),
        }
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

    pub async fn new(
        field_id: FieldId,
        name: String,
        vtype: ValueType,
        iterator: &mut RowIterator,
    ) -> Result<Self, Error> {
        let super_version = match iterator.version.clone() {
            Some(v) => v,
            None => return Ok(Self::empty(vtype, name)),
        };

        let time_ranges: Vec<TimeRange> = filter_to_time_ranges(&iterator.option.time_filter);
        debug!("Pushed time range filter: {:?}", time_ranges);
        let time_predicate = |ts| {
            time_ranges
                .iter()
                .any(|time_range| time_range.is_boundless() || time_range.contains(ts))
        };

        // get data from im_memcache and memcache
        let mut mem_data: Vec<DataType> = Vec::new();
        super_version.caches.read_field_data(
            field_id,
            time_predicate,
            |_| true,
            |d| mem_data.push(d),
        );
        mem_data.sort_by_key(|data| data.timestamp());

        debug!(
            "build memcache data id: {:02X}, len: {}",
            field_id,
            mem_data.len()
        );

        // get data from levelinfo
        let mut locations = vec![];
        for level in super_version.version.levels_info.iter().rev() {
            for file in level.files.iter() {
                if file.is_deleted() {
                    continue;
                }
                if !file.contains_field_id(field_id) {
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

                    let tsm_reader = iterator
                        .get_tsm_reader(super_version.version.clone(), file.clone())
                        .await?;
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

#[async_trait::async_trait]
impl Cursor for FieldCursor {
    fn name(&self) -> &String {
        &self.name
    }

    async fn peek(&mut self) -> Result<Option<DataType>, Error> {
        let mut data = DataType::new(self.value_type, i64::MAX);
        for loc in self.locations.iter_mut() {
            if let Some(val) = loc.peek().await? {
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

    async fn next(&mut self, ts: i64) {
        if let Some(val) = self.peek_cache() {
            if val.timestamp() == ts {
                self.cache_index += 1;
            }
        }

        for loc in self.locations.iter_mut() {
            if let Some(val) = loc.peek().await.unwrap() {
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
    series_index: usize,
    series: Vec<u32>,
    engine: EngineRef,
    option: QueryOption,
    columns: Vec<CursorPtr>,
    version: Option<Arc<SuperVersion>>,

    open_files: HashMap<ColumnFileId, Arc<TsmReader>>,

    batch_size: usize,
    vnode_id: u32,
    metrics: TskvSourceMetrics,

    finished: bool,
}

impl RowIterator {
    pub async fn new(engine: EngineRef, option: QueryOption, vnode_id: u32) -> Result<Self, Error> {
        let version = engine
            .get_db_version(&option.tenant, &option.table_schema.db, vnode_id)
            .await?;

        let series = engine
            .get_series_id_by_filter(
                vnode_id,
                &option.tenant,
                &option.table_schema.db,
                &option.table_schema.name,
                &option.tags_filter,
            )
            .await
            .context(error::IndexErrSnafu)?;

        info!("vnode_id: {}, series number: {}", vnode_id, series.len());

        let metrics = option.metrics.clone();
        let batch_size = option.batch_size;
        Ok(Self {
            series,
            engine,
            option,
            version,
            vnode_id,

            columns: vec![],
            series_index: usize::MAX,
            open_files: HashMap::new(),

            batch_size,
            metrics,
            finished: false,
        })
    }

    pub async fn get_tsm_reader(
        &mut self,
        version: Arc<Version>,
        file: Arc<ColumnFile>,
    ) -> Result<Arc<TsmReader>, Error> {
        if let Some(val) = self.open_files.get(&file.file_id()) {
            return Ok(val.clone());
        }
        // let tsm_reader = TsmReader::open(file.file_path()).await?;
        let tsm_reader = version.get_tsm_reader(file.file_path()).await?;
        self.open_files.insert(file.file_id(), tsm_reader.clone());

        Ok(tsm_reader)
    }

    async fn build_series_columns(&mut self, id: SeriesId) -> Result<(), Error> {
        let start = Instant::now();

        if let Some(key) = self
            .engine
            .get_series_key(
                &self.option.tenant,
                &self.option.table_schema.db,
                self.vnode_id,
                id,
            )
            .await
            .context(IndexErrSnafu)?
        {
            self.columns.clear();
            let fields = self.option.table_schema.columns().to_vec();
            for item in fields {
                let field_name = item.name.clone();
                debug!("build series columns id:{:02X}, {:?}", id, item);
                let column: CursorPtr = match item.column_type {
                    ColumnType::Time => Box::new(TimeCursor::new(0, field_name)),

                    ColumnType::Tag => {
                        let tag_val = match key.tag_val(&item.name) {
                            Some(val) => {
                                Some(String::from_utf8(val).map_err(|_| Error::ErrCharacterSet)?)
                            }
                            None => None,
                        };

                        Box::new(TagCursor::new(field_name, tag_val))
                    }

                    ColumnType::Field(vtype) => match vtype {
                        ValueType::Unknown => {
                            error!("Unknown field type for column {}", &field_name);
                            todo!()
                        }
                        _ => {
                            let cursor =
                                FieldCursor::new(unite_id(item.id, id), field_name, vtype, self)
                                    .await?;
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

    async fn next_series(&mut self) -> Result<Option<()>, Error> {
        if self.series_index == usize::MAX {
            self.series_index = 0;
        } else {
            self.series_index += 1;
        }

        if self.series_index >= self.series.len() {
            return Ok(None);
        }

        self.build_series_columns(self.series[self.series_index])
            .await?;

        Ok(Some(()))
    }

    async fn collect_row_data(
        &mut self,
        builder: &mut [ArrayBuilderPtr],
    ) -> Result<Option<()>, Error> {
        debug!("======collect_row_data=========");
        let timer = self.metrics.elapsed_field_scan().timer();

        let mut min_time = i64::MAX;
        let mut values = Vec::with_capacity(self.columns.len());
        for column in self.columns.iter_mut() {
            let val = column.peek().await?;
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
                    column.next(ts).await;
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
                    return Err(Error::CommonError {
                        reason: format!("unknown type of {}", self.columns[i].name()),
                    });
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

    async fn collect_aggregate_row_data(
        &mut self,
        builder: &mut [ArrayBuilderPtr],
    ) -> Result<Option<()>, Error> {
        if self.is_finish() {
            return Ok(None);
        }
        self.finished = true;
        debug!("======collect_aggregate_row_data=========");
        let time_ranges: Vec<TimeRange> = filter_to_time_ranges(&self.option.time_filter);
        let time_ranges = Arc::new(time_ranges);
        if self.version.is_none() {
            return Ok(None);
        }
        let version = self.version.clone().unwrap();
        if let Some(aggregates) = self.option.aggregates.as_ref() {
            for (i, item) in aggregates.iter().enumerate() {
                match item.column_type {
                    ColumnType::Tag => todo!(),
                    ColumnType::Time => {
                        let agg_ret = count_column_non_null_values(
                            version.clone(),
                            &self.series,
                            None,
                            time_ranges.clone(),
                        )
                        .await?;
                        let field_builder = builder[i]
                            .as_any_mut()
                            .downcast_mut::<Int64Builder>()
                            .unwrap();
                        field_builder.append_value(agg_ret as i64);
                    }
                    ColumnType::Field(vtype) => match vtype {
                        ValueType::Unknown => {
                            return Err(Error::CommonError {
                                reason: format!("unknown type of {}", self.columns[i].name()),
                            });
                        }
                        _ => {
                            let agg_ret = count_column_non_null_values(
                                version.clone(),
                                &self.series,
                                Some(item.id),
                                time_ranges.clone(),
                            )
                            .await?;
                            let field_builder = builder[i]
                                .as_any_mut()
                                .downcast_mut::<Int64Builder>()
                                .unwrap();
                            field_builder.append_value(agg_ret as i64);
                            // field_builder.append_null();
                        }
                    },
                };
            }

            Ok(Some(()))
        } else {
            Ok(None)
        }
    }

    async fn next_row(&mut self, builder: &mut [ArrayBuilderPtr]) -> Result<Option<()>, Error> {
        if self.option.aggregates.is_some() {
            self.collect_aggregate_row_data(builder).await
        } else {
            loop {
                if self.columns.is_empty() && self.next_series().await?.is_none() {
                    return Ok(None);
                }

                if self.collect_row_data(builder).await?.is_some() {
                    return Ok(Some(()));
                }
            }
        }
    }

    fn record_builder(&self) -> Vec<ArrayBuilderPtr> {
        if let Some(aggregates) = self.option.aggregates.as_ref() {
            // TODO: Correct the aggregate columns order.
            let mut builders: Vec<ArrayBuilderPtr> = Vec::with_capacity(aggregates.len());
            for agg in self.option.aggregates.iter() {
                builders.push(Box::new(Int64Builder::with_capacity(self.batch_size)));
            }
            return builders;
        }
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
        if self.finished {
            return true;
        }
        if self.series_index == usize::MAX {
            return false;
        }

        self.series_index >= self.series.len()
    }
}

impl RowIterator {
    pub async fn next(&mut self) -> Option<Result<RecordBatch, Error>> {
        if self.is_finish() {
            return None;
        }

        let timer = self.metrics.elapsed_point_to_record_batch().timer();
        let mut builder = self.record_builder();
        timer.done();

        for _ in 0..self.batch_size {
            match self.next_row(&mut builder).await {
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

            match RecordBatch::try_new(self.option.df_schema.clone(), cols) {
                Ok(batch) => Some(Ok(batch)),
                Err(err) => Some(Err(Error::CommonError {
                    reason: format!("iterator fail, {}", err),
                })),
            }
        };
        timer.done();

        result
    }
}
