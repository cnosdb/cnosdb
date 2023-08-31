use std::sync::Arc;

use datafusion::arrow::array::{
    ArrayBuilder, ArrayRef, BooleanArray, BooleanBuilder, Float64Builder, Int64Builder,
    PrimitiveArray, PrimitiveBuilder, StringArray, StringBuilder, TimestampMicrosecondBuilder,
    TimestampMillisecondBuilder, TimestampNanosecondBuilder, TimestampSecondBuilder, UInt64Builder,
};
use datafusion::arrow::datatypes::{
    ArrowPrimitiveType, Float64Type, Int64Type, SchemaRef, TimeUnit, TimestampMicrosecondType,
    TimestampMillisecondType, TimestampNanosecondType, TimestampSecondType, UInt64Type,
};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::physical_plan::metrics::{self, ExecutionPlanMetricsSet, MetricBuilder};
use minivec::MiniVec;
use models::meta_data::VnodeId;
use models::predicate::domain::{self, QueryArgs, QueryExpr, TimeRanges};
use models::predicate::PlacedSplit;
use models::schema::{ColumnType, TableColumn, TskvTableSchemaRef};
use models::utils::{min_num, unite_id};
use models::{FieldId, SeriesId, Timestamp, ValueType};
use protos::kv_service::QueryRecordBatchRequest;
use tokio::runtime::Runtime;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use trace::{debug, error, SpanRecorder};

use crate::compute::count::count_column_non_null_values;
use crate::error::Result;
use crate::memcache::DataType;
use crate::reader::Cursor;
use crate::tseries_family::SuperVersion;
use crate::tsm::{BlockMetaIterator, DataBlock, TsmReader};
use crate::{EngineRef, Error};

pub type CursorPtr = Box<dyn Cursor>;

pub struct ArrayBuilderPtr {
    pub ptr: Box<dyn ArrayBuilder>,
    pub column_type: ColumnType,
}

impl ArrayBuilderPtr {
    pub fn new(ptr: Box<dyn ArrayBuilder>, column_type: ColumnType) -> Self {
        Self { ptr, column_type }
    }

    #[inline(always)]
    fn builder<T: ArrowPrimitiveType>(&mut self) -> Option<&mut PrimitiveBuilder<T>> {
        self.ptr.as_any_mut().downcast_mut::<PrimitiveBuilder<T>>()
    }

    pub fn append_primitive<T: ArrowPrimitiveType>(&mut self, t: T::Native) {
        if let Some(b) = self.builder::<T>() {
            b.append_value(t);
        } else {
            error!(
                "Failed to get primitive-type array builder to insert {:?} array",
                self.column_type
            );
        }
    }

    pub fn append_timestamp(&mut self, unit: &TimeUnit, timestamp: Timestamp) {
        match unit {
            TimeUnit::Second => self.append_primitive::<TimestampSecondType>(timestamp),
            TimeUnit::Millisecond => self.append_primitive::<TimestampMillisecondType>(timestamp),
            TimeUnit::Microsecond => self.append_primitive::<TimestampMicrosecondType>(timestamp),
            TimeUnit::Nanosecond => self.append_primitive::<TimestampNanosecondType>(timestamp),
        }
    }

    pub fn append_value(
        &mut self,
        value_type: ValueType,
        value: Option<DataType>,
        column_name: &str,
    ) -> Result<()> {
        match value_type {
            ValueType::Unknown => {
                return Err(Error::CommonError {
                    reason: format!("unknown type of column '{}'", column_name),
                });
            }
            ValueType::String => {
                if let Some(DataType::Str(_, val)) = value {
                    let data =
                        String::from_utf8(val.to_vec()).map_err(|_| Error::ErrCharacterSet)?;
                    self.append_string(data);
                } else {
                    self.append_null_string();
                }
            }
            ValueType::Boolean => {
                if let Some(DataType::Bool(_, val)) = value {
                    self.append_bool(val);
                } else {
                    self.append_null_bool();
                }
            }
            ValueType::Float => {
                if let Some(DataType::F64(_, val)) = value {
                    self.append_primitive::<Float64Type>(val);
                } else {
                    self.append_primitive_null::<Float64Type>();
                }
            }
            ValueType::Integer => {
                if let Some(DataType::I64(_, val)) = value {
                    self.append_primitive::<Int64Type>(val);
                } else {
                    self.append_primitive_null::<Int64Type>();
                }
            }
            ValueType::Unsigned => {
                if let Some(DataType::U64(_, val)) = value {
                    self.append_primitive::<UInt64Type>(val);
                } else {
                    self.append_primitive_null::<UInt64Type>();
                }
            }
        }
        Ok(())
    }

    pub fn append_primitive_null<T: ArrowPrimitiveType>(&mut self) {
        if let Some(b) = self.builder::<T>() {
            b.append_null();
        } else {
            error!(
                "Failed to get primitive-type array builder to insert {:?} array",
                self.column_type
            );
        }
    }

    pub fn append_bool(&mut self, data: bool) {
        if let Some(b) = self.ptr.as_any_mut().downcast_mut::<BooleanBuilder>() {
            b.append_value(data);
        } else {
            error!(
                "Failed to get boolean array builder to insert {:?} array",
                self.column_type
            );
        }
    }

    pub fn append_null_bool(&mut self) {
        if let Some(b) = self.ptr.as_any_mut().downcast_mut::<BooleanBuilder>() {
            b.append_null();
        } else {
            error!(
                "Failed to get boolean array builder to insert {:?} array",
                self.column_type
            );
        }
    }

    pub fn append_string(&mut self, data: String) {
        if let Some(b) = self.ptr.as_any_mut().downcast_mut::<StringBuilder>() {
            b.append_value(data);
        } else {
            error!(
                "Failed to get string array builder to insert {:?} array",
                self.column_type
            );
        }
    }

    pub fn append_null_string(&mut self) {
        if let Some(b) = self.ptr.as_any_mut().downcast_mut::<StringBuilder>() {
            b.append_null();
        } else {
            error!(
                "Failed to get string array builder to insert {:?} array",
                self.column_type
            );
        }
    }

    fn extend_primitive_array<T: ArrowPrimitiveType>(&mut self, array: ArrayRef) {
        let builder = self.builder::<T>();
        let array = array.as_any().downcast_ref::<PrimitiveArray<T>>();
        if let (Some(b), Some(a)) = (builder, array) {
            b.extend(a.iter())
        } else {
            error!(
                "Failed to get primitive-type array and array builder to insert {:?} array",
                self.column_type
            );
        }
    }

    fn extend_bool_array(&mut self, array: ArrayRef) {
        let builder = self.ptr.as_any_mut().downcast_mut::<BooleanBuilder>();
        let array = array.as_any().downcast_ref::<BooleanArray>();
        if let (Some(b), Some(a)) = (builder, array) {
            b.extend(a.iter())
        } else {
            error!(
                "Failed to get boolean array and array builder to insert {:?} array",
                self.column_type
            );
        }
    }

    fn extend_string_array(&mut self, array: ArrayRef) {
        let builder = self.ptr.as_any_mut().downcast_mut::<StringBuilder>();
        let array = array.as_any().downcast_ref::<StringArray>();
        if let (Some(b), Some(a)) = (builder, array) {
            b.extend(a)
        } else {
            error!(
                "Failed to get string array and array builder to insert {:?} array",
                self.column_type
            );
        }
    }

    pub fn append_column_data(&mut self, column: ArrayRef) {
        match self.column_type {
            ColumnType::Tag | ColumnType::Field(ValueType::String) => {
                self.extend_string_array(column);
            }
            ColumnType::Time(ref unit) => match unit {
                TimeUnit::Second => self.extend_primitive_array::<TimestampSecondType>(column),
                TimeUnit::Millisecond => {
                    self.extend_primitive_array::<TimestampMillisecondType>(column)
                }
                TimeUnit::Microsecond => {
                    self.extend_primitive_array::<TimestampMicrosecondType>(column)
                }
                TimeUnit::Nanosecond => {
                    self.extend_primitive_array::<TimestampNanosecondType>(column)
                }
            },
            ColumnType::Field(ValueType::Float) => {
                self.extend_primitive_array::<Float64Type>(column);
            }
            ColumnType::Field(ValueType::Integer) => {
                self.extend_primitive_array::<Int64Type>(column);
            }
            ColumnType::Field(ValueType::Unsigned) => {
                self.extend_primitive_array::<UInt64Type>(column);
            }
            ColumnType::Field(ValueType::Boolean) => {
                self.extend_bool_array(column);
            }
            _ => {
                error!("Trying to get unknown-type array builder");
            }
        }
    }
}

/// Stores metrics about the table writer execution.
#[derive(Debug, Clone)]
pub struct SeriesGroupRowIteratorMetrics {
    elapsed_point_to_record_batch: metrics::Time,
    elapsed_field_scan: metrics::Time,
    elapsed_series_scan: metrics::Time,
    elapsed_build_resp_stream: metrics::Time,
    elapsed_get_data_from_memcache: metrics::Time,
    elapsed_get_field_location: metrics::Time,
}

impl SeriesGroupRowIteratorMetrics {
    /// Create new metrics
    pub fn new(metrics: &ExecutionPlanMetricsSet, partition: usize) -> Self {
        let elapsed_point_to_record_batch =
            MetricBuilder::new(metrics).subset_time("elapsed_point_to_record_batch", partition);

        let elapsed_field_scan =
            MetricBuilder::new(metrics).subset_time("elapsed_field_scan", partition);

        let elapsed_series_scan =
            MetricBuilder::new(metrics).subset_time("elapsed_series_scan", partition);

        let elapsed_build_resp_stream =
            MetricBuilder::new(metrics).subset_time("elapsed_build_resp_stream", partition);

        let elapsed_get_data_from_memcache =
            MetricBuilder::new(metrics).subset_time("elapsed_get_data_from_memcache", partition);

        let elapsed_get_field_location =
            MetricBuilder::new(metrics).subset_time("elapsed_get_field_location", partition);

        Self {
            elapsed_point_to_record_batch,
            elapsed_field_scan,
            elapsed_series_scan,
            elapsed_build_resp_stream,
            elapsed_get_data_from_memcache,
            elapsed_get_field_location,
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

    pub fn elapsed_build_resp_stream(&self) -> &metrics::Time {
        &self.elapsed_build_resp_stream
    }

    pub fn elapsed_get_data_from_memcache(&self) -> &metrics::Time {
        &self.elapsed_get_data_from_memcache
    }

    pub fn elapsed_get_field_location(&self) -> &metrics::Time {
        &self.elapsed_get_field_location
    }
}

// 1. Tsm文件遍历： KeyCursor
//  功能：根据输入参数遍历Tsm文件
//  输入参数： SeriesKey、FieldName、StartTime、EndTime、Ascending
//  功能函数：调用Peek()—>(value, timestamp)得到一个值；调用Next()方法游标移到下一个值。
// 2. Field遍历： FiledCursor
//  功能：一个Field特定SeriesKey的遍历
//  输入输出参数同KeyCursor，区别是需要读取缓存数据，并按照特定顺序返回
// 3. Fields->行转换器
//  一行数据是由同一个时间点的多个Field得到。借助上面的FieldCursor按照时间点对齐多个Field-Value拼接成一行数据。其过程类似于多路归并排序。
// 4. Iterator接口抽象层
//  调用Next接口返回一行数据，并且屏蔽查询是本机节点数据还是其他节点数据
// 5. 行数据到DataFusion的RecordBatch转换器
//  调用Iterator.Next得到行数据，然后转换行数据为RecordBatch结构
#[derive(Debug, Clone)]
pub struct QueryOption {
    pub batch_size: usize,
    pub split: PlacedSplit,
    pub df_schema: SchemaRef,
    pub table_schema: TskvTableSchemaRef,
    pub aggregates: Option<Vec<TableColumn>>, // TODO: Use PushedAggregateFunction
}

impl QueryOption {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        batch_size: usize,
        split: PlacedSplit,
        aggregates: Option<Vec<TableColumn>>, // TODO: Use PushedAggregateFunction
        df_schema: SchemaRef,
        table_schema: TskvTableSchemaRef,
    ) -> Self {
        Self {
            batch_size,
            split,
            aggregates,
            df_schema,
            table_schema,
        }
    }

    pub fn tenant_name(&self) -> &str {
        &self.table_schema.tenant
    }

    pub fn to_query_record_batch_request(
        &self,
        vnode_ids: Vec<VnodeId>,
    ) -> Result<QueryRecordBatchRequest, models::Error> {
        let args = QueryArgs {
            vnode_ids,
            limit: self.split.limit(),
            batch_size: self.batch_size,
        };
        let expr = QueryExpr {
            split: self.split.clone(),
            df_schema: self.df_schema.as_ref().clone(),
            table_schema: self.table_schema.clone(),
        };

        let args_bytes = QueryArgs::encode(&args)?;
        let expr_bytes = QueryExpr::encode(&expr)?;
        let aggs_bytes = domain::encode_agg(&self.aggregates)?;

        Ok(QueryRecordBatchRequest {
            args: args_bytes,
            expr: expr_bytes,
            aggs: aggs_bytes,
        })
    }
}

pub struct FieldFileLocation {
    reader: Arc<TsmReader>,
    block_meta_iter: BlockMetaIterator,
    time_ranges: Arc<TimeRanges>,

    data_block: DataBlock,
    /// The first index of a DataType in a DataBlock
    data_block_i: usize,
    /// The last index of a DataType in a DataBlock
    data_block_i_end: usize,
    intersected_time_ranges: TimeRanges,
    intersected_time_ranges_i: usize,
}

impl FieldFileLocation {
    pub fn new(
        reader: Arc<TsmReader>,
        time_ranges: Arc<TimeRanges>,
        block_meta_iter: BlockMetaIterator,
        vtype: ValueType,
    ) -> Self {
        Self {
            reader,
            block_meta_iter,
            time_ranges,
            // TODO: can here use unsafe api MaybeUninit<DataBLock> ?
            data_block: DataBlock::new(0, vtype),
            // Let data block index > end index when init to make it load from reader
            // for the first time to `peek()`.
            data_block_i: 1,
            data_block_i_end: 0,
            intersected_time_ranges: TimeRanges::empty(),
            intersected_time_ranges_i: 0,
        }
    }

    pub async fn peek(&mut self) -> Result<Option<DataType>> {
        // Check if we need to init.
        if self.data_block_i > self.data_block_i_end
            && !self.next_intersected_index_range()
            && !self.next_data_block().await?
        {
            return Ok(None);
        }

        Ok(self.data_block.get(self.data_block_i))
    }

    pub fn next(&mut self) {
        self.data_block_i += 1;
    }

    /// Iterates the ramaining BlockMeta in `block_meta_iter`, if there are no remaining BlockMeta's,
    /// then return Ok(false).
    ///
    /// Iteration will continue until there are intersected time range between DataBlock and `time_ranges`.
    async fn next_data_block(&mut self) -> Result<bool> {
        let mut has_next_block = false;

        // Get next BlockMeta to locate the next DataBlock from file.
        while let Some(meta) = self.block_meta_iter.next() {
            if meta.count() == 0 {
                continue;
            }
            let time_range = meta.time_range();
            // Check if the time range of the BlockMeta intersected with the given time ranges.
            if let Some(intersected_tr) = self.time_ranges.intersect(&time_range) {
                // Load a DataBlock from reader by BlockMeta.
                self.data_block = self.reader.get_data_block(&meta).await?;
                self.intersected_time_ranges = intersected_tr;
                self.intersected_time_ranges_i = 0;
                if self.next_intersected_index_range() {
                    // Found next DataBlock and range to iterate.
                    has_next_block = true;
                    break;
                }
            }
        }

        Ok(has_next_block)
    }

    /// Iterates the ramaining TimeRange in `intersected_time_ranges`, if there are no remaning TimeRange's.
    /// then return false.
    ///
    /// If there are overlaped time range of DataBlock and TimeRanges, set iteration range of `data_block`
    /// and return true, otherwise set the iteration range a zero-length range `[1, 0]` and return false.
    ///
    /// **Note**: Call of this method should be arranged after the call of method `next_data_block`.
    fn next_intersected_index_range(&mut self) -> bool {
        self.data_block_i = 1;
        self.data_block_i_end = 0;
        if self.intersected_time_ranges.is_empty()
            || self.intersected_time_ranges_i >= self.intersected_time_ranges.len()
        {
            false
        } else {
            let tr_idx_start = self.intersected_time_ranges_i;
            for tr in self.intersected_time_ranges.time_ranges()[tr_idx_start..].iter() {
                self.intersected_time_ranges_i += 1;
                // Check if the DataBlock matches one of the intersected time ranges.
                // TODO: sometimes the comparison in loop can stop earily.
                if let Some((min, max)) = self.data_block.index_range(tr) {
                    self.data_block_i = min;
                    self.data_block_i_end = max;
                    return true;
                }
            }
            false
        }
    }
}

impl std::fmt::Debug for FieldFileLocation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FieldFileLocation")
            .field("file_id", &self.reader.file_id())
            .field("time_ranges", &self.time_ranges)
            .field("data_block_range", &self.data_block.time_range())
            .field("data_block_i", &self.data_block_i)
            .field("data_block_i_end", &self.data_block_i_end)
            .field("intersected_time_ranges", &self.intersected_time_ranges)
            .field("intersected_time_ranges_i", &self.intersected_time_ranges_i)
            .finish()
    }
}

//-----------Time Cursor----------------
pub struct TimeCursor {
    ts: i64,
    name: String,
    unit: TimeUnit,
}

impl TimeCursor {
    pub fn new(ts: i64, name: String, unit: TimeUnit) -> Self {
        Self { ts, name, unit }
    }
}

#[async_trait::async_trait]
impl Cursor for TimeCursor {
    fn name(&self) -> &String {
        &self.name
    }

    fn column_type(&self) -> ColumnType {
        ColumnType::Time(self.unit.clone())
    }

    async fn next(&mut self, _ts: i64) {}

    async fn peek(&mut self) -> Result<Option<DataType>> {
        let data = DataType::I64(self.ts, self.ts);

        Ok(Some(data))
    }
}

//-----------Tag Cursor----------------
pub struct TagCursor {
    name: String,
    value: Option<Vec<u8>>,
}

impl TagCursor {
    pub fn new(name: String, value: Option<Vec<u8>>) -> Self {
        Self { name, value }
    }
}

#[async_trait::async_trait]
impl Cursor for TagCursor {
    fn name(&self) -> &String {
        &self.name
    }

    fn column_type(&self) -> ColumnType {
        ColumnType::Tag
    }

    async fn next(&mut self, _ts: i64) {}

    async fn peek(&mut self) -> Result<Option<DataType>> {
        match &self.value {
            Some(value) => Ok(Some(DataType::Str(0, MiniVec::from(value.as_slice())))),
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

    async fn peek(&mut self) -> Result<Option<DataType>> {
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

    fn column_type(&self) -> ColumnType {
        ColumnType::Field(self.value_type)
    }
}

pub struct RowIterator {
    runtime: Arc<Runtime>,
    engine: EngineRef,
    query_option: Arc<QueryOption>,
    vnode_id: VnodeId,

    /// Super version of vnode_id, maybe None.
    super_version: Option<Arc<SuperVersion>>,
    /// List of series id filtered from engine.
    series_ids: Arc<Vec<SeriesId>>,
    series_iter_receiver: Receiver<Option<Result<RecordBatch>>>,
    series_iter_closer: CancellationToken,
    /// Whether this iterator was finsihed.
    is_finished: bool,
    #[allow(unused)]
    span_recorder: SpanRecorder,
    metrics_set: ExecutionPlanMetricsSet,
}

impl RowIterator {
    pub async fn new(
        runtime: Arc<Runtime>,
        engine: EngineRef,
        query_option: QueryOption,
        vnode_id: VnodeId,
        span_recorder: SpanRecorder,
    ) -> Result<Self> {
        // TODO refac: None 代表没有数据，后续不需要执行
        let super_version = {
            let mut span_recorder = span_recorder.child("get super version");
            engine
                .get_db_version(
                    &query_option.table_schema.tenant,
                    &query_option.table_schema.db,
                    vnode_id,
                )
                .await
                .map_err(|err| {
                    span_recorder.error(err.to_string());
                    err
                })?
        };

        let series_ids = {
            let mut span_recorder = span_recorder.child("get series ids by filter");
            engine
                .get_series_id_by_filter(
                    &query_option.table_schema.tenant,
                    &query_option.table_schema.db,
                    &query_option.table_schema.name,
                    vnode_id,
                    query_option.split.tags_filter(),
                )
                .await
                .map_err(|err| {
                    span_recorder.error(err.to_string());
                    err
                })?
        };

        debug!(
            "Iterating rows: vnode_id: {vnode_id}, serie_ids_count: {}",
            series_ids.len()
        );
        let metrics_set = ExecutionPlanMetricsSet::new();
        let query_option = Arc::new(query_option);
        let series_len = series_ids.len();
        let (tx, rx) = channel(1);
        if query_option.aggregates.is_some() {
            // TODO: Correct the aggregate columns order.
            let mut row_iterator = Self {
                runtime,
                engine,
                query_option,
                vnode_id,
                super_version,
                series_ids: Arc::new(series_ids),
                series_iter_receiver: rx,
                series_iter_closer: CancellationToken::new(),
                is_finished: false,
                span_recorder,
                metrics_set,
            };
            row_iterator.new_series_group_iteration(0, series_len, tx);

            Ok(row_iterator)
        } else {
            let mut row_iterator = Self {
                runtime,
                engine,
                query_option,
                vnode_id,
                super_version,
                series_ids: Arc::new(series_ids),
                series_iter_receiver: rx,
                series_iter_closer: CancellationToken::new(),
                is_finished: false,
                span_recorder,
                metrics_set,
            };

            if series_len == 0 {
                return Ok(row_iterator);
            }

            // TODO：get series_group_size more intelligently
            let series_group_num = num_cpus::get().min(series_len);
            // `usize::div_ceil(self, Self)` is now unstable, so do it by-hand.
            let series_group_size = series_len / series_group_num
                + if series_len % series_group_num == 0 {
                    0
                } else {
                    1
                };

            for i in 0..series_group_num {
                let start = series_group_size * i;
                let mut end = start + series_group_size;
                if end > series_len {
                    end = series_len
                }

                row_iterator.new_series_group_iteration(start, end, tx.clone());
            }

            Ok(row_iterator)
        }
    }

    fn new_series_group_iteration(
        &mut self,
        start: usize,
        end: usize,
        sender: Sender<Option<Result<RecordBatch>>>,
    ) {
        let mut iter = SeriesGroupRowIterator {
            runtime: self.runtime.clone(),
            engine: self.engine.clone(),
            query_option: self.query_option.clone(),
            vnode_id: self.vnode_id,
            super_version: self.super_version.clone(),
            series_ids: self.series_ids.clone(),
            start,
            end,
            batch_size: self.query_option.batch_size,
            i: start,
            columns: Vec::with_capacity(self.query_option.table_schema.columns().len()),
            is_finished: false,
            span_recorder: self
                .span_recorder
                .child(format!("SeriesGroupRowIterator [{}, {})", start, end)),
            metrics: SeriesGroupRowIteratorMetrics::new(&self.metrics_set, start),
        };
        let can_tok = self.series_iter_closer.clone();
        self.runtime.spawn(async move {
            loop {
                tokio::select! {
                    _ = can_tok.cancelled() => {
                        break;
                    }
                    iter_ret = iter.next() => {
                        if let Some(ret) = iter_ret {
                            if sender.send(Some(ret)).await.is_err() {
                                return;
                            }
                        } else {
                            let _ = sender.send(None).await;
                            break;
                        }
                    }
                }
            }
        });
    }

    fn build_record_builders(query_option: &QueryOption) -> Result<Vec<ArrayBuilderPtr>> {
        // Get builders for aggregating.
        if let Some(aggregates) = query_option.aggregates.as_ref() {
            let mut builders: Vec<ArrayBuilderPtr> = Vec::with_capacity(aggregates.len());
            for _ in 0..aggregates.len() {
                builders.push(ArrayBuilderPtr::new(
                    Box::new(Int64Builder::with_capacity(query_option.batch_size)),
                    ColumnType::Field(ValueType::Integer),
                ));
            }
            return Ok(builders);
        }

        // Get builders for table scan.
        let mut builders: Vec<ArrayBuilderPtr> =
            Vec::with_capacity(query_option.table_schema.columns().len());
        for item in query_option.table_schema.columns().iter() {
            debug!(
                "Building record builder: schema info {:02X} {}",
                item.id, item.name
            );
            let builder_item =
                Self::new_column_builder(&item.column_type, query_option.batch_size)?;
            builders.push(ArrayBuilderPtr::new(builder_item, item.column_type.clone()))
        }
        Ok(builders)
    }

    fn new_column_builder(
        column_type: &ColumnType,
        batch_size: usize,
    ) -> Result<Box<dyn ArrayBuilder>> {
        Ok(match column_type {
            ColumnType::Tag => Box::new(StringBuilder::with_capacity(batch_size, batch_size * 32)),
            ColumnType::Time(unit) => match unit {
                TimeUnit::Second => Box::new(TimestampSecondBuilder::with_capacity(batch_size)),
                TimeUnit::Millisecond => {
                    Box::new(TimestampMillisecondBuilder::with_capacity(batch_size))
                }
                TimeUnit::Microsecond => {
                    Box::new(TimestampMicrosecondBuilder::with_capacity(batch_size))
                }
                TimeUnit::Nanosecond => {
                    Box::new(TimestampNanosecondBuilder::with_capacity(batch_size))
                }
            },
            ColumnType::Field(t) => match t {
                ValueType::Float => Box::new(Float64Builder::with_capacity(batch_size)),
                ValueType::Integer => Box::new(Int64Builder::with_capacity(batch_size)),
                ValueType::Unsigned => Box::new(UInt64Builder::with_capacity(batch_size)),
                ValueType::Boolean => Box::new(BooleanBuilder::with_capacity(batch_size)),
                ValueType::String => {
                    Box::new(StringBuilder::with_capacity(batch_size, batch_size * 32))
                }
                ValueType::Unknown => {
                    return Err(Error::CommonError {
                        reason: "failed to create column builder: unkown column type".to_string(),
                    })
                }
            },
        })
    }
}

impl RowIterator {
    pub async fn next(&mut self) -> Option<Result<RecordBatch>> {
        if self.is_finished {
            return None;
        }
        if self.series_ids.is_empty() {
            self.is_finished = true;
            // Build an empty result.
            // TODO record elapsed_point_to_record_batch
            // let timer = self.metrics.elapsed_point_to_record_batch().timer();
            let mut empty_builders = match Self::build_record_builders(self.query_option.as_ref()) {
                Ok(builders) => builders,
                Err(e) => return Some(Err(e)),
            };
            let mut empty_cols = vec![];
            for item in empty_builders.iter_mut() {
                empty_cols.push(item.ptr.finish())
            }
            let empty_result =
                RecordBatch::try_new(self.query_option.df_schema.clone(), empty_cols).map_err(
                    |err| Error::CommonError {
                        reason: format!("iterator fail, {}", err),
                    },
                );
            // timer.done();

            return Some(empty_result);
        }

        while let Some(ret) = self.series_iter_receiver.recv().await {
            match ret {
                Some(Ok(r)) => {
                    return Some(Ok(r));
                }
                Some(Err(e)) => {
                    self.series_iter_closer.cancel();
                    return Some(Err(e));
                }
                None => {
                    // Do nothing
                    debug!("One of series group iterator finished.");
                }
            }
        }

        None
    }
}

impl Drop for RowIterator {
    fn drop(&mut self) {
        if self.span_recorder.span_ctx().is_some() {
            let version_number = self.super_version.as_ref().map(|v| v.version_number);
            let ts_family_id = self.super_version.as_ref().map(|v| v.ts_family_id);

            self.span_recorder
                .set_metadata("version_number", format!("{version_number:?}"));
            self.span_recorder
                .set_metadata("ts_family_id", format!("{ts_family_id:?}"));
            self.span_recorder.set_metadata("vnode_id", self.vnode_id);
            self.span_recorder
                .set_metadata("series_ids_num", self.series_ids.len());

            let metrics = self
                .metrics_set
                .clone_inner()
                .aggregate_by_name()
                .sorted_for_display()
                .timestamps_removed();

            metrics.iter().for_each(|e| {
                self.span_recorder
                    .set_metadata(e.value().name().to_string(), e.value().to_string());
            });
        }
    }
}

struct SeriesGroupRowIterator {
    runtime: Arc<Runtime>,
    engine: EngineRef,
    query_option: Arc<QueryOption>,
    vnode_id: u32,
    super_version: Option<Arc<SuperVersion>>,
    series_ids: Arc<Vec<u32>>,
    start: usize,
    end: usize,
    batch_size: usize,

    /// The index of series_ids.
    i: usize,
    /// The temporary columns of the series_id.
    columns: Vec<CursorPtr>,
    /// Whether this iterator was finsihed.
    is_finished: bool,

    #[allow(unused)]
    span_recorder: SpanRecorder,
    metrics: SeriesGroupRowIteratorMetrics,
}

impl SeriesGroupRowIterator {
    pub async fn next(&mut self) -> Option<Result<RecordBatch>> {
        if self.is_finished {
            return None;
        }
        // record elapsed_point_to_record_batch
        let timer = self.metrics.elapsed_point_to_record_batch().timer();
        let mut builders = match RowIterator::build_record_builders(self.query_option.as_ref()) {
            Ok(builders) => builders,
            Err(e) => return Some(Err(e)),
        };
        timer.done();

        for _ in 0..self.batch_size {
            match self.fetch_next_row(&mut builders).await {
                Ok(Some(_)) => {}
                Ok(None) => {
                    self.is_finished = true;
                    break;
                }
                Err(err) => return Some(Err(err)),
            };
        }
        // record elapsed_point_to_record_batch
        let timer = self.metrics.elapsed_point_to_record_batch().timer();
        let result = {
            let mut cols = Vec::with_capacity(builders.len());
            for builder in builders.iter_mut() {
                cols.push(builder.ptr.finish())
            }

            match RecordBatch::try_new(self.query_option.df_schema.clone(), cols) {
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

impl SeriesGroupRowIterator {
    /// Try to fetch next row into array builders.
    ///
    /// If there is no remaning data to fetch, return Ok(None), otherwise return Ok(Some(())).
    async fn fetch_next_row(&mut self, builder: &mut [ArrayBuilderPtr]) -> Result<Option<()>> {
        if self.query_option.aggregates.is_some() {
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

    /// Try to fetch next series of this iterator.
    ///
    /// If series ids of this iterator are all consumed, return Ok(None),
    /// otherwise return Ok(Some(())).
    async fn next_series(&mut self) -> Result<Option<()>> {
        if self.i >= self.end {
            return Ok(None);
        }
        self.build_series_columns(self.series_ids[self.i]).await?;
        self.i += 1;

        Ok(Some(()))
    }

    /// Build cursors to read data.
    ///
    /// Get series key by series id, for each of columns in the given schema
    /// (may be Time, Tag of Field) build a Cursor.
    async fn build_series_columns(&mut self, series_id: SeriesId) -> Result<()> {
        let start = Instant::now();

        if let Some(key) = self
            .engine
            .get_series_key(
                &self.query_option.table_schema.tenant,
                &self.query_option.table_schema.db,
                self.vnode_id,
                series_id,
            )
            .await?
        {
            self.columns.clear();
            for item in self.query_option.table_schema.columns() {
                debug!(
                    "Building series columns: sid={:02X}, column={:?}",
                    series_id, item
                );
                let column_cursor: CursorPtr = match item.column_type {
                    ColumnType::Time(ref unit) => {
                        Box::new(TimeCursor::new(0, item.name.clone(), unit.clone()))
                    }

                    ColumnType::Tag => {
                        let tag_val = key.tag_val(&item.name);
                        Box::new(TagCursor::new(item.name.clone(), tag_val))
                    }

                    ColumnType::Field(vtype) => match vtype {
                        ValueType::Unknown => {
                            error!("Unknown field type of column {}", &item.name);
                            todo!("return an empty-cursor for unknown type field");
                        }
                        _ => {
                            let cursor = self
                                .build_field_cursor(
                                    unite_id(item.id, series_id),
                                    item.name.clone(),
                                    vtype,
                                )
                                .await?;
                            Box::new(cursor)
                        }
                    },
                };

                self.columns.push(column_cursor);
            }
        }

        // Record elapsed_series_scan
        self.metrics
            .elapsed_series_scan()
            .add_duration(start.elapsed());

        Ok(())
    }

    /// Build a FieldCursor with cached data and file locations.
    async fn build_field_cursor(
        &self,
        field_id: FieldId,
        field_name: String,
        field_type: ValueType,
    ) -> Result<FieldCursor> {
        let super_version = match self.super_version {
            Some(ref v) => v.clone(),
            None => return Ok(FieldCursor::empty(field_type, field_name)),
        };

        let time_ranges_ref = self.query_option.split.time_ranges();
        let time_predicate = |ts| time_ranges_ref.is_boundless() || time_ranges_ref.contains(ts);
        debug!("Pushed down time range filter: {:?}", time_ranges_ref);

        let timer = self.metrics.elapsed_get_data_from_memcache().timer();

        // Get data from im_memcache and memcache
        let mut cache_data: Vec<DataType> = Vec::new();
        super_version.caches.read_field_data(
            field_id,
            time_predicate,
            |_| true,
            |d| cache_data.push(d),
        );
        cache_data.sort_by_key(|data| data.timestamp());

        timer.done();

        debug!(
            "build memcache data id: {:02X}, len: {}",
            field_id,
            cache_data.len()
        );

        let timer = self.metrics.elapsed_get_field_location().timer();

        // Get data from level info, find time range overlapped files and file locations.
        // TODO: Init locations in parallel with other fields.
        let mut locations = vec![];
        for level in super_version.version.levels_info.iter().rev() {
            if !time_ranges_ref.overlaps(&level.time_range) {
                continue;
            }
            for file in level.files.iter() {
                if !time_ranges_ref.overlaps(file.time_range()) || !file.contains_field_id(field_id)
                {
                    continue;
                }
                let path = file.file_path();
                debug!(
                    "Building FieldCursor: field: {:02X}, path: '{}'",
                    field_id,
                    path.display()
                );

                if !path.is_file() {
                    return Err(Error::TsmFileBroken {
                        source: crate::tsm::ReadTsmError::FileNotFound {
                            reason: format!("File Not Found: {}", path.display()),
                        },
                    });
                }

                let tsm_reader = super_version.version.get_tsm_reader(path).await?;
                for idx_meta in tsm_reader.index_iterator_opt(field_id) {
                    let location = FieldFileLocation::new(
                        tsm_reader.clone(),
                        time_ranges_ref.clone(),
                        idx_meta.block_iterator_opt(time_ranges_ref.clone()),
                        field_type,
                    );
                    locations.push(location);
                }
            }
        }
        debug!("Building FieldCursor: locations: {:?}", &locations);
        timer.done();

        Ok(FieldCursor {
            name: field_name,
            value_type: field_type,
            cache_index: 0,
            cache_data,
            locations,
        })
    }

    async fn collect_row_data(&mut self, builder: &mut [ArrayBuilderPtr]) -> Result<Option<()>> {
        trace::trace!("======collect_row_data=========");
        // Record elapsed_field_scan
        let timer = self.metrics.elapsed_field_scan().timer();

        let mut min_time = i64::MAX;
        let mut row_cols = Vec::with_capacity(self.columns.len());
        // For each column, peek next (timestamp, value), set column_values, and
        // specify the next min_time (if column is a `Field`).
        for col_cursor in self.columns.iter_mut() {
            let ts_val = col_cursor.peek().await?;
            if let Some(ref d) = ts_val {
                if col_cursor.is_field() {
                    min_time = min_num(min_time, d.timestamp());
                }
            }
            row_cols.push(ts_val)
        }

        // For the specified min_time, fill each column data.
        // If a column data is for later time, just set it None.
        let mut test_collected_col_num = 0_usize;
        for (col_cursor, ts_val) in self.columns.iter_mut().zip(row_cols.iter_mut()) {
            trace::trace!("field: {}, value: {:?}", col_cursor.name(), ts_val);
            if !col_cursor.is_field() {
                continue;
            }

            if let Some(d) = ts_val {
                let ts = d.timestamp();
                if ts == min_time {
                    test_collected_col_num += 1;
                    col_cursor.next(ts).await;
                } else {
                    *ts_val = None;
                }
            }
        }

        // Step field_scan completed.
        timer.done();
        trace::trace!(
            "Collected data, series_id: {}, column count: {test_collected_col_num}, timestamp: {min_time}",
            self.series_ids[self.i - 1],
        );
        if min_time == i64::MAX {
            // If peeked no data, return.
            self.columns.clear();
            return Ok(None);
        }

        // Record elapsed_point_to_record_batch
        let timer = self.metrics.elapsed_point_to_record_batch().timer();

        for (i, value) in row_cols.into_iter().enumerate() {
            match self.columns[i].column_type() {
                ColumnType::Time(unit) => {
                    builder[i].append_timestamp(&unit, min_time);
                }
                ColumnType::Tag => {
                    builder[i].append_value(ValueType::String, value, self.columns[i].name())?;
                }
                ColumnType::Field(value_type) => {
                    builder[i].append_value(value_type, value, self.columns[i].name())?;
                }
            }
        }

        timer.done();

        Ok(Some(()))
    }

    async fn collect_aggregate_row_data(
        &mut self,
        builder: &mut [ArrayBuilderPtr],
    ) -> Result<Option<()>> {
        if self.is_finished {
            return Ok(None);
        }
        self.is_finished = true;
        trace::trace!("======collect_aggregate_row_data=========");
        match (
            self.super_version.as_ref(),
            self.query_option.aggregates.as_ref(),
        ) {
            (Some(version), Some(aggregates)) => {
                for (i, item) in aggregates.iter().enumerate() {
                    match item.column_type {
                        ColumnType::Tag => todo!("collect count for tag"),
                        ColumnType::Time(_) => {
                            let agg_ret = count_column_non_null_values(
                                self.runtime.clone(),
                                version.clone(),
                                self.series_ids.clone(),
                                None,
                                self.query_option.split.time_ranges(),
                            )
                            .await?;
                            builder[i].append_primitive::<Int64Type>(agg_ret as i64);
                        }
                        ColumnType::Field(vtype) => match vtype {
                            ValueType::Unknown => {
                                return Err(Error::CommonError {
                                    reason: format!("unknown type of {}", item.name),
                                });
                            }
                            _ => {
                                let agg_ret = count_column_non_null_values(
                                    self.runtime.clone(),
                                    version.clone(),
                                    self.series_ids.clone(),
                                    Some(item.id),
                                    self.query_option.split.time_ranges(),
                                )
                                .await?;
                                builder[i].append_primitive::<Int64Type>(agg_ret as i64);
                            }
                        },
                    };
                }

                Ok(Some(()))
            }
            _ => Ok(None),
        }
    }
}

#[cfg(test)]
mod test {
    #[test]
    fn test_field_cursor() {
        // TODO: Test multi-level contains the same timestamp with different values.
    }
}
