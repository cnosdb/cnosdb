use std::sync::Arc;
use std::task::Poll;

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
use datafusion::error::DataFusionError;
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryPool, MemoryReservation};
use datafusion::physical_plan::metrics::{
    self, BaselineMetrics, ExecutionPlanMetricsSet, MetricBuilder,
};
use minivec::MiniVec;
use models::meta_data::VnodeId;
use models::predicate::domain::{self, QueryArgs, QueryExpr, TimeRange};
use models::predicate::Split;
use models::schema::{ColumnType, TableColumn, TskvTableSchema};
use models::utils::{min_num, unite_id};
use models::{FieldId, SeriesId, Timestamp, ValueType};
use parking_lot::RwLock;
use protos::kv_service::QueryRecordBatchRequest;
use tokio::runtime::Runtime;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use trace::{debug, error};

use crate::compute::count::count_column_non_null_values;
use crate::error::Result;
use crate::memcache::DataType;
use crate::query_iterator::Cursor;
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
            b.extend(a.into_iter())
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
    pub split: Split,
    pub df_schema: SchemaRef,
    pub table_schema: TskvTableSchema,
    pub metrics: TskvSourceMetrics,
    pub aggregates: Option<Vec<TableColumn>>, // TODO: Use PushedAggregateFunction
}

impl QueryOption {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        batch_size: usize,
        split: Split,
        aggregates: Option<Vec<TableColumn>>, // TODO: Use PushedAggregateFunction
        df_schema: SchemaRef,
        table_schema: TskvTableSchema,
        metrics: TskvSourceMetrics,
    ) -> Self {
        Self {
            batch_size,
            split,
            aggregates,
            df_schema,
            table_schema,
            metrics,
        }
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
    block_it: BlockMetaIterator,
    time_range: TimeRange,
    read_index: usize,
    end_index: usize,
    data_block: DataBlock,
}

impl FieldFileLocation {
    pub fn new(
        reader: Arc<TsmReader>,
        time_range: TimeRange,
        block_it: BlockMetaIterator,
        vtype: ValueType,
    ) -> Self {
        Self {
            reader,
            block_it,
            time_range,
            // make read_index > end_index,  when init
            read_index: 1,
            end_index: 0,
            data_block: DataBlock::new(0, vtype),
        }
    }

    pub async fn peek(&mut self) -> Result<Option<DataType>> {
        while self.read_index > self.end_index {
            // let data = self.data_block.get(self.read_index);
            if let Some(meta) = self.block_it.next() {
                self.data_block = self.reader.get_data_block(&meta).await?;
                if let Some(time_range) = self.data_block.time_range() {
                    let tr = TimeRange::from(time_range);
                    let ts = self.time_range.intersect(&tr).unwrap();
                    if let Some((min, max)) = self.data_block.index_range(&ts) {
                        self.read_index = min;
                        self.end_index = max;
                        break;
                    }
                }
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
    metrics: TskvSourceMetrics,

    /// Super version of vnode_id, maybe None.
    super_version: Option<Arc<SuperVersion>>,
    /// List of series id filtered from engine.
    series_ids: Arc<Vec<SeriesId>>,
    series_iter_receiver: Receiver<Option<Result<RecordBatch>>>,
    series_iter_closer: CancellationToken,
    /// Whether this iterator was finsihed.
    is_finished: bool,
}

impl RowIterator {
    pub async fn new(
        runtime: Arc<Runtime>,
        engine: EngineRef,
        query_option: QueryOption,
        vnode_id: VnodeId,
    ) -> Result<Self> {
        let super_version = engine
            .get_db_version(
                &query_option.table_schema.tenant,
                &query_option.table_schema.db,
                vnode_id,
            )
            .await?;

        let series_ids = engine
            .get_series_id_by_filter(
                &query_option.table_schema.tenant,
                &query_option.table_schema.db,
                &query_option.table_schema.name,
                vnode_id,
                query_option.split.tags_filter(),
            )
            .await?;

        debug!(
            "Iterating rows: vnode_id={}, serie_ids_count={}",
            vnode_id,
            series_ids.len()
        );
        let query_option = Arc::new(query_option);
        let metrics = query_option.metrics.clone();
        let series_len = series_ids.len();
        let (tx, rx) = channel(1);
        if query_option.aggregates.is_some() {
            // TODO: Correct the aggregate columns order.
            let mut row_iterator = Self {
                runtime,
                engine,
                query_option,
                vnode_id,
                metrics,
                super_version,
                series_ids: Arc::new(series_ids),
                series_iter_receiver: rx,
                series_iter_closer: CancellationToken::new(),
                is_finished: false,
            };
            row_iterator.new_series_group_iteration(0, series_len, tx);

            Ok(row_iterator)
        } else {
            // TODO：get series_group_size more intelligently
            let series_group_size = num_cpus::get();
            // `usize::div_ceil(self, Self)` is now unstable, so do it by-hand.
            let series_group_num = series_len / series_group_size
                + if series_len % series_group_size == 0 {
                    0
                } else {
                    1
                };

            let mut row_iterator = Self {
                runtime,
                engine,
                query_option,
                vnode_id,
                metrics,
                super_version,
                series_ids: Arc::new(series_ids),
                series_iter_receiver: rx,
                series_iter_closer: CancellationToken::new(),
                is_finished: false,
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

    pub async fn verify_file_integrity(&self) -> Result<()> {
        let super_version = match self.super_version {
            Some(ref v) => v.clone(),
            None => return Ok(()),
        };

        for lv in super_version.version.levels_info.iter().rev() {
            for cf in lv.files.iter() {
                let path = cf.file_path();
                if !path.is_file() {
                    return Err(Error::ReadTsm {
                        source: crate::tsm::ReadTsmError::FileNotFound {
                            reason: format!("File Not Found: {}", path.display()),
                        },
                    });
                }
            }
        }

        Ok(())
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
            metrics: self.metrics.clone(),
            super_version: self.super_version.clone(),
            series_ids: self.series_ids.clone(),
            start,
            end,
            batch_size: self.query_option.batch_size,
            i: start,
            columns: Vec::with_capacity(self.query_option.table_schema.columns().len()),
            is_finished: false,
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
            let timer = self.metrics.elapsed_point_to_record_batch().timer();
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
            timer.done();

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

struct SeriesGroupRowIterator {
    runtime: Arc<Runtime>,
    engine: EngineRef,
    query_option: Arc<QueryOption>,
    vnode_id: u32,
    metrics: TskvSourceMetrics,
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
}

impl SeriesGroupRowIterator {
    pub async fn next(&mut self) -> Option<Result<RecordBatch>> {
        if self.is_finished {
            return None;
        }

        let timer = self.metrics.elapsed_point_to_record_batch().timer();
        let mut builders = match RowIterator::build_record_builders(self.query_option.as_ref()) {
            Ok(builders) => builders,
            Err(e) => return Some(Err(e)),
        };
        timer.done();

        for _ in 0..self.batch_size {
            match self.next_row(&mut builders).await {
                Ok(Some(_)) => {}
                Ok(None) => {
                    self.is_finished = true;
                    break;
                }
                Err(err) => return Some(Err(err)),
            };
        }

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
    async fn next_row(&mut self, builder: &mut [ArrayBuilderPtr]) -> Result<Option<()>> {
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

    async fn next_series(&mut self) -> Result<Option<()>> {
        if self.i >= self.end {
            return Ok(None);
        }
        self.build_series_columns(self.series_ids[self.i]).await?;
        self.i += 1;

        Ok(Some(()))
    }

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
                let column: CursorPtr = match item.column_type {
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

                self.columns.push(column);
            }
        }

        self.metrics
            .elapsed_series_scan()
            .add_duration(start.elapsed());

        Ok(())
    }

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

        let time_range = *self.query_option.split.time_range();
        let time_predicate = |ts| time_range.is_boundless() || time_range.contains(ts);
        debug!("Pushed time range filter: {:?}", time_range);

        // get data from im_memcache and memcache
        let mut cache_data: Vec<DataType> = Vec::new();
        super_version.caches.read_field_data(
            field_id,
            time_predicate,
            |_| true,
            |d| cache_data.push(d),
        );
        cache_data.sort_by_key(|data| data.timestamp());

        debug!(
            "build memcache data id: {:02X}, len: {}",
            field_id,
            cache_data.len()
        );

        // get data from levelinfo
        // TODO: Init locations in parallel with other fields.
        let mut locations = vec![];
        for lv in super_version.version.levels_info.iter().rev() {
            if !lv.time_range.overlaps(&time_range) {
                continue;
            }
            for cf in lv.files.iter() {
                if !cf.overlap(&time_range) || !cf.contains_field_id(field_id) {
                    continue;
                }
                let path = cf.file_path();
                debug!(
                    "building FieldCursor({:02X}) at '{}'",
                    field_id,
                    path.display()
                );

                let tsm_reader = super_version.version.get_tsm_reader(path).await?;
                for idx in tsm_reader.index_iterator_opt(field_id) {
                    let location = FieldFileLocation::new(
                        tsm_reader.clone(),
                        time_range,
                        idx.block_iterator_opt(&time_range),
                        field_type,
                    );
                    locations.push(location);
                }
            }
        }

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
            trace::trace!("field: {}, value: {:?}", column.name(), value);
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

        trace::trace!("min time: {}", min_time);
        if min_time == i64::MAX {
            self.columns.clear();
            return Ok(None);
        }

        let timer = self.metrics.elapsed_point_to_record_batch().timer();

        for (i, value) in values.into_iter().enumerate() {
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
                                *self.query_option.split.time_range(),
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
                                    *self.query_option.split.time_range(),
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
