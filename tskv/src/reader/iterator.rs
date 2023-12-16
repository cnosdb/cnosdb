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
use models::field_value::DataType;
use models::meta_data::VnodeId;
use models::predicate::domain::{self, QueryArgs, QueryExpr};
use models::predicate::PlacedSplit;
use models::schema::{PhysicalCType as ColumnType, TableColumn, TskvTableSchemaRef};
use models::{PhysicalDType as ValueType, SeriesId, Timestamp};
use protos::kv_service::QueryRecordBatchRequest;
use tokio::runtime::Runtime;
use tokio::sync::mpsc::Receiver;
use tokio_util::sync::CancellationToken;
use trace::{debug, error, SpanRecorder};

use crate::error::Result;
use crate::reader::Cursor;
use crate::tseries_family::SuperVersion;
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
            ValueType::String => match value {
                Some(DataType::Str(_, val)) => {
                    // Safety
                    // All val is valid UTF-8 String
                    let str = unsafe { std::str::from_utf8_unchecked(val.as_slice()) };
                    self.append_string(str)
                }
                _ => self.append_null_string(),
            },
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

    pub fn append_string(&mut self, data: &str) {
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
    elapsed_series_scan: metrics::Time,
    elapsed_build_resp_stream: metrics::Time,
    elapsed_get_data_from_memcache: metrics::Time,
    elapsed_get_field_location: metrics::Time,
    elapsed_collect_row_time: metrics::Time,
    elapsed_collect_aggregate_time: metrics::Time,
}

impl SeriesGroupRowIteratorMetrics {
    /// Create new metrics
    pub fn new(metrics: &ExecutionPlanMetricsSet, partition: usize) -> Self {
        let elapsed_series_scan =
            MetricBuilder::new(metrics).subset_time("elapsed_series_scan", partition);

        let elapsed_build_resp_stream =
            MetricBuilder::new(metrics).subset_time("elapsed_build_resp_stream", partition);

        let elapsed_get_data_from_memcache =
            MetricBuilder::new(metrics).subset_time("elapsed_get_data_from_memcache", partition);

        let elapsed_get_field_location =
            MetricBuilder::new(metrics).subset_time("elapsed_get_field_location", partition);

        let elapsed_collect_row_time =
            MetricBuilder::new(metrics).subset_time("elapsed_collect_row_time", partition);

        let elapsed_collect_aggregate_time =
            MetricBuilder::new(metrics).subset_time("elapsed_collect_aggregate_time", partition);

        Self {
            elapsed_series_scan,
            elapsed_build_resp_stream,
            elapsed_get_data_from_memcache,
            elapsed_get_field_location,
            elapsed_collect_row_time,
            elapsed_collect_aggregate_time,
        }
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

    pub fn elapsed_collect_row_time(&self) -> &metrics::Time {
        &self.elapsed_collect_row_time
    }

    pub fn elapsed_collect_aggregate_time(&self) -> &metrics::Time {
        &self.elapsed_collect_aggregate_time
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
            let kv_dt = item.column_type.to_physical_type();
            let builder_item = Self::new_column_builder(&kv_dt, query_option.batch_size)?;
            builders.push(ArrayBuilderPtr::new(builder_item, kv_dt))
        }
        Ok(builders)
    }

    pub fn new_column_builder(
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
        self.series_iter_closer.cancel();

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

#[cfg(test)]
mod test {
    #[test]
    fn test_field_cursor() {
        // TODO: Test multi-level contains the same timestamp with different values.
    }
}
