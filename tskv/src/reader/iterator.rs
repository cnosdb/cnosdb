use std::iter;
use std::ops::Not;
use std::sync::Arc;

use arrow::datatypes::{Schema, SchemaRef};
use datafusion::arrow::array::{
    ArrayBuilder, BooleanBuilder, Float64Builder, Int64Builder, PrimitiveBuilder, StringBuilder,
    TimestampMicrosecondBuilder, TimestampMillisecondBuilder, TimestampNanosecondBuilder,
    TimestampSecondBuilder, UInt64Builder,
};
use datafusion::arrow::datatypes::{
    ArrowPrimitiveType, Float64Type, Int64Type, TimeUnit, TimestampMicrosecondType,
    TimestampMillisecondType, TimestampNanosecondType, TimestampSecondType, UInt64Type,
};
use datafusion::physical_plan::metrics::{self, ExecutionPlanMetricsSet, MetricBuilder};
use datafusion_proto::physical_plan::from_proto::parse_physical_expr;
use models::field_value::DataType;
use models::meta_data::VnodeId;
use models::predicate::domain::{self, QueryArgs, QueryExpr, TimeRanges};
use models::predicate::PlacedSplit;
use models::schema::tskv_table_schema::{PhysicalCType, TableColumn, TskvTableSchemaRef};
use models::{ColumnId, PhysicalDType, SeriesId, SeriesKey, Timestamp};
use protos::kv_service::QueryRecordBatchRequest;
use snafu::ResultExt;
use tokio::runtime::Runtime;
use tokio_util::sync::CancellationToken;
use trace::span_ext::SpanExt;
use trace::{debug, error, Span, SpanContext};

use super::display::DisplayableBatchReader;
use super::memcache_reader::MemCacheReader;
use super::merge::DataMerger;
use super::series::SeriesReader;
use super::trace::Recorder;
use super::{
    DataReference, EmptySchemableTskvRecordBatchStream, Predicate, PredicateRef, Projection,
    SendableTskvRecordBatchStream,
};
use crate::error::{CommonSnafu, SchemaSnafu, TskvResult};
use crate::reader::chunk::filter_column_groups;
use crate::reader::column_group::ColumnGroupReader;
use crate::reader::filter::DataFilter;
use crate::reader::function_register::NoRegistry;
use crate::reader::paralle_merge::ParallelMergeAdapter;
use crate::reader::schema_alignmenter::SchemaAlignmenter;
use crate::reader::trace::TraceCollectorBatcherReaderProxy;
use crate::reader::utils::group_overlapping_segments;
use crate::reader::{BatchReaderRef, CombinedBatchReader};
use crate::schema::error::{ColumnNotFoundSnafu, SchemaResult};
use crate::tseries_family::{CacheGroup, ColumnFile, SuperVersion};
use crate::tsm::reader::TsmReader;
use crate::EngineRef;

pub struct ArrayBuilderPtr {
    pub ptr: Box<dyn ArrayBuilder>,
    pub column_type: PhysicalCType,
}

impl ArrayBuilderPtr {
    pub fn new(ptr: Box<dyn ArrayBuilder>, column_type: PhysicalCType) -> Self {
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
        value_type: PhysicalDType,
        value: Option<DataType>,
        column_name: &str,
    ) -> TskvResult<()> {
        match value_type {
            PhysicalDType::Unknown => {
                return Err(CommonSnafu {
                    reason: format!("unknown type of column '{}'", column_name),
                }
                .build());
            }
            PhysicalDType::String => match value {
                Some(DataType::Str(_, val)) => {
                    // Safety
                    // All val is valid UTF-8 String
                    let str = unsafe { std::str::from_utf8_unchecked(val.as_slice()) };
                    self.append_string(str)
                }
                _ => self.append_null_string(),
            },
            PhysicalDType::Boolean => {
                if let Some(DataType::Bool(_, val)) = value {
                    self.append_bool(val);
                } else {
                    self.append_null_bool();
                }
            }
            PhysicalDType::Float => {
                if let Some(DataType::F64(_, val)) = value {
                    self.append_primitive::<Float64Type>(val);
                } else {
                    self.append_primitive_null::<Float64Type>();
                }
            }
            PhysicalDType::Integer => {
                if let Some(DataType::I64(_, val)) = value {
                    self.append_primitive::<Int64Type>(val);
                } else {
                    self.append_primitive_null::<Int64Type>();
                }
            }
            PhysicalDType::Unsigned => {
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
}

pub struct SeriesGroupBatchReaderFactory {
    engine: EngineRef,
    query_option: QueryOption,
    super_version: Arc<SuperVersion>,

    span: Span,
    metrics_set: ExecutionPlanMetricsSet,
    series_reader_metrics_set: Arc<ExecutionPlanMetricsSet>,
    column_group_reader_metrics_set: Arc<ExecutionPlanMetricsSet>,
    filter_reader_metrics_set: Arc<ExecutionPlanMetricsSet>,
    merge_reader_metrics_set: Arc<ExecutionPlanMetricsSet>,
    schema_align_reader_metrics_set: Arc<ExecutionPlanMetricsSet>,
}

impl SeriesGroupBatchReaderFactory {
    pub fn new(
        engine: EngineRef,
        query_option: QueryOption,
        super_version: Arc<SuperVersion>,
        span: Span,
        metrics_set: ExecutionPlanMetricsSet,
    ) -> Self {
        Self {
            engine,
            query_option,
            super_version,
            span,
            metrics_set,
            series_reader_metrics_set: Arc::new(ExecutionPlanMetricsSet::new()),
            column_group_reader_metrics_set: Arc::new(ExecutionPlanMetricsSet::new()),
            filter_reader_metrics_set: Arc::new(ExecutionPlanMetricsSet::new()),
            merge_reader_metrics_set: Arc::new(ExecutionPlanMetricsSet::new()),
            schema_align_reader_metrics_set: Arc::new(ExecutionPlanMetricsSet::new()),
        }
    }

    pub fn schema(&self) -> SchemaRef {
        self.query_option.df_schema.clone()
    }

    /// ParallelMergeAdapter: schema=[{}]                               -------- 并行执行多个 stream
    ///   SchemaAlignmenter: schema=[{}]                                -------- 用 Null 值补齐缺失的 tag 列
    ///    SeriesReader: sid={}, schema=[{}]                            -------- 根据 series key 补齐对应的 tag 列
    ///      DataMerger: schema=[{}]                                  -------- 合并相同 series 下时间段重叠的chunk数据
    ///        SchemaAlignmenter:                                         -------- 用 Null 值补齐缺失的 Field 列
    ///          DataFilter: expr=[{}], schema=[{}]                     -------- 根据下推的过滤条件精确过滤数据
    ///            MemRowGroup/ChunkReader: sid={}, projection=[{}], schema=[{}]    -------- 读取单个chunk或memcache rowgroup的数据的指定列的数据
    ///          DataFilter:
    ///            MemRowGroup/ChunkReader [PageReader]
    ///          ......
    ///      DataMerger: schema=[{}]
    ///        SchemaAlignmenter:
    ///          DataFilter: expr=[{}], schema=[{}]
    ///            MemRowGroup/ChunkReader: sid={}, projection=[{}], schema=[{}]
    ///          DataFilter
    ///            MemRowGroup/ChunkReader [PageReader]
    ///          ......
    ///        ......
    ///  SchemaAlignmenter: schema=[{}]                                                
    ///    SeriesReader: sid={}, schema=[{}]
    ///      DataMerger: schema=[{}]
    ///        SchemaAlignmenter:
    ///          DataFilter: expr=[{}], schema=[{}]
    ///            MemRowGroup/ChunkReader: sid={}, projection=[{}], schema=[{}]
    ///          DataFilter
    ///            MemRowGroup/ChunkReader [PageReader]
    ///          ......
    ///      ......
    ///    ......
    pub async fn create(
        &self,
        span: Span,
        series_ids: &[u32],
        predicate: Option<PredicateRef>,
    ) -> TskvResult<Option<BatchReaderRef>> {
        let metrics = SeriesGroupBatchReaderMetrics::new(
            &self.metrics_set,
            self.super_version.ts_family_id as usize,
        );

        let _timer = metrics.elapsed_build_batch_reader_time().timer();

        if series_ids.is_empty() {
            return Ok(None);
        }

        // 采集读取的 series 数量
        metrics.series_nums().set(series_ids.len());

        let kv_schema = &self.query_option.table_schema;
        let schema = &self.query_option.df_schema;
        // TODO 投影中一定包含 time 列，后续优化掉
        let time_fields_schema = project_time_fields(kv_schema, schema).context(SchemaSnafu)?;

        let super_version = &self.super_version;
        let vnode_id = super_version.ts_family_id;
        // TODO time column id 需要从上面传下来，当前schema中一定包含time列，所以这里写死为0
        let projection = Projection::from_schema(kv_schema.as_ref(), 0);
        let time_ranges = self.query_option.split.time_ranges();
        let column_files = super_version
            .column_files_by_sid_and_time(series_ids, time_ranges.as_ref())
            .await?;

        // 采集过滤后的文件数量
        metrics
            .file_nums_filtered_by_time_range()
            .set(column_files.len());

        // 获取所有的文件的 reader
        let mut column_files_with_reader = Vec::with_capacity(column_files.len());
        {
            let _timer = metrics.elapsed_get_tsm_readers_time().timer();
            for f in column_files {
                let reader = super_version.version.get_tsm_reader(f.file_path()).await?;
                column_files_with_reader.push((f, reader));
            }
        }

        // 通过sid获取serieskey
        let sid_keys = {
            let _timer = metrics.elapsed_get_series_keys_time().timer();
            self.series_keys(vnode_id, series_ids).await?
        };

        // 获取所有的符合条件的chunk Vec<(SeriesKey, Vec<DataReference>)>
        let mut series_chunk_readers = Vec::with_capacity(series_ids.len());
        for (sid, series_key) in series_ids.iter().zip(sid_keys) {
            // 选择含有series的所有chunk Vec<DataReference::Chunk(chunk, reader)>
            let mut chunks = Self::filter_chunks(&column_files_with_reader, *sid).await?;
            // 获取所有符合条件的 memcache rowgroup Vec<DataReference::Memcache(rowgroup)>)
            chunks.append(
                Self::filter_rowgroups(super_version.caches.clone(), *sid, time_ranges.clone())
                    .await?
                    .as_mut(),
            );
            // 按时间范围过滤chunk reader
            chunks.retain(|d| {
                d.time_range().is_none().not() && time_ranges.overlaps(&d.time_range())
            });
            series_chunk_readers.push((series_key, chunks));
        }

        metrics
            .chunk_nums()
            .set(series_chunk_readers.iter().map(|(_, e)| e.len()).sum());

        let series_readers = series_chunk_readers
            .into_iter()
            .filter_map(|(series_key, chunks)| {
                self.build_series_reader(
                    series_key,
                    chunks,
                    self.query_option.batch_size,
                    self.query_option.table_schema.clone(),
                    &projection,
                    &predicate,
                    schema.clone(),
                    time_fields_schema.clone(),
                    &metrics,
                )
                .transpose()
            })
            .collect::<TskvResult<Vec<_>>>()?;

        if series_readers.is_empty() {
            return Ok(None);
        }

        let limit = self.query_option.split.limit();

        // 设置并行度为cpu逻辑核心数
        // TODO 可配置
        let readers = series_readers
            .chunks((series_readers.len() + num_cpus::get()) / num_cpus::get())
            .map(|readers| Arc::new(CombinedBatchReader::new(readers.to_vec())) as BatchReaderRef)
            .collect::<Vec<_>>();
        let reader = Arc::new(ParallelMergeAdapter::try_new(
            schema.clone(),
            readers,
            limit,
        )?);

        // 添加收集trace信息的reader
        let reader = Arc::new(
            TraceCollectorBatcherReaderProxy::new(reader, span)
                .register_metrics_set("series_reader", self.series_reader_metrics_set.clone())
                .register_metrics_set(
                    "column_group_reader",
                    self.column_group_reader_metrics_set.clone(),
                )
                .register_metrics_set("filter_reader", self.filter_reader_metrics_set.clone())
                .register_metrics_set("merge_reader", self.merge_reader_metrics_set.clone())
                .register_metrics_set(
                    "schema_align_reader",
                    self.schema_align_reader_metrics_set.clone(),
                ),
        );

        trace::trace!(
            "Final batch reader tree: \n{}",
            DisplayableBatchReader::new(reader.as_ref()).indent()
        );

        Ok(Some(reader))
    }

    /// 返回指定series的serieskey
    async fn series_keys(
        &self,
        vnode_id: VnodeId,
        series_ids: &[SeriesId],
    ) -> TskvResult<Vec<SeriesKey>> {
        // 通过sid获取serieskey
        let sid_keys = self
            .engine
            .get_series_key(
                &self.query_option.table_schema.tenant,
                &self.query_option.table_schema.db,
                &self.query_option.table_schema.name,
                vnode_id,
                series_ids,
            )
            .await?;

        Ok(sid_keys)
    }

    /// 从给定的文件列表中选择含有指定series的所有chunk及其对应的TsmReader
    async fn filter_chunks(
        column_files: &[(Arc<ColumnFile>, Arc<TsmReader>)],
        sid: SeriesId,
    ) -> TskvResult<Vec<DataReference>> {
        // 选择含有series的所有文件
        let mut files = Vec::new();
        for (cf, reader) in column_files {
            if cf.maybe_contains_series_id(sid).await? {
                files.push((cf.clone(), reader.clone()));
            }
        }
        // 选择含有series的所有chunk
        let mut chunks = Vec::with_capacity(files.len());
        for (_, reader) in files {
            let chunk = reader.chunk().get(&sid);
            match chunk {
                None => continue,
                Some(chunk) => {
                    chunks.push(DataReference::Chunk(chunk.clone(), reader.clone()));
                }
            }
        }

        Ok(chunks)
    }

    /// filter rowgroup by sid
    async fn filter_rowgroups(
        caches: CacheGroup,
        sid: SeriesId,
        time_ranges: Arc<TimeRanges>,
    ) -> TskvResult<Vec<DataReference>> {
        let mut rowgroups = Vec::new();
        for cache in caches
            .immut_cache
            .iter()
            .chain(iter::once(&caches.mut_cache))
        {
            if let Some(series) = cache.read().read_series_data_by_id(sid) {
                if let Some(new_time_ranges) = time_ranges.intersect(&series.read().range) {
                    rowgroups.push(DataReference::Memcache(
                        series.clone(),
                        Arc::new(new_time_ranges),
                    ))
                }
            }
        }

        Ok(rowgroups)
    }

    fn build_chunk_reader(
        &self,
        chunk: DataReference,
        batch_size: usize,
        projection: &[ColumnId],
        predicate: &Option<Arc<Predicate>>,
        metrics: &SeriesGroupBatchReaderMetrics,
    ) -> TskvResult<Option<BatchReaderRef>> {
        let chunk_reader: Option<BatchReaderRef> = match chunk {
            DataReference::Chunk(chunk, reader) => {
                let chunk_schema = chunk.schema();
                let cgs = chunk.column_group().values().cloned().collect::<Vec<_>>();
                // filter column groups
                metrics.column_group_nums().add(cgs.len());
                trace::debug!("All column group nums: {}", cgs.len());
                let cgs = filter_column_groups(cgs, predicate, chunk_schema)?;
                trace::debug!("Filtered column group nums: {}", cgs.len());
                metrics.filtered_column_group_nums().add(cgs.len());

                let batch_readers = cgs
                    .into_iter()
                    .map(|e| {
                        let column_group_reader = ColumnGroupReader::try_new(
                            reader.clone(),
                            chunk.series_id(),
                            e,
                            projection,
                            batch_size,
                            self.column_group_reader_metrics_set.clone(),
                        )?;
                        Ok(Arc::new(column_group_reader) as BatchReaderRef)
                    })
                    .collect::<TskvResult<Vec<_>>>()?;

                Some(Arc::new(CombinedBatchReader::new(batch_readers)))
            }
            DataReference::Memcache(series_data, time_ranges) => {
                MemCacheReader::try_new(series_data, time_ranges, batch_size, projection)?
                    .map(|e| e as BatchReaderRef)
            }
        };

        // 数据过滤
        if let Some(predicate) = &predicate {
            if let Some(chunk_reader) = chunk_reader {
                return Ok(Some(Arc::new(DataFilter::new(
                    predicate.clone(),
                    chunk_reader,
                    self.filter_reader_metrics_set.clone(),
                ))));
            }
        }

        Ok(chunk_reader)
    }

    fn build_chunk_readers(
        &self,
        chunks: Vec<DataReference>,
        batch_size: usize,
        projection: &Projection,
        predicate: &Option<Arc<Predicate>>,
        metrics: &SeriesGroupBatchReaderMetrics,
    ) -> TskvResult<Vec<BatchReaderRef>> {
        let projection = if chunks.len() > 1 {
            // 需要进行合并去重，所以必须含有time列
            projection.fields_with_time()
        } else {
            projection.fields()
        };

        let mut chunk_readers = Vec::new();
        for data_reference in chunks.into_iter() {
            let chunk_reader = self.build_chunk_reader(
                data_reference,
                batch_size,
                projection,
                predicate,
                metrics,
            )?;
            if let Some(chunk_reader) = chunk_reader {
                chunk_readers.push(chunk_reader);
            }
        }

        Ok(chunk_readers)
    }

    fn build_series_reader(
        &self,
        series_key: SeriesKey,
        mut chunks: Vec<DataReference>,
        batch_size: usize,
        query_schema: TskvTableSchemaRef,
        projection: &Projection,
        predicate: &Option<Arc<Predicate>>,
        schema: SchemaRef,
        time_fields_schema: SchemaRef,
        metrics: &SeriesGroupBatchReaderMetrics,
    ) -> TskvResult<Option<BatchReaderRef>> {
        if chunks.is_empty() {
            return Ok(None);
        }
        // TODO performance 通过物理表达式根据 chunk 统计信息过滤chunk
        metrics
            .chunk_nums_filtered_by_statistics()
            .add(chunks.len());

        // 对 chunk 按照时间顺序排序
        // 使用 group_overlapping_segments 函数来对具有重叠关系的chunk进行分组。
        chunks.sort_unstable_by_key(|e| e.time_range());
        let grouped_chunks = group_overlapping_segments(&chunks);

        debug!(
            "series_key: {:?}, grouped_chunks num: {}, grouped_chunks: {:?}",
            series_key,
            grouped_chunks.len(),
            grouped_chunks,
        );
        metrics.grouped_chunk_nums().add(grouped_chunks.len());

        let readers = grouped_chunks
            .into_iter()
            .map(|chunks| -> TskvResult<BatchReaderRef> {
                let chunk_readers = self.build_chunk_readers(
                    chunks.segments(),
                    batch_size,
                    projection,
                    predicate,
                    metrics,
                )?;

                // 用 Null 值补齐缺失的 Field 列
                let chunk_readers = chunk_readers
                    .into_iter()
                    .map(|r| {
                        Arc::new(SchemaAlignmenter::new(
                            r,
                            time_fields_schema.clone(),
                            self.schema_align_reader_metrics_set.clone(),
                        )) as BatchReaderRef
                    })
                    .collect::<Vec<_>>();

                let reader: BatchReaderRef = if chunk_readers.len() > 1 {
                    // 如果有多个重叠的 chunk reader 则需要做合并
                    Arc::new(DataMerger::new(
                        time_fields_schema.clone(),
                        chunk_readers,
                        batch_size,
                        self.merge_reader_metrics_set.clone(),
                    ))
                } else {
                    Arc::new(CombinedBatchReader::new(chunk_readers))
                };

                Ok(reader)
            })
            .collect::<TskvResult<Vec<_>>>()?;

        let limit = predicate.as_ref().and_then(|p| p.limit());
        // 根据 series key 补齐对应的 tag 列
        let series_reader = Arc::new(SeriesReader::new(
            series_key,
            Arc::new(CombinedBatchReader::new(readers)),
            query_schema,
            self.series_reader_metrics_set.clone(),
            limit,
        ));
        // 用 Null 值补齐缺失的 tag 列
        let reader = Arc::new(SchemaAlignmenter::new(
            series_reader,
            schema,
            self.schema_align_reader_metrics_set.clone(),
        ));

        Ok(Some(reader))
    }
}

impl Drop for SeriesGroupBatchReaderFactory {
    fn drop(&mut self) {
        if self.span.context().is_some() {
            self.metrics_set
                .clone_inner()
                .record(&mut self.span, "metrics");
        }
    }
}

/// Extracts columns from the provided table schema and schema reference, excluding tag columns.
/// Returns a new schema reference containing the extracted columns.
///
/// # Arguments
///
/// * `table_schema` - A reference to the table schema (`&TskvTableSchemaRef`)
/// * `schema` - A reference to the schema (`&SchemaRef`)
///
/// # Errors
///
/// Returns an `Error` if a column is not found in the table schema.
///
fn project_time_fields(
    table_schema: &TskvTableSchemaRef,
    schema: &SchemaRef,
) -> SchemaResult<SchemaRef> {
    let mut fields = vec![];
    for field in schema.fields() {
        if let Some(col) = table_schema.column(field.name()) {
            if !col.column_type.is_tag() {
                fields.push(field.clone());
            }
        } else {
            return Err(ColumnNotFoundSnafu {
                column: field.name().to_string(),
            }
            .build());
        }
    }

    Ok(SchemaRef::new(Schema::new(fields)))
}

/// Stores metrics about the table writer execution.
#[derive(Debug, Clone)]
pub struct SeriesGroupBatchReaderMetrics {
    elapsed_get_series_keys_time: metrics::Time,
    elapsed_get_tsm_readers_time: metrics::Time,
    elapsed_build_batch_reader_time: metrics::Time,
    series_nums: metrics::Gauge,
    file_nums_filtered_by_time_range: metrics::Gauge,
    chunk_nums: metrics::Gauge,
    chunk_nums_filtered_by_statistics: metrics::Count,
    grouped_chunk_nums: metrics::Count,
    column_group_nums: metrics::Count,
    filtered_column_group_nums: metrics::Count,
}

impl SeriesGroupBatchReaderMetrics {
    /// Create new metrics
    pub fn new(metrics: &ExecutionPlanMetricsSet, partition: usize) -> Self {
        let elapsed_get_series_keys_time =
            MetricBuilder::new(metrics).subset_time("elapsed_get_series_keys_time", partition);

        let elapsed_get_tsm_readers_time =
            MetricBuilder::new(metrics).subset_time("elapsed_get_tsm_readers_time", partition);

        let elapsed_build_batch_reader_time =
            MetricBuilder::new(metrics).subset_time("elapsed_build_batch_reader_time", partition);

        let series_nums = MetricBuilder::new(metrics).gauge("series_nums", partition);

        let file_nums_filtered_by_time_range =
            MetricBuilder::new(metrics).gauge("file_nums_filtered_by_time_range", partition);

        let chunk_nums = MetricBuilder::new(metrics).gauge("chunk_nums", partition);

        let chunk_nums_filtered_by_statistics =
            MetricBuilder::new(metrics).counter("chunk_nums_filtered_by_statistics", partition);

        let grouped_chunk_nums =
            MetricBuilder::new(metrics).counter("grouped_chunk_nums", partition);

        let column_group_nums = MetricBuilder::new(metrics).counter("column_group_nums", partition);

        let filtered_column_group_nums =
            MetricBuilder::new(metrics).counter("filtered_column_group_nums", partition);

        Self {
            elapsed_get_series_keys_time,
            elapsed_get_tsm_readers_time,
            elapsed_build_batch_reader_time,
            series_nums,
            file_nums_filtered_by_time_range,
            chunk_nums,
            chunk_nums_filtered_by_statistics,
            grouped_chunk_nums,
            column_group_nums,
            filtered_column_group_nums,
        }
    }

    pub fn elapsed_get_series_keys_time(&self) -> &metrics::Time {
        &self.elapsed_get_series_keys_time
    }

    pub fn elapsed_get_tsm_readers_time(&self) -> &metrics::Time {
        &self.elapsed_get_tsm_readers_time
    }

    pub fn elapsed_build_batch_reader_time(&self) -> &metrics::Time {
        &self.elapsed_build_batch_reader_time
    }

    pub fn series_nums(&self) -> &metrics::Gauge {
        &self.series_nums
    }

    pub fn file_nums_filtered_by_time_range(&self) -> &metrics::Gauge {
        &self.file_nums_filtered_by_time_range
    }

    pub fn chunk_nums(&self) -> &metrics::Gauge {
        &self.chunk_nums
    }

    pub fn chunk_nums_filtered_by_statistics(&self) -> &metrics::Count {
        &self.chunk_nums_filtered_by_statistics
    }

    pub fn grouped_chunk_nums(&self) -> &metrics::Count {
        &self.grouped_chunk_nums
    }

    pub fn column_group_nums(&self) -> &metrics::Count {
        &self.column_group_nums
    }

    pub fn filtered_column_group_nums(&self) -> &metrics::Count {
        &self.filtered_column_group_nums
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
    ) -> TskvResult<QueryRecordBatchRequest, models::ModelError> {
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
    vnode_id: VnodeId,

    /// Super version of vnode_id, maybe None.
    super_version: Option<Arc<SuperVersion>>,
    /// List of series id filtered from engine.
    series_ids: Arc<Vec<SeriesId>>,
    series_iter_closer: CancellationToken,
    /// Whether this iterator was finsihed.
    #[allow(unused)]
    span: Span,
    metrics_set: ExecutionPlanMetricsSet,
}

impl RowIterator {
    pub fn new_column_builder(
        column_type: &PhysicalCType,
        batch_size: usize,
    ) -> TskvResult<Box<dyn ArrayBuilder>> {
        Ok(match column_type {
            PhysicalCType::Tag => {
                Box::new(StringBuilder::with_capacity(batch_size, batch_size * 32))
            }
            PhysicalCType::Time(unit) => match unit {
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
            PhysicalCType::Field(t) => match t {
                PhysicalDType::Float => Box::new(Float64Builder::with_capacity(batch_size)),
                PhysicalDType::Integer => Box::new(Int64Builder::with_capacity(batch_size)),
                PhysicalDType::Unsigned => Box::new(UInt64Builder::with_capacity(batch_size)),
                PhysicalDType::Boolean => Box::new(BooleanBuilder::with_capacity(batch_size)),
                PhysicalDType::String => {
                    Box::new(StringBuilder::with_capacity(batch_size, batch_size * 32))
                }
                PhysicalDType::Unknown => {
                    return Err(CommonSnafu {
                        reason: "failed to create column builder: unkown column type".to_string(),
                    }
                    .build())
                }
            },
        })
    }
}

impl Drop for RowIterator {
    fn drop(&mut self) {
        self.series_iter_closer.cancel();

        if SpanContext::from_span(&self.span).is_some() {
            let version_number = self.super_version.as_ref().map(|v| v.version_number);
            let ts_family_id = self.super_version.as_ref().map(|v| v.ts_family_id);

            self.span.add_properties(|| {
                [
                    ("version_number", format!("{version_number:?}")),
                    ("ts_family_id", format!("{ts_family_id:?}")),
                    ("vnode_id", self.vnode_id.to_string()),
                    ("series_ids_num", self.series_ids.len().to_string()),
                ]
            });

            let metrics = self
                .metrics_set
                .clone_inner()
                .aggregate_by_name()
                .sorted_for_display()
                .timestamps_removed();

            metrics.iter().for_each(|e| {
                self.span
                    .add_property(|| (e.value().name().to_string(), e.value().to_string()));
            });
        }
    }
}

pub async fn execute(
    runtime: Arc<Runtime>,
    engine: EngineRef,
    query_option: QueryOption,
    vnode_id: VnodeId,
    span: Span,
) -> TskvResult<SendableTskvRecordBatchStream> {
    let super_version = {
        let span = Span::enter_with_parent("get super version", &span);
        engine
            .get_db_version(
                &query_option.table_schema.tenant,
                &query_option.table_schema.db,
                vnode_id,
            )
            .await
            .map_err(|err| {
                span.error(err.to_string());
                err
            })?
    };

    let schema = query_option.df_schema.clone();

    if let Some(super_version) = super_version {
        return build_stream(
            runtime,
            super_version,
            engine,
            query_option,
            vnode_id,
            Span::enter_with_parent("build stream", &span),
        )
        .await;
    }

    Ok(Box::pin(EmptySchemableTskvRecordBatchStream::new(schema)))
}

async fn build_stream(
    _runtime: Arc<Runtime>,
    super_version: Arc<SuperVersion>,
    engine: EngineRef,
    query_option: QueryOption,
    vnode_id: VnodeId,
    span: Span,
) -> TskvResult<SendableTskvRecordBatchStream> {
    let series_ids = {
        let span = Span::enter_with_parent("get series ids by filter", &span);
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
                span.error(err.to_string());
                err
            })?
    };

    // TODO 这里需要验证table schema是否正确
    let expr = query_option.split.filter();
    let arrow_schema = query_option.table_schema.to_arrow_schema();
    let physical_expr = if expr.expr_type.is_none() {
        None
    } else {
        Some(parse_physical_expr(expr, &NoRegistry, &arrow_schema)?)
    };

    let predicate = PredicateRef::new(Predicate::new(
        physical_expr,
        arrow_schema,
        query_option.split.limit(),
    ));

    if series_ids.is_empty() {
        return Ok(Box::pin(EmptySchemableTskvRecordBatchStream::new(
            query_option.df_schema.clone(),
        )));
    }

    if query_option.aggregates.is_some() {
        // TODO: 重新实现聚合下推
        return Err(CommonSnafu {
            reason: "aggregates push down is not supported yet".to_string(),
        }
        .build());
    }

    let factory = SeriesGroupBatchReaderFactory::new(
        engine,
        query_option,
        super_version,
        Span::enter_with_parent("SeriesGroupBatchReaderFactory", &span),
        ExecutionPlanMetricsSet::new(),
    );

    if let Some(reader) = factory
        .create(
            Span::enter_with_parent("SeriesGroupBatchReader", &span),
            &series_ids,
            Some(predicate),
        )
        .await?
    {
        return Ok(Box::pin(reader.process()?));
    }

    Ok(Box::pin(EmptySchemableTskvRecordBatchStream::new(
        factory.schema(),
    )))
}

#[cfg(test)]
mod test {
    #[test]
    fn test_field_cursor() {
        // TODO: Test multi-level contains the same timestamp with different values.
    }
}
