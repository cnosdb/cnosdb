use std::sync::Arc;

use arrow::datatypes::{Schema, SchemaRef};
use datafusion::physical_plan::metrics::{
    self, ExecutionPlanMetricsSet, MetricBuilder, MetricsSet,
};
use models::meta_data::VnodeId;
use models::schema::TskvTableSchemaRef;
use models::{SeriesId, SeriesKey};
use tokio::runtime::Runtime;
use trace::{debug, SpanRecorder};

use super::display::DisplayableBatchReader;
use super::merge::DataMerger;
use super::series::SeriesReader;
use super::utils::OverlappingSegments;
use super::{
    DataReference, EmptySchemableTskvRecordBatchStream, Predicate, Projection, QueryOption,
    SendableTskvRecordBatchStream,
};
use crate::reader::chunk::ChunkReader;
use crate::reader::filter::DataFilter;
use crate::reader::paralle_merge::ParallelMergeAdapter;
use crate::reader::schema_alignmenter::SchemaAlignmenter;
use crate::reader::utils::group_overlapping_segments;
use crate::reader::BatchReaderRef;
use crate::schema::error::SchemaError;
use crate::tseries_family::{ColumnFile, SuperVersion, Version};
use crate::{EngineRef, Error, Result};

pub async fn execute(
    runtime: Arc<Runtime>,
    engine: EngineRef,
    query_option: QueryOption,
    vnode_id: VnodeId,
    span_recorder: SpanRecorder,
) -> Result<SendableTskvRecordBatchStream> {
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

    let schema = query_option.df_schema.clone();

    if let Some(super_version) = super_version {
        return build_stream(
            runtime,
            super_version,
            engine,
            query_option,
            vnode_id,
            span_recorder,
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
    span_recorder: SpanRecorder,
) -> Result<SendableTskvRecordBatchStream> {
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

    if series_ids.is_empty() {
        return Ok(Box::pin(EmptySchemableTskvRecordBatchStream::new(
            query_option.df_schema.clone(),
        )));
    }

    if query_option.aggregates.is_some() {
        // TODO: 重新实现聚合下推
        return Err(Error::CommonError {
            reason: "aggregates push down is not supported yet".to_string(),
        });
    }

    let factory = SeriesGroupBatchReaderFactory::new(
        engine,
        query_option,
        super_version,
        span_recorder,
        ExecutionPlanMetricsSet::new(),
    );

    if let Some(reader) = factory.create(&series_ids).await? {
        return Ok(reader.process()?);
    }

    Ok(Box::pin(EmptySchemableTskvRecordBatchStream::new(
        factory.schema(),
    )))
}

pub struct SeriesGroupBatchReaderFactory {
    engine: EngineRef,
    query_option: QueryOption,
    super_version: Arc<SuperVersion>,

    #[allow(unused)]
    span_recorder: SpanRecorder,
    metrics_set: ExecutionPlanMetricsSet,
}

impl SeriesGroupBatchReaderFactory {
    pub fn new(
        engine: EngineRef,
        query_option: QueryOption,
        super_version: Arc<SuperVersion>,
        span_recorder: SpanRecorder,
        metrics_set: ExecutionPlanMetricsSet,
    ) -> Self {
        Self {
            engine,
            query_option,
            super_version,
            span_recorder,
            metrics_set,
        }
    }

    pub fn schema(&self) -> SchemaRef {
        self.query_option.df_schema.clone()
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics_set.clone_inner())
    }

    /// ParallelMergeAdapter: schema=[{}]                               -------- 并行执行多个 stream
    ///   SchemaAlignmenter: schema=[{}]                                -------- 用 Null 值补齐缺失的 tag 列
    ///    SeriesReader: sid={}, schema=[{}]                            -------- 根据 series key 补齐对应的 tag 列
    ///      SchemaAlignmenter:                                         -------- 用 Null 值补齐缺失的 Field 列
    ///        DataMerger: schema=[{}]                                  -------- 合并相同 series 下时间段重叠的chunk数据
    ///          DataFilter: expr=[{}], schema=[{}]                     -------- 根据下推的过滤条件精确过滤数据
    ///            MemRowGroup/ChunkReader: sid={}, projection=[{}], schema=[{}]    -------- 读取单个chunk或memcache rowgroup的数据的指定列的数据
    ///          DataFilter:
    ///            MemRowGroup/ChunkReader [PageReader]
    ///          ......
    ///      SchemaAlignmenter:
    ///        DataMerger: schema=[{}]
    ///          DataFilter: expr=[{}], schema=[{}]
    ///            MemRowGroup/ChunkReader: sid={}, projection=[{}], schema=[{}]
    ///          DataFilter
    ///            MemRowGroup/ChunkReader [PageReader]
    ///          ......
    ///        ......
    ///  SchemaAlignmenter: schema=[{}]                                                
    ///    SeriesReader: sid={}, schema=[{}]
    ///      SchemaAlignmenter:
    ///        DataMerger: schema=[{}]
    ///          DataFilter: expr=[{}], schema=[{}]
    ///            MemRowGroup/ChunkReader: sid={}, projection=[{}], schema=[{}]
    ///          DataFilter
    ///            MemRowGroup/ChunkReader [PageReader]
    ///          ......
    ///      ......
    ///    ......
    pub async fn create(&self, series_ids: &[u32]) -> Result<Option<BatchReaderRef>> {
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

        let schema = &self.query_option.df_schema;
        let time_fields_schema = project_time_fields(
            &self.query_option.table_schema,
            &self.query_option.df_schema,
        )?;
        // TODO 使用query下推的物理表达式
        let predicate: Option<Arc<Predicate>> = None;

        let super_version = &self.super_version;
        let vnode_id = super_version.ts_family_id;
        let projection = Projection::from(schema.as_ref());
        let time_ranges = self.query_option.split.time_ranges();
        let column_files = super_version.column_files(time_ranges.as_ref());

        // 采集过滤后的文件数量
        metrics
            .file_nums_filtered_by_time_range()
            .set(column_files.len());

        // 通过sid获取serieskey
        let sid_keys = self.series_keys(vnode_id, series_ids).await?;

        // 获取所有的符合条件的chunk Vec<(SeriesKey, Vec<DataReference>)>
        let mut series_chunk_readers = vec![];
        for (sid, series_key) in series_ids.iter().zip(sid_keys) {
            // 选择含有series的所有chunk Vec<DataReference::Chunk(chunk, reader)>
            let chunks =
                Self::filter_chunks(super_version.version.as_ref(), &column_files, *sid).await?;
            // TODO @lutengda 获取所有符合条件的 memcache rowgroup Vec<DataReference::Memcache(rowgroup)>)
            series_chunk_readers.push((series_key, chunks));
        }

        metrics
            .chunk_nums()
            .set(series_chunk_readers.iter().map(|(_, e)| e.len()).sum());

        let series_readers = series_chunk_readers
            .into_iter()
            .filter_map(|(series_key, chunks)| {
                Self::build_series_reader(
                    series_key,
                    chunks,
                    self.query_option.batch_size,
                    &projection,
                    &predicate,
                    schema.clone(),
                    time_fields_schema.clone(),
                    &metrics,
                )
                .transpose()
            })
            .collect::<Result<Vec<_>>>()?;

        if series_readers.is_empty() {
            return Ok(None);
        }

        // 不同series reader并行执行
        let reader = Arc::new(ParallelMergeAdapter::try_new(
            schema.clone(),
            series_readers,
            self.query_option.batch_size,
        )?);

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
    ) -> Result<Vec<SeriesKey>> {
        // 通过sid获取serieskey
        let mut sid_keys = vec![];
        for sid in series_ids {
            let series_key = self
                .engine
                .get_series_key(
                    &self.query_option.table_schema.tenant,
                    &self.query_option.table_schema.db,
                    vnode_id,
                    *sid,
                )
                .await?
                .ok_or_else(|| Error::CommonError {
                    reason: format!("Get series key of sid {sid}, this maybe a bug"),
                })?;
            sid_keys.push(series_key);
        }

        Ok(sid_keys)
    }

    /// 从给定的文件列表中选择含有指定series的所有chunk及其对应的TSMReader
    async fn filter_chunks(
        version: &Version,
        column_files: &[Arc<ColumnFile>],
        sid: SeriesId,
    ) -> Result<Vec<DataReference>> {
        // 选择含有series的所有文件
        let files = column_files
            .iter()
            .filter(|cf| cf.contains_series_id(sid))
            .collect::<Vec<_>>();
        // 选择含有series的所有chunk
        let mut chunks = Vec::with_capacity(files.len());
        for f in files {
            let reader = version.get_tsm_reader2(f.file_path()).await?;
            let chunk = reader.chunk().get(&sid).ok_or_else(|| Error::CommonError {
                reason: format!("Retrieve chunks with sid {sid}, this maybe a bug"),
            })?;
            chunks.push(DataReference::Chunk(chunk.clone(), reader));
        }

        Ok(chunks)
    }

    fn build_chunk_reader(
        chunk: DataReference,
        batch_size: usize,
        projection: &Projection,
        predicate: &Option<Arc<Predicate>>,
    ) -> Result<BatchReaderRef> {
        let chunk_reader = match chunk {
            DataReference::Chunk(chunk, reader) => Arc::new(ChunkReader::try_new(
                reader, chunk, projection, predicate, batch_size,
            )?),
            DataReference::Memcache(_) => {
                // TODO @lutengda 构建memcache rowgroup reader(需要自己实现 trait BatchReader)
                return Err(Error::Unimplemented {
                    msg: "memcache reader is not supported yet".to_string(),
                });
            }
        };

        // 数据过滤
        if let Some(predicate) = &predicate {
            return Ok(Arc::new(DataFilter::new(predicate.clone(), chunk_reader)));
        }

        Ok(chunk_reader)
    }

    fn build_chunk_readers(
        chunks: OverlappingSegments<DataReference>,
        batch_size: usize,
        projection: &Projection,
        predicate: &Option<Arc<Predicate>>,
    ) -> Result<Vec<BatchReaderRef>> {
        let chunk_readers = chunks
            .into_iter()
            .map(|data_reference| -> Result<BatchReaderRef> {
                let chunk_reader =
                    Self::build_chunk_reader(data_reference, batch_size, projection, predicate)?;

                Ok(chunk_reader)
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(chunk_readers)
    }

    fn build_series_reader(
        series_key: SeriesKey,
        mut chunks: Vec<DataReference>,
        batch_size: usize,
        projection: &Projection,
        predicate: &Option<Arc<Predicate>>,
        schema: SchemaRef,
        time_fields_schema: SchemaRef,
        metrics: &SeriesGroupBatchReaderMetrics,
    ) -> Result<Option<BatchReaderRef>> {
        if chunks.is_empty() {
            return Ok(None);
        }
        // TODO performance 通过物理表达式根据 chunk 统计信息过滤chunk

        metrics
            .chunk_nums_filtered_by_statistics()
            .set(chunks.len());

        // 对 chunk 按照时间顺序排序
        // 使用 group_overlapping_segments 函数来对具有重叠关系的chunk进行分组。
        chunks.sort_unstable_by_key(|e| *e.time_range());
        let grouped_chunks = group_overlapping_segments(&chunks);

        debug!(
            "series_key: {:?}, grouped_chunks num: {}",
            series_key,
            grouped_chunks.len()
        );
        metrics.grouped_chunk_nums().set(grouped_chunks.len());

        let readers = grouped_chunks
            .into_iter()
            .map(|chunks| -> Result<BatchReaderRef> {
                let chunk_readers =
                    Self::build_chunk_readers(chunks, batch_size, projection, predicate)?;

                // 用 Null 值补齐缺失的 Field 列
                let chunk_readers = chunk_readers
                    .into_iter()
                    .map(|r| {
                        Arc::new(SchemaAlignmenter::new(r, time_fields_schema.clone()))
                            as BatchReaderRef
                    })
                    .collect::<Vec<_>>();

                let reader: BatchReaderRef = if chunk_readers.len() > 1 {
                    // 如果有多个重叠的 chunk reader 则需要做合并
                    Arc::new(DataMerger::new(time_fields_schema.clone(), chunk_readers, batch_size))
                } else {
                    Arc::new(chunk_readers)
                };

                // 用 Null 值补齐缺失的 Field 列
                Ok(reader)
            })
            .collect::<Result<Vec<_>>>()?;

        // 根据 series key 补齐对应的 tag 列
        let series_reader = Arc::new(SeriesReader::new(series_key, Arc::new(readers)));
        // 用 Null 值补齐缺失的 tag 列
        let reader = Arc::new(SchemaAlignmenter::new(series_reader, schema));

        Ok(Some(reader))
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
fn project_time_fields(table_schema: &TskvTableSchemaRef, schema: &SchemaRef) -> Result<SchemaRef> {
    let mut fields = vec![];
    for field in schema.fields() {
        if let Some(col) = table_schema.column(field.name()) {
            if !col.column_type.is_tag() {
                fields.push(field.clone());
            }
        } else {
            return Err(Error::Schema {
                source: SchemaError::ColumnNotFound {
                    column: field.name().to_string(),
                },
            });
        }
    }

    Ok(SchemaRef::new(Schema::new(fields)))
}

/// Stores metrics about the table writer execution.
#[derive(Debug, Clone)]
pub struct SeriesGroupBatchReaderMetrics {
    elapsed_build_batch_reader_time: metrics::Time,
    series_nums: metrics::Gauge,
    file_nums_filtered_by_time_range: metrics::Gauge,
    chunk_nums: metrics::Gauge,
    chunk_nums_filtered_by_statistics: metrics::Gauge,
    grouped_chunk_nums: metrics::Gauge,
}

impl SeriesGroupBatchReaderMetrics {
    /// Create new metrics
    pub fn new(metrics: &ExecutionPlanMetricsSet, partition: usize) -> Self {
        let elapsed_build_batch_reader_time =
            MetricBuilder::new(metrics).subset_time("elapsed_build_batch_reader_time", partition);

        let series_nums = MetricBuilder::new(metrics).gauge("series_nums", partition);

        let file_nums_filtered_by_time_range =
            MetricBuilder::new(metrics).gauge("file_nums_filtered_by_time_range", partition);

        let chunk_nums = MetricBuilder::new(metrics).gauge("chunk_nums", partition);

        let chunk_nums_filtered_by_statistics =
            MetricBuilder::new(metrics).gauge("chunk_nums_filtered_by_statistics", partition);

        let grouped_chunk_nums =
            MetricBuilder::new(metrics).gauge("chunk_nums_filtered_by_statistics", partition);

        Self {
            elapsed_build_batch_reader_time,
            series_nums,
            file_nums_filtered_by_time_range,
            chunk_nums,
            chunk_nums_filtered_by_statistics,
            grouped_chunk_nums,
        }
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

    pub fn chunk_nums_filtered_by_statistics(&self) -> &metrics::Gauge {
        &self.chunk_nums_filtered_by_statistics
    }

    pub fn grouped_chunk_nums(&self) -> &metrics::Gauge {
        &self.grouped_chunk_nums
    }
}
