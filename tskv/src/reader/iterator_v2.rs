use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion::physical_plan::PhysicalExpr;
use models::meta_data::VnodeId;
use models::{SeriesId, SeriesKey};
use tokio::runtime::Runtime;
use trace::{debug, SpanRecorder};

use super::merge::{DataMerger, ParallelMergeAdapter};
use super::series::SeriesReader;
use super::utils::OverlappingSegments;
use super::{
    EmptySchemableTskvRecordBatchStream, Projection, QueryOption, SendableTskvRecordBatchStream,
};
use crate::reader::chunk::ChunkReader;
use crate::reader::filter::DataFilter;
use crate::reader::schema_alignmenter::SchemaAlignmenter;
use crate::reader::utils::group_overlapping_segments;
use crate::reader::BatchReaderRef;
use crate::tseries_family::{ColumnFile, SuperVersion, Version};
use crate::tsm2::page::Chunk;
use crate::tsm2::reader::TSM2Reader;
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
    metrics: ExecutionPlanMetricsSet,
}

impl SeriesGroupBatchReaderFactory {
    pub fn new(
        engine: EngineRef,
        query_option: QueryOption,
        super_version: Arc<SuperVersion>,
        span_recorder: SpanRecorder,
        metrics: ExecutionPlanMetricsSet,
    ) -> Self {
        Self {
            engine,
            query_option,
            super_version,
            span_recorder,
            metrics,
        }
    }

    pub fn schema(&self) -> SchemaRef {
        self.query_option.df_schema.clone()
    }

    /// ParallelMergeAdapter: schema=[{}]                               -------- 并行执行多个 stream
    ///   SchemaAlignmenter: schema=[{}]                                -------- 用 Null 值补齐缺失的 tag 列
    ///    SeriesReader: sid={}, schema=[{}]                            -------- 根据 series key 补齐对应的 tag 列
    ///      SchemaAlignmenter:                                         -------- 用 Null 值补齐缺失的 Field 列
    ///        DataMerger: schema=[{}]                                  -------- 合并相同 series 下时间段重叠的chunk数据
    ///          DataFilter: expr=[{}], schema=[{}]                     -------- 根据下推的过滤条件精确过滤数据
    ///            ChunkReader: sid={}, projection=[{}], schema=[{}]    -------- 读取单个chunk的数据的指定列的数据
    ///          DataFilter:
    ///            ChunkReader [PageReader]
    ///          ......
    ///        DataMerger: schema=[{}]
    ///          DataFilter: expr=[{}], schema=[{}]
    ///            ChunkReader: sid={}, projection=[{}], schema=[{}]
    ///          DataFilter
    ///            ChunkReader [PageReader]
    ///          ......
    ///        ......
    ///  SchemaAlignmenter: schema=[{}]                                                
    ///    SeriesReader: sid={}, schema=[{}]
    ///      SchemaAlignmenter:
    ///        DataMerger: schema=[{}]
    ///          DataFilter: expr=[{}], schema=[{}]
    ///            ChunkReader: sid={}, projection=[{}], schema=[{}]
    ///          DataFilter
    ///            ChunkReader [PageReader]
    ///          ......
    ///      ......
    ///    ......
    pub async fn create(&self, series_ids: &[u32]) -> Result<Option<BatchReaderRef>> {
        // let metrics = SeriesGroupRowIteratorMetrics::new(&metrics_set, partition),
        if series_ids.is_empty() {
            return Ok(None);
        }

        let schema = &self.query_option.df_schema;
        let time_fields_schema = &self.query_option.df_schema;
        // TODO 使用query下推的物理表达式
        let predicate: Option<Arc<dyn PhysicalExpr>> = None;

        let super_version = &self.super_version;
        let vnode_id = super_version.ts_family_id;
        let projection = Projection::from(schema.as_ref());
        let time_ranges = self.query_option.split.time_ranges();
        let column_files = super_version.column_files(time_ranges.as_ref());

        // 通过sid获取serieskey
        let sid_keys = self.series_keys(vnode_id, series_ids).await?;

        // 获取所有的符合条件的chunk Vec<(SeriesKey, Vec<(chunk, reader)>)>
        let mut series_chunk_readers = vec![];
        for (sid, series_key) in series_ids.iter().zip(sid_keys) {
            // 选择含有series的所有chunk
            let chunks =
                Self::filter_chunks(super_version.version.as_ref(), &column_files, *sid).await?;
            series_chunk_readers.push((series_key, chunks));
        }

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
        )?);

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
                    reason: "Thi maybe a bug".to_string(),
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
    ) -> Result<Vec<(Arc<Chunk>, Arc<TSM2Reader>)>> {
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
                reason: "Thi maybe a bug".to_string(),
            })?;
            chunks.push((chunk.clone(), reader));
        }

        Ok(chunks)
    }

    fn build_chunk_reader(
        chunk: Arc<Chunk>,
        reader: Arc<TSM2Reader>,
        batch_size: usize,
        projection: &Projection,
        predicate: &Option<Arc<dyn PhysicalExpr>>,
    ) -> Result<BatchReaderRef> {
        let chunk_reader = Arc::new(ChunkReader::try_new(reader, chunk, projection, batch_size)?);

        // 数据过滤
        if let Some(predicate) = &predicate {
            return Ok(Arc::new(DataFilter::new(predicate.clone(), chunk_reader)));
        }

        Ok(chunk_reader)
    }

    fn build_chunk_readers(
        chunks: OverlappingSegments<(Arc<Chunk>, Arc<TSM2Reader>)>,
        batch_size: usize,
        projection: &Projection,
        predicate: &Option<Arc<dyn PhysicalExpr>>,
    ) -> Result<Vec<BatchReaderRef>> {
        let chunk_readers = chunks
            .into_iter()
            .map(|(chunk, reader)| -> Result<BatchReaderRef> {
                let chunk_reader =
                    Self::build_chunk_reader(chunk, reader, batch_size, projection, predicate)?;

                Ok(chunk_reader)
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(chunk_readers)
    }

    fn build_series_reader(
        series_key: SeriesKey,
        mut chunks: Vec<(Arc<Chunk>, Arc<TSM2Reader>)>,
        batch_size: usize,
        projection: &Projection,
        predicate: &Option<Arc<dyn PhysicalExpr>>,
        schema: SchemaRef,
        time_fields_schema: SchemaRef,
    ) -> Result<Option<BatchReaderRef>> {
        if chunks.is_empty() {
            return Ok(None);
        }
        // TODO 通过物理表达式根据page统计信息过滤chunk

        // 对 chunk 按照时间顺序排序
        // 使用 group_overlapping_segments 函数来对具有重叠关系的chunk进行分组。
        chunks.sort_unstable_by_key(|(e, _)| *e.time_range());
        let grouped_chunks = group_overlapping_segments(&chunks);

        debug!(
            "series_key: {:?}, grouped_chunks num: {}",
            series_key,
            grouped_chunks.len()
        );

        let readers = grouped_chunks
            .into_iter()
            .map(|chunks| -> Result<BatchReaderRef> {
                let chunk_readers =
                    Self::build_chunk_readers(chunks, batch_size, projection, predicate)?;

                let merger = Arc::new(DataMerger::new(chunk_readers));
                // 用 Null 值补齐缺失的 Field 列
                let reader = Arc::new(SchemaAlignmenter::new(merger, time_fields_schema.clone()));
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
