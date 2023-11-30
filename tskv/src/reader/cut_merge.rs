use std::collections::VecDeque;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{ready, Context, Poll};

use arrow_array::RecordBatch;
use arrow_schema::{ArrowError, SchemaRef};
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricBuilder, Time};
use futures::{Stream, StreamExt};
use stable_vec::StableVec;

use super::metrics::BaselineMetrics;
use crate::reader::batch_cut::cut_record_batches;
use crate::reader::sort_merge::sort_merge;
use crate::reader::{
    SchemableMemoryBatchReaderStream, SchemableTskvRecordBatchStream,
    SendableSchemableTskvRecordBatchStream,
};
use crate::Result;

pub struct CutMergeStream {
    schema: SchemaRef,
    // 发送过来的一个record batch中时间戳不会重复
    streams: StableVec<SendableSchemableTskvRecordBatchStream>,

    record_batch_cache: StableVec<VecDeque<RecordBatch>>,

    // merge_sort and merge rows
    merge_stream: Option<SendableSchemableTskvRecordBatchStream>,

    sort_column: Arc<String>,

    batch_size: usize,

    metrics: CutMergeMetrics,
}

impl CutMergeStream {
    pub fn new(
        schema: SchemaRef,
        streams: Vec<SendableSchemableTskvRecordBatchStream>,
        batch_size: usize,
        sort_column: impl Into<String>,
        metrics: CutMergeMetrics,
    ) -> Result<Self> {
        let sort_column = Arc::new(sort_column.into());
        schema.field_with_name(sort_column.as_str())?;

        for stream in streams.iter() {
            stream.schema().field_with_name(sort_column.as_str())?;
        }

        let cache = (0..streams.len()).map(|_| VecDeque::default()).collect();
        let streams = StableVec::from_iter(streams.into_iter());

        Ok(Self {
            schema,
            streams,
            record_batch_cache: cache,
            merge_stream: None,
            sort_column,
            batch_size,
            metrics,
        })
    }

    /// if is empty return true
    /// if record_batch.num_rows() == 0 {
    ///     stream.next()
    /// }
    /// 拉到缓存并补齐
    ///
    fn pull_stream_to_cache(
        cx: &mut Context<'_>,
        stream: &mut SendableSchemableTskvRecordBatchStream,
        cache: &mut VecDeque<RecordBatch>,
    ) -> Poll<Result<bool>> {
        loop {
            match cache.front() {
                None => match ready!(stream.poll_next_unpin(cx)) {
                    None => return Poll::Ready(Ok(true)),
                    Some(Ok(r)) => {
                        cache.push_back(r);
                    }
                    Some(Err(e)) => return Poll::Ready(Err(e)),
                },
                Some(b) => {
                    if b.num_rows() == 0 {
                        cache.pop_front();
                    } else {
                        return Poll::Ready(Ok(false));
                    }
                }
            }
        }
    }

    fn pull_merge_stream(&mut self, cx: &mut Context<'_>) -> Poll<Option<Result<RecordBatch>>> {
        if let Some(s) = self.merge_stream.as_mut() {
            match ready!(s.poll_next_unpin(cx)) {
                None => self.merge_stream = None,
                other => return Poll::Ready(other),
            }
        }

        Poll::Ready(None)
    }

    fn pull_streams_to_caches(&mut self, cx: &mut Context<'_>) -> Poll<Result<()>> {
        let mut empty_stream = vec![];
        for ((stream_idx, stream), (cache_idx, cache)) in self
            .streams
            .iter_mut()
            .zip(self.record_batch_cache.iter_mut())
        {
            match ready!(Self::pull_stream_to_cache(cx, stream, cache)) {
                Ok(empty) => {
                    if empty {
                        empty_stream.push((stream_idx, cache_idx))
                    }
                }
                Err(e) => return Poll::Ready(Err(e)),
            }
        }

        // clean empty stream
        if !empty_stream.is_empty() {
            for (stream_idx, cache_idx) in empty_stream.iter() {
                self.streams.remove(*stream_idx);
                self.record_batch_cache.remove(*cache_idx);
            }
            self.streams.make_compact();
            self.record_batch_cache.make_compact();
            empty_stream.clear();
        }
        Poll::Ready(Ok(()))
    }

    fn next_record_batch(&mut self, cx: &mut Context<'_>) -> Poll<Option<Result<RecordBatch>>> {
        if let Err(e) = ready!(self.pull_streams_to_caches(cx)) {
            return Poll::Ready(Some(Err(e)));
        };

        // 没有record_batch 处理返回
        if self.record_batch_cache.is_empty() {
            return Poll::Ready(None);
        }

        let _timer = self.metrics.elapsed_data_merge_time().timer();

        let time_column = self
            .schema
            .column_with_name(self.sort_column.as_ref())
            .ok_or_else(|| {
                ArrowError::SchemaError(format!(
                    "Unable to get field named \"{}\".",
                    self.sort_column.as_str()
                ))
            })?;
        let mut merge = cut_record_batches(
            &mut self.record_batch_cache,
            time_column.1.data_type(),
            time_column.0,
        )?;
        if merge.len() == 1 {
            return Poll::Ready(Some(Ok(merge.pop().unwrap())));
        }

        let stream = merge
            .into_iter()
            .map(|r| {
                Box::pin(SchemableMemoryBatchReaderStream::new(r.schema(), vec![r]))
                    as SendableSchemableTskvRecordBatchStream
            })
            .collect::<Vec<_>>();

        let mut stream = sort_merge(
            stream,
            self.schema.clone(),
            self.batch_size,
            self.sort_column.as_str(),
        )?;

        match ready!(stream.poll_next_unpin(cx)) {
            None => Poll::Ready(None),
            Some(Ok(r)) => {
                self.merge_stream = Some(stream);
                Poll::Ready(Some(Ok(r)))
            }
            other => Poll::Ready(other),
        }
    }
}

/// record_batch.schema 按 schema 对齐
// pub fn padding_record_batch(
//     schema: SchemaRef,
//     record_batch: RecordBatch,
// ) -> std::result::Result<RecordBatch, ArrowError> {
//     if schema.as_ref().eq(record_batch.schema().as_ref()) {
//         return Ok(record_batch);
//     }
//
//     time_field_from_schema(record_batch.schema().as_ref())?;
//     let record_batch_schema = record_batch.schema();
//     let len = record_batch.num_rows();
//     let columns = schema
//         .fields
//         .iter()
//         .map(|f| match record_batch_schema.column_with_name(f.name()) {
//             None => arrow_array::new_null_array(f.data_type(), len),
//             Some((idx, _)) => record_batch.column(idx).clone(),
//         })
//         .collect::<Vec<_>>();
//     RecordBatch::try_new(schema, columns)
// }
//

impl Stream for CutMergeStream {
    type Item = Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let poll = self.next_record_batch(cx);
        self.metrics.record_poll(poll)
    }
}

impl SchemableTskvRecordBatchStream for CutMergeStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

/// Stores metrics about the table writer execution.
#[derive(Debug, Clone)]
pub struct CutMergeMetrics {
    elapsed_data_merge_time: Time,
    inner: BaselineMetrics,
}

impl CutMergeMetrics {
    /// Create new metrics
    pub fn new(metrics: &ExecutionPlanMetricsSet) -> Self {
        let elapsed_data_merge_time =
            MetricBuilder::new(metrics).subset_time("elapsed_data_merge_time", 0);

        let inner = BaselineMetrics::new(metrics);

        Self {
            elapsed_data_merge_time,
            inner,
        }
    }

    pub fn elapsed_data_merge_time(&self) -> &Time {
        &self.elapsed_data_merge_time
    }
    pub fn elapsed_compute(&self) -> &Time {
        self.inner.elapsed_compute()
    }

    pub fn record_poll(
        &self,
        poll: Poll<Option<Result<RecordBatch>>>,
    ) -> Poll<Option<Result<RecordBatch>>> {
        self.inner.record_poll(poll)
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use arrow_array::{Int64Array, RecordBatch};
    use arrow_schema::{DataType, Field, Fields, Schema, SchemaRef};
    use futures::StreamExt;

    use crate::reader::cut_merge::CutMergeStream;
    use crate::reader::test_util::cut_merge_metrices;
    use crate::reader::SchemableMemoryBatchReaderStream;

    #[tokio::test]
    async fn test_cut_merge_stream() {
        let fields = Fields::from(vec![
            Field::new("column1", DataType::Int64, true),
            Field::new("column2", DataType::Int64, true),
        ]);
        let schema = SchemaRef::new(Schema::new(fields));

        let array11 = Arc::new(Int64Array::from_iter_values([1, 2, 3]));
        let array12 = Arc::new(Int64Array::from_iter_values([1, 2, 3]));
        let record_batch1 = RecordBatch::try_new(schema.clone(), vec![array11, array12]).unwrap();
        let array21 = Arc::new(Int64Array::from_iter_values([2, 3, 4]));
        let array22 = Arc::new(Int64Array::from_iter_values([4, 5, 6]));
        let record_batch2 = RecordBatch::try_new(schema.clone(), vec![array21, array22]).unwrap();
        let array31 = Arc::new(Int64Array::from_iter_values([3, 4, 5]));
        let array32 = Arc::new(Int64Array::from_iter_values([7, 8, 9]));
        let record_batch3 = RecordBatch::try_new(schema.clone(), vec![array31, array32]).unwrap();

        let batches = vec![
            vec![record_batch1],
            vec![record_batch2],
            vec![record_batch3],
        ];
        let streams = SchemableMemoryBatchReaderStream::new_partitions(schema.clone(), batches);

        let mut stream = CutMergeStream::new(
            schema.clone(),
            streams,
            4096,
            "column1",
            cut_merge_metrices(),
        )
        .unwrap();

        let res = stream.next().await.unwrap().unwrap();
        let array1 = Arc::new(Int64Array::from_iter_values([1]));
        let array2 = Arc::new(Int64Array::from_iter_values([1]));
        let expected_batch = RecordBatch::try_new(schema.clone(), vec![array1, array2]).unwrap();
        assert_eq!(res, expected_batch);

        let res = stream.next().await.unwrap().unwrap();
        let array1 = Arc::new(Int64Array::from_iter_values([2]));
        let array2 = Arc::new(Int64Array::from_iter_values([4]));
        let expected_batch = RecordBatch::try_new(schema.clone(), vec![array1, array2]).unwrap();
        assert_eq!(res, expected_batch);

        let res = stream.next().await.unwrap().unwrap();
        let array1 = Arc::new(Int64Array::from_iter_values([3]));
        let array2 = Arc::new(Int64Array::from_iter_values([7]));
        let expected_batch = RecordBatch::try_new(schema.clone(), vec![array1, array2]).unwrap();
        assert_eq!(res, expected_batch);

        let res = stream.next().await.unwrap().unwrap();
        let array1 = Arc::new(Int64Array::from_iter_values([4]));
        let array2 = Arc::new(Int64Array::from_iter_values([8]));
        let expected_batch = RecordBatch::try_new(schema.clone(), vec![array1, array2]).unwrap();
        assert_eq!(res, expected_batch);

        let res = stream.next().await.unwrap().unwrap();
        let array1 = Arc::new(Int64Array::from_iter_values([5]));
        let array2 = Arc::new(Int64Array::from_iter_values([9]));
        let expected_batch = RecordBatch::try_new(schema.clone(), vec![array1, array2]).unwrap();
        assert_eq!(res, expected_batch);

        assert!(stream.next().await.is_none());
    }

    #[tokio::test]
    async fn test_cut_merge_null() {
        let fields = Fields::from(vec![
            Field::new("column1", DataType::Int64, true),
            Field::new("column2", DataType::Int64, true),
        ]);
        let schema = SchemaRef::new(Schema::new(fields));

        let array11 = Arc::new(Int64Array::from_iter_values([1, 2, 3]));
        let array12 = Arc::new(Int64Array::from(vec![None, Some(1), Some(2)]));
        let record_batch1 = RecordBatch::try_new(schema.clone(), vec![array11, array12]).unwrap();
        let array21 = Arc::new(Int64Array::from_iter_values([2, 3, 4]));
        let array22 = Arc::new(Int64Array::from(vec![None, None, Some(3)]));
        let record_batch2 = RecordBatch::try_new(schema.clone(), vec![array21, array22]).unwrap();
        let array31 = Arc::new(Int64Array::from_iter_values([3, 4, 5]));
        let array32 = Arc::new(Int64Array::from(vec![Some(4), Some(5), Some(6)]));
        let record_batch3 = RecordBatch::try_new(schema.clone(), vec![array31, array32]).unwrap();

        let batches = vec![
            vec![record_batch1],
            vec![record_batch2],
            vec![record_batch3],
        ];
        let streams = SchemableMemoryBatchReaderStream::new_partitions(schema.clone(), batches);

        let mut stream = CutMergeStream::new(
            schema.clone(),
            streams,
            4096,
            "column1",
            cut_merge_metrices(),
        )
        .unwrap();

        let res = stream.next().await.unwrap().unwrap();
        let array1 = Arc::new(Int64Array::from_iter_values([1]));
        let array2 = Arc::new(Int64Array::from(vec![None]));
        let expected_batch = RecordBatch::try_new(schema.clone(), vec![array1, array2]).unwrap();
        assert_eq!(res, expected_batch);

        let res = stream.next().await.unwrap().unwrap();
        let array1 = Arc::new(Int64Array::from_iter_values([2]));
        let array2 = Arc::new(Int64Array::from_iter_values([1]));
        let expected_batch = RecordBatch::try_new(schema.clone(), vec![array1, array2]).unwrap();
        assert_eq!(res, expected_batch);

        let res = stream.next().await.unwrap().unwrap();
        let array1 = Arc::new(Int64Array::from_iter_values([3]));
        let array2 = Arc::new(Int64Array::from_iter_values([4]));
        let expected_batch = RecordBatch::try_new(schema.clone(), vec![array1, array2]).unwrap();
        assert_eq!(res, expected_batch);

        let res = stream.next().await.unwrap().unwrap();
        let array1 = Arc::new(Int64Array::from_iter_values([4]));
        let array2 = Arc::new(Int64Array::from_iter_values([5]));
        let expected_batch = RecordBatch::try_new(schema.clone(), vec![array1, array2]).unwrap();
        assert_eq!(res, expected_batch);

        let res = stream.next().await.unwrap().unwrap();
        let array1 = Arc::new(Int64Array::from_iter_values([5]));
        let array2 = Arc::new(Int64Array::from_iter_values([6]));
        let expected_batch = RecordBatch::try_new(schema.clone(), vec![array1, array2]).unwrap();
        assert_eq!(res, expected_batch);

        assert!(stream.next().await.is_none());
    }
}
