pub mod statistics;

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow::compute::kernels::cast;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow_array::{ArrayRef, RecordBatch};
use datafusion::physical_plan::metrics::{Count, ExecutionPlanMetricsSet, MetricBuilder, Time};
use futures::{ready, Stream, StreamExt};
use models::arrow::stream::BoxStream;
use models::predicate::domain::TimeRange;
use models::{ColumnId, SeriesId};
use snafu::ResultExt;

use super::metrics::BaselineMetrics;
use super::page::{PageReaderRef, PrimitiveArrayReader};
use super::{
    BatchReader, BatchReaderRef, SchemableTskvRecordBatchStream,
    SendableSchemableTskvRecordBatchStream,
};
use crate::error::ArrowSnafu;
use crate::tsm::column_group::ColumnGroup;
use crate::tsm::page::PageWriteSpec;
use crate::tsm::reader::TsmReader;
use crate::TskvResult;

pub struct ColumnGroupReader {
    column_group: Arc<ColumnGroup>,
    page_readers: Vec<PageReaderRef>,
    schema: SchemaRef,
    metrics: Arc<ExecutionPlanMetricsSet>,
}
impl ColumnGroupReader {
    pub fn try_new(
        reader: Arc<TsmReader>,
        series_id: SeriesId,
        column_group: Arc<ColumnGroup>,
        projection: &[ColumnId],
        schema_meta: HashMap<String, String>,
        _batch_size: usize,
        metrics: Arc<ExecutionPlanMetricsSet>,
    ) -> TskvResult<Self> {
        let columns = projection.iter().filter_map(|e| {
            column_group
                .pages()
                .iter()
                .find(|e2| e2.meta().column.id == *e)
        });

        let time_page = Arc::new(column_group.time_page_write_spec()?);
        let reader_builder = ReaderBuilder::new(
            reader,
            series_id,
            time_page,
            *column_group.time_range(),
            metrics.clone(),
        );

        let page_readers = columns
            .clone()
            .map(|e| reader_builder.build(e))
            .collect::<TskvResult<Vec<_>>>()?;

        let fields = columns
            .map(|e| &e.meta().column)
            .map(Field::from)
            .collect::<Vec<_>>();

        let schema = Arc::new(Schema::new_with_metadata(fields, schema_meta));

        Ok(Self {
            column_group,
            page_readers,
            schema,
            metrics,
        })
    }
}

impl BatchReader for ColumnGroupReader {
    fn process(&self) -> TskvResult<SendableSchemableTskvRecordBatchStream> {
        let streams = self
            .page_readers
            .iter()
            .map(|r| r.process())
            .collect::<TskvResult<Vec<_>>>()?;

        Ok(Box::pin(ColumnGroupRecordBatchStream {
            schema: self.schema.clone(),
            column_arrays: Vec::with_capacity(self.schema.fields().len()),
            streams,
            metrics: ColumnGroupReaderMetrics::new(self.metrics.as_ref()),
        }))
    }

    fn fmt_as(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let column_group_id = self.column_group.column_group_id();
        let pages_offset = self.column_group.pages_offset();
        let column_group_bytes = self.column_group.size();
        let time_range = self.column_group.time_range();

        write!(
            f,
            "ColumnGroupReader: column_group_id={column_group_id}, time_range={time_range}, pages_offset={pages_offset}, column_group_bytes={column_group_bytes}"
        )
    }

    fn children(&self) -> Vec<BatchReaderRef> {
        vec![]
    }
}

struct ColumnGroupRecordBatchStream {
    schema: SchemaRef,

    /// Stream entries
    column_arrays: Vec<ArrayRef>,
    streams: Vec<BoxStream<TskvResult<ArrayRef>>>,

    metrics: ColumnGroupReaderMetrics,
}

impl SchemableTskvRecordBatchStream for ColumnGroupRecordBatchStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl ColumnGroupRecordBatchStream {
    fn poll_inner(&mut self, cx: &mut Context<'_>) -> Poll<Option<TskvResult<RecordBatch>>> {
        let schema = self.schema.clone();
        let column_nums = self.streams.len();

        loop {
            let next_column_idx = self.column_arrays.len();
            let target_type = self.schema.field(next_column_idx).data_type().clone();

            match ready!(self.streams[next_column_idx].poll_next_unpin(cx)) {
                Some(Ok(array)) => {
                    let arrays = &mut self.column_arrays;

                    // 如果类型不匹配，需要进行类型转换
                    match convert_data_type_if_necessary(array, &target_type) {
                        Ok(array) => {
                            arrays.push(array);
                        }
                        Err(e) => {
                            return Poll::Ready(Some(Err(e)));
                        }
                    }

                    if arrays.len() == column_nums {
                        // 可以构造 RecordBatch
                        let arrays = std::mem::take(arrays);
                        return Poll::Ready(Some(
                            RecordBatch::try_new(schema, arrays).context(ArrowSnafu),
                        ));
                    }
                    continue;
                }
                Some(Err(e)) => {
                    return Poll::Ready(Some(Err(e)));
                }
                None => {
                    return Poll::Ready(None);
                }
            }
        }
    }
}

impl Stream for ColumnGroupRecordBatchStream {
    type Item = TskvResult<RecordBatch>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let poll = self.poll_inner(cx);
        self.metrics.record_poll(poll)
    }
}

/// Stores metrics about the table writer execution.
#[derive(Debug, Clone)]
pub struct ColumnGroupReaderMetrics {
    elapsed_page_scan_time: Time,
    elapsed_page_to_array_time: Time,
    page_read_count: Count,
    page_read_bytes: Count,
    inner: BaselineMetrics,
}

impl ColumnGroupReaderMetrics {
    /// Create new metrics
    pub fn new(metrics: &ExecutionPlanMetricsSet) -> Self {
        let elapsed_page_scan_time =
            MetricBuilder::new(metrics).subset_time("elapsed_page_scan_time", 0);
        let elapsed_page_to_array_time =
            MetricBuilder::new(metrics).subset_time("elapsed_page_to_array_time", 0);
        let page_read_count = MetricBuilder::new(metrics).counter("page_read_count", 0);
        let page_read_bytes = MetricBuilder::new(metrics).counter("page_read_bytes", 0);

        let inner = BaselineMetrics::new(metrics);

        Self {
            elapsed_page_scan_time,
            elapsed_page_to_array_time,
            page_read_count,
            page_read_bytes,
            inner,
        }
    }

    pub fn elapsed_page_scan_time(&self) -> &Time {
        &self.elapsed_page_scan_time
    }

    pub fn elapsed_page_to_array_time(&self) -> &Time {
        &self.elapsed_page_to_array_time
    }

    pub fn page_read_count(&self) -> &Count {
        &self.page_read_count
    }

    pub fn page_read_bytes(&self) -> &Count {
        &self.page_read_bytes
    }

    pub fn record_poll(
        &self,
        poll: Poll<Option<TskvResult<RecordBatch>>>,
    ) -> Poll<Option<TskvResult<RecordBatch>>> {
        self.inner.record_poll(poll)
    }
}

fn convert_data_type_if_necessary(array: ArrayRef, target_type: &DataType) -> TskvResult<ArrayRef> {
    if array.data_type() != target_type {
        cast::cast(&array, target_type).context(ArrowSnafu)
    } else {
        Ok(array)
    }
}

pub struct ReaderBuilder {
    reader: Arc<TsmReader>,
    series_id: SeriesId,
    time_page_meta: Arc<PageWriteSpec>,
    time_range: TimeRange,
    metrics: Arc<ExecutionPlanMetricsSet>,
}

impl ReaderBuilder {
    pub fn new(
        reader: Arc<TsmReader>,
        series_id: SeriesId,
        time_page_meta: Arc<PageWriteSpec>,
        time_range: TimeRange,
        metrics: Arc<ExecutionPlanMetricsSet>,
    ) -> Self {
        Self {
            reader,
            series_id,
            time_page_meta,
            time_range,
            metrics,
        }
    }

    pub fn build(&self, page_meta: &PageWriteSpec) -> TskvResult<PageReaderRef> {
        Ok(Arc::new(PrimitiveArrayReader::new(
            self.reader.clone(),
            page_meta,
            self.series_id,
            self.time_page_meta.clone(),
            self.time_range,
            self.metrics.clone(),
        )))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::assert_batches_eq;
    use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
    use futures::TryStreamExt;

    use crate::reader::column_group::ColumnGroupReader;
    use crate::reader::page::tests::TestPageReader;
    use crate::reader::page::PageReaderRef;
    use crate::reader::BatchReader;
    use crate::tsm::column_group::ColumnGroup;

    #[tokio::test]
    async fn test_column_group_reader() {
        let page_readers: Vec<PageReaderRef> = vec![
            Arc::new(TestPageReader::<i64>::new(9)),
            Arc::new(TestPageReader::<u64>::new(9)),
            Arc::new(TestPageReader::<f64>::new(9)),
            Arc::new(TestPageReader::<String>::new(9)),
            Arc::new(TestPageReader::<bool>::new(9)),
        ];

        let schema = Arc::new(Schema::new(vec![
            Field::new("time", DataType::Int64, true),
            Field::new("c1", DataType::UInt64, true),
            Field::new("c2", DataType::Float64, true),
            Field::new("c3", DataType::Utf8, true),
            Field::new("c4", DataType::Boolean, true),
        ]));

        let column_group_reader = ColumnGroupReader {
            column_group: Arc::new(ColumnGroup::new(0)),
            page_readers,
            schema,
            metrics: Arc::new(ExecutionPlanMetricsSet::new()),
        };

        let stream = column_group_reader.process().expect("chunk_reader");

        let result = stream.try_collect::<Vec<_>>().await.unwrap();

        let expected = [
            "+------+----+-----+-------+-------+",
            "| time | c1 | c2  | c3    | c4    |",
            "+------+----+-----+-------+-------+",
            "|      |    |     |       |       |",
            "| 1    | 1  | 1.0 | str_1 | false |",
            "|      |    |     |       |       |",
            "| 3    | 3  | 3.0 | str_3 | true  |",
            "|      |    |     |       |       |",
            "| 5    | 5  | 5.0 | str_5 | false |",
            "|      |    |     |       |       |",
            "| 7    | 7  | 7.0 | str_7 | false |",
            "|      |    |     |       |       |",
            "+------+----+-----+-------+-------+",
        ];

        assert_batches_eq!(expected, &result);
    }
}
