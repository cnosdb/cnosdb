pub mod statistics;

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow::datatypes::{Field, Schema, SchemaRef};
use arrow_array::RecordBatch;
use datafusion::physical_plan::metrics::{Count, ExecutionPlanMetricsSet, MetricBuilder, Time};
use futures::{ready, Stream, StreamExt};
use models::arrow::stream::BoxStream;
use models::{ColumnId, SeriesId};

use super::metrics::BaselineMetrics;
use super::{
    BatchReader, BatchReaderRef, SchemableTskvRecordBatchStream,
    SendableSchemableTskvRecordBatchStream,
};
use crate::tsm::column_group::ColumnGroup;
use crate::tsm::page::PageWriteSpec;
use crate::tsm::reader::{decode_pages, TsmReader};
use crate::TskvResult;

pub struct ColumnGroupReader {
    column_group: Arc<ColumnGroup>,
    reader: Arc<TsmReader>,
    series_id: SeriesId,
    pages_meta: Vec<PageWriteSpec>,
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
        let pages_meta = projection
            .iter()
            .filter_map(|e| {
                column_group
                    .pages()
                    .iter()
                    .find(|e2| e2.meta().column.id == *e)
            })
            .cloned()
            .collect::<Vec<_>>();

        let fields = pages_meta
            .iter()
            .map(|e| &e.meta().column)
            .map(Field::from)
            .collect::<Vec<_>>();

        let schema = Arc::new(Schema::new_with_metadata(fields, schema_meta));

        Ok(Self {
            column_group,
            reader,
            series_id,
            pages_meta,
            schema,
            metrics,
        })
    }
}

impl BatchReader for ColumnGroupReader {
    fn process(&self) -> TskvResult<SendableSchemableTskvRecordBatchStream> {
        let stream = Box::pin(futures::stream::once(read(
            self.reader.clone(),
            self.series_id,
            self.pages_meta.clone(),
            self.schema.metadata().clone(),
            ColumnGroupReaderMetrics::new(self.metrics.as_ref()),
        )));

        Ok(Box::pin(ColumnGroupRecordBatchStream {
            schema: self.schema.clone(),
            stream,
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
    stream: BoxStream<TskvResult<RecordBatch>>,

    metrics: ColumnGroupReaderMetrics,
}

impl SchemableTskvRecordBatchStream for ColumnGroupRecordBatchStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl ColumnGroupRecordBatchStream {
    fn poll_inner(&mut self, cx: &mut Context<'_>) -> Poll<Option<TskvResult<RecordBatch>>> {
        match ready!(self.stream.poll_next_unpin(cx)) {
            Some(Ok(record_batch)) => Poll::Ready(Some(Ok(record_batch))),
            Some(Err(e)) => Poll::Ready(Some(Err(e))),
            None => Poll::Ready(None),
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
    elapsed_pages_to_record_batch_time: Time,
    page_read_count: Count,
    page_read_bytes: Count,
    inner: BaselineMetrics,
}

impl ColumnGroupReaderMetrics {
    /// Create new metrics
    pub fn new(metrics: &ExecutionPlanMetricsSet) -> Self {
        let elapsed_page_scan_time =
            MetricBuilder::new(metrics).subset_time("elapsed_page_scan_time", 0);
        let elapsed_pages_to_record_batch_time =
            MetricBuilder::new(metrics).subset_time("elapsed_page_to_array_time", 0);
        let page_read_count = MetricBuilder::new(metrics).counter("page_read_count", 0);
        let page_read_bytes = MetricBuilder::new(metrics).counter("page_read_bytes", 0);

        let inner = BaselineMetrics::new(metrics);

        Self {
            elapsed_page_scan_time,
            elapsed_pages_to_record_batch_time,
            page_read_count,
            page_read_bytes,
            inner,
        }
    }

    pub fn elapsed_page_scan_time(&self) -> &Time {
        &self.elapsed_page_scan_time
    }

    pub fn elapsed_pages_to_record_batch_time(&self) -> &Time {
        &self.elapsed_pages_to_record_batch_time
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

async fn read(
    reader: Arc<TsmReader>,
    series_id: SeriesId,
    pages_meta: Vec<PageWriteSpec>,
    schema_meta: HashMap<String, String>,
    metrics: ColumnGroupReaderMetrics,
) -> TskvResult<RecordBatch> {
    let mut sorted_pages = pages_meta.clone();
    sorted_pages.sort_by_key(|p| p.offset());

    let merged_reads = merge_adjacent_pages(&sorted_pages);

    let mut pages = Vec::with_capacity(pages_meta.len());
    for batch in merged_reads {
        let _timer = metrics.elapsed_page_scan_time().timer();
        metrics.page_read_count().add(batch.len());
        let total_size: usize = batch.iter().map(|p| p.size() as usize).sum();
        metrics.page_read_bytes().add(total_size);
        let batch_pages = reader.read_adjacent_pages(&batch).await?;
        pages.extend(batch_pages);
    }

    let _timer = metrics.elapsed_pages_to_record_batch_time().timer();
    let record_batch = decode_pages(pages, schema_meta, Some((reader.tombstone(), series_id)))?;
    Ok(record_batch)
}

fn merge_adjacent_pages(pages: &[PageWriteSpec]) -> Vec<Vec<PageWriteSpec>> {
    let mut result = Vec::new();
    let mut current_batch = Vec::new();
    let mut last_end = 0;

    for page in pages {
        if current_batch.is_empty() || page.offset() == last_end {
            current_batch.push(page.clone());
            last_end = page.offset() + page.size();
        } else {
            result.push(std::mem::take(&mut current_batch));
            current_batch.push(page.clone());
            last_end = page.offset() + page.size();
        }
    }

    if !current_batch.is_empty() {
        result.push(current_batch);
    }

    result
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{
        ArrayRef, BooleanArray, Float64Array, RecordBatch, StringArray, TimestampNanosecondArray,
        UInt64Array,
    };
    use arrow_schema::TimeUnit;
    use datafusion::assert_batches_eq;
    use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
    use futures::TryStreamExt;
    use models::codec::Encoding;
    use models::schema::tskv_table_schema::{ColumnType, TableColumn, TskvTableSchema};
    use models::{SeriesKey, ValueType};

    use crate::reader::column_group::ColumnGroupReader;
    use crate::reader::BatchReader;
    use crate::tsm::reader::TsmReader;
    use crate::tsm::writer::TsmWriter;

    #[tokio::test]
    async fn test_column_group_reader() {
        let path = "/tmp/test/tskv/reader/column_group/mod/test_column_group_reader".to_string();

        let schema = TskvTableSchema::new(
            "cnosdb".to_string(),
            "public".to_string(),
            "test0".to_string(),
            vec![
                TableColumn::new(
                    0,
                    "time".to_string(),
                    ColumnType::Time(TimeUnit::Nanosecond),
                    Encoding::default(),
                ),
                TableColumn::new(
                    1,
                    "c1".to_string(),
                    ColumnType::Field(ValueType::Unsigned),
                    Encoding::default(),
                ),
                TableColumn::new(
                    2,
                    "c2".to_string(),
                    ColumnType::Field(ValueType::Float),
                    Encoding::default(),
                ),
                TableColumn::new(
                    3,
                    "c3".to_string(),
                    ColumnType::Field(ValueType::String),
                    Encoding::default(),
                ),
                TableColumn::new(
                    4,
                    "c4".to_string(),
                    ColumnType::Field(ValueType::Boolean),
                    Encoding::default(),
                ),
            ],
        );
        let schema = Arc::new(schema);
        let mut tsm_writer = TsmWriter::open(&path, 0, 0, false)
            .await
            .expect("tsm_writer");
        let df_schema = schema.to_arrow_schema();
        let data1 = RecordBatch::try_new(
            df_schema.clone(),
            vec![
                Arc::new(TimestampNanosecondArray::from(vec![1, 3, 5, 7])) as ArrayRef,
                Arc::new(UInt64Array::from(vec![1, 3, 5, 7])) as ArrayRef,
                Arc::new(Float64Array::from(vec![1.0, 3.0, 5.0, 7.0])) as ArrayRef,
                Arc::new(StringArray::from(vec!["str_1", "str_3", "str_5", "str_7"])) as ArrayRef,
                Arc::new(BooleanArray::from(vec![false, true, false, false])) as ArrayRef,
            ],
        )
        .unwrap();
        tsm_writer
            .write_record_batch(1, SeriesKey::default(), schema, data1)
            .await
            .expect("write_record_batch");
        tsm_writer.finish().await.expect("finish");
        let path = tsm_writer.path().to_path_buf();
        drop(tsm_writer);

        let tsm_reader = TsmReader::open(path).await.expect("tsm_reader");
        let column_group = tsm_reader
            .chunk()
            .get(&1)
            .expect("column_group")
            .clone()
            .column_group()
            .get(&0)
            .expect("column_group")
            .clone();
        let pages_meta = column_group.pages().to_vec();

        let column_group_reader = ColumnGroupReader {
            column_group,
            reader: Arc::new(tsm_reader),
            series_id: 0,
            pages_meta,
            schema: df_schema,
            metrics: Arc::new(ExecutionPlanMetricsSet::new()),
        };

        let stream = column_group_reader.process().expect("chunk_reader");

        let result = stream.try_collect::<Vec<_>>().await.unwrap();

        let expected = [
            "+-------------------------------+----+-----+-------+-------+",
            "| time                          | c1 | c2  | c3    | c4    |",
            "+-------------------------------+----+-----+-------+-------+",
            "| 1970-01-01T00:00:00.000000001 | 1  | 1.0 | str_1 | false |",
            "| 1970-01-01T00:00:00.000000003 | 3  | 3.0 | str_3 | true  |",
            "| 1970-01-01T00:00:00.000000005 | 5  | 5.0 | str_5 | false |",
            "| 1970-01-01T00:00:00.000000007 | 7  | 7.0 | str_7 | false |",
            "+-------------------------------+----+-----+-------+-------+",
        ];

        assert_batches_eq!(expected, &result);
    }
}
