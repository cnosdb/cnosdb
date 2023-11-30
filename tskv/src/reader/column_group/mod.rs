pub mod statistics;

use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow::compute::kernels::cast;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow_array::{ArrayRef, RecordBatch};
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricBuilder, Time};
use futures::{ready, Stream, StreamExt};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use trace::warn;

use super::metrics::BaselineMetrics;
use super::page::{PageReaderRef, PrimitiveArrayReader};
use super::{
    BatchReader, BatchReaderRef, Projection, SchemableTskvRecordBatchStream,
    SendableSchemableTskvRecordBatchStream,
};
use crate::tsm2::page::{ColumnGroup, PageWriteSpec};
use crate::tsm2::reader::TSM2Reader;
use crate::Result;

pub struct ColumnGroupReader {
    column_group: Arc<ColumnGroup>,
    page_readers: Vec<PageReaderRef>,
    schema: SchemaRef,
    metrics: Arc<ExecutionPlanMetricsSet>,
}
impl ColumnGroupReader {
    pub fn try_new(
        reader: Arc<TSM2Reader>,
        column_group: Arc<ColumnGroup>,
        projection: &Projection,
        _batch_size: usize,
        metrics: Arc<ExecutionPlanMetricsSet>,
    ) -> Result<Self> {
        let columns = projection.iter().filter_map(|e| {
            column_group
                .pages()
                .iter()
                .find(|e2| &e2.meta().column.name == e)
        });

        let page_readers = columns
            .clone()
            .map(|e| build_reader(reader.clone(), e))
            .collect::<Result<Vec<_>>>()?;

        let fields = columns
            .map(|e| &e.meta().column)
            .map(Field::from)
            .collect::<Vec<_>>();

        let schema = Arc::new(Schema::new(fields));

        Ok(Self {
            column_group,
            page_readers,
            schema,
            metrics,
        })
    }

    pub fn new_with_unchecked(
        column_group: Arc<ColumnGroup>,
        page_readers: Vec<PageReaderRef>,
        schema: SchemaRef,
    ) -> Self {
        Self {
            column_group,
            page_readers,
            schema,
            metrics: Arc::new(ExecutionPlanMetricsSet::new()),
        }
    }
}

impl BatchReader for ColumnGroupReader {
    fn process(&self) -> Result<SendableSchemableTskvRecordBatchStream> {
        let streams = self
            .page_readers
            .iter()
            .map(|r| r.process())
            .collect::<Result<Vec<_>>>()?;

        let metrics = ColumnGroupReaderMetrics::new(self.metrics.as_ref());

        let (join_handles, buffers): (Vec<_>, Vec<_>) = streams
            .into_iter()
            .map(|mut s| {
                let (sender, receiver) = mpsc::channel::<Result<ArrayRef>>(1);

                let metrics = metrics.clone();
                let task = async move {
                    // 记录读取 page 的时间
                    let _timer = metrics.elapsed_page_scan_time().timer();
                    while let Some(item) = s.next().await {
                        let exit = item.is_err();
                        // If send fails, stream being torn down,
                        // there is no place to send the error.
                        if sender.send(item).await.is_err() {
                            warn!("Stopping execution: output is gone, PageReader cancelling");
                            return;
                        }
                        if exit {
                            return;
                        }
                    }
                };
                let join_handle = tokio::spawn(task);
                (join_handle, receiver)
            })
            .unzip();

        Ok(Box::pin(ColumnGroupRecordBatchStream {
            schema: self.schema.clone(),
            column_arrays: Vec::with_capacity(self.schema.fields().len()),
            buffers,
            join_handles,
            metrics: ColumnGroupReaderMetrics::new(self.metrics.as_ref()),
        }))
    }

    fn fmt_as(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let column_group_id = self.column_group.column_group_id();
        let pages_offset = self.column_group.pages_offset();
        let column_group_bytes = self.column_group.size();

        write!(
            f,
            "ColumnGroupReader: column_group_id={column_group_id}, pages_offset={pages_offset}, column_group_bytes={column_group_bytes}"
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
    buffers: Vec<mpsc::Receiver<Result<ArrayRef>>>,
    join_handles: Vec<JoinHandle<()>>,

    metrics: ColumnGroupReaderMetrics,
}

impl SchemableTskvRecordBatchStream for ColumnGroupRecordBatchStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl ColumnGroupRecordBatchStream {
    fn poll_inner(&mut self, cx: &mut Context<'_>) -> Poll<Option<Result<RecordBatch>>> {
        let schema = self.schema.clone();
        let column_nums = self.buffers.len();

        loop {
            let next_column_idx = self.column_arrays.len();
            let target_type = self.schema.field(next_column_idx).data_type().clone();

            match ready!(self.buffers[next_column_idx].poll_recv(cx)) {
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
                            RecordBatch::try_new(schema, arrays).map_err(Into::into),
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
    type Item = Result<RecordBatch>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let poll = self.poll_inner(cx);
        self.metrics.record_poll(poll)
    }
}

/// Stores metrics about the table writer execution.
#[derive(Debug, Clone)]
pub struct ColumnGroupReaderMetrics {
    elapsed_page_scan_time: Time,
    inner: BaselineMetrics,
}

impl ColumnGroupReaderMetrics {
    /// Create new metrics
    pub fn new(metrics: &ExecutionPlanMetricsSet) -> Self {
        let elapsed_page_scan_time =
            MetricBuilder::new(metrics).subset_time("elapsed_page_scan_time", 0);

        let inner = BaselineMetrics::new(metrics);

        Self {
            elapsed_page_scan_time,
            inner,
        }
    }

    pub fn elapsed_page_scan_time(&self) -> &Time {
        &self.elapsed_page_scan_time
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

fn convert_data_type_if_necessary(array: ArrayRef, target_type: &DataType) -> Result<ArrayRef> {
    if array.data_type() != target_type {
        match cast::cast(&array, target_type) {
            Ok(array) => Ok(array),
            Err(e) => Err(e.into()),
        }
    } else {
        Ok(array)
    }
}

fn build_reader(reader: Arc<TSM2Reader>, page_meta: &PageWriteSpec) -> Result<PageReaderRef> {
    // TODO 根据指定列及其元数据和文件读取器，构造 PageReader
    let data_type = page_meta.meta().column.column_type.to_physical_data_type();
    Ok(Arc::new(PrimitiveArrayReader::new(
        data_type, reader, page_meta,
    )))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::assert_batches_eq;
    use futures::TryStreamExt;

    use crate::reader::column_group::ColumnGroupReader;
    use crate::reader::page::tests::TestPageReader;
    use crate::reader::page::PageReaderRef;
    use crate::reader::BatchReader;
    use crate::tsm2::page::ColumnGroup;

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

        let column_group_reader = ColumnGroupReader::new_with_unchecked(
            Arc::new(ColumnGroup::new(0)),
            page_readers,
            schema,
        );

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
