use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow::datatypes::{DataType, Field, Fields, Schema, SchemaRef};
use arrow_array::builder::StringBuilder;
use arrow_array::RecordBatch;
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricBuilder, Time};
use futures::{ready, Stream, StreamExt};
use models::{SeriesKey, Tag};

use super::metrics::BaselineMetrics;
use super::{
    BatchReader, BatchReaderRef, SchemableTskvRecordBatchStream,
    SendableSchemableTskvRecordBatchStream,
};
use crate::{Error, Result};

/// 添加 SeriesKey 对应的 tag 列到 RecordBatch
pub struct SeriesReader {
    skey: SeriesKey,
    input: BatchReaderRef,
    metrics: Arc<ExecutionPlanMetricsSet>,
}
impl SeriesReader {
    pub fn new(
        skey: SeriesKey,
        input: BatchReaderRef,
        metrics: Arc<ExecutionPlanMetricsSet>,
    ) -> Self {
        Self {
            skey,
            input,
            metrics,
        }
    }
}
impl BatchReader for SeriesReader {
    fn process(&self) -> Result<SendableSchemableTskvRecordBatchStream> {
        let input = self.input.process()?;

        let ori_schema = input.schema();

        let mut append_column = Vec::with_capacity(self.skey.tags().len());
        let mut append_column_values = Vec::with_capacity(self.skey.tags().len());
        for Tag { key, value } in self.skey.tags() {
            let field = Arc::new(Field::new(
                String::from_utf8(key.to_vec()).map_err(|err| Error::InvalidUtf8 {
                    message: format!("Convert tag {key:?}"),
                    source: err.utf8_error(),
                })?,
                DataType::Utf8,
                true,
            ));
            let array = String::from_utf8(value.to_vec()).map_err(|err| Error::InvalidUtf8 {
                message: format!("Convert tag {}'s value: {:?}", field.name(), value),
                source: err.utf8_error(),
            })?;
            append_column.push(field);
            append_column_values.push(array);
        }

        let new_fields = ori_schema
            .fields()
            .iter()
            .chain(append_column.iter())
            .cloned();
        let schema = Arc::new(Schema::new(Fields::from_iter(new_fields)));

        Ok(Box::pin(SeriesReaderStream {
            input,
            append_column_values,
            schema,
            metrics: SeriesReaderMetrics::new(self.metrics.as_ref()),
        }))
    }

    fn fmt_as(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "SeriesReader: series=[{}]", self.skey.string())
    }

    fn children(&self) -> Vec<BatchReaderRef> {
        vec![self.input.clone()]
    }
}

struct SeriesReaderStream {
    input: SendableSchemableTskvRecordBatchStream,
    append_column_values: Vec<String>,
    schema: SchemaRef,
    metrics: SeriesReaderMetrics,
}

impl SchemableTskvRecordBatchStream for SeriesReaderStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl SeriesReaderStream {
    fn poll_inner(&mut self, cx: &mut Context<'_>) -> Poll<Option<Result<RecordBatch>>> {
        match ready!(self.input.poll_next_unpin(cx)) {
            Some(Ok(batch)) => {
                // 记录补齐tag列所用时间
                let _timer = self.metrics.elapsed_complete_tag_columns_time().timer();

                let num_rows = batch.num_rows();

                let mut arrays = batch.columns().to_vec();
                for value in &self.append_column_values {
                    let mut builder =
                        StringBuilder::with_capacity(num_rows, value.as_bytes().len());
                    builder.extend(std::iter::repeat(Some(value)).take(num_rows));
                    let value_array = Arc::new(builder.finish());
                    arrays.push(value_array);
                }

                match RecordBatch::try_new(self.schema.clone(), arrays) {
                    Ok(batch) => Poll::Ready(Some(Ok(batch))),
                    Err(err) => Poll::Ready(Some(Err(err.into()))),
                }
            }
            Some(Err(e)) => Poll::Ready(Some(Err(e))),
            None => Poll::Ready(None),
        }
    }
}

impl Stream for SeriesReaderStream {
    type Item = Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let cloned_time = self.metrics.elapsed_compute().clone();
        let _timer = cloned_time.timer();

        let poll = self.poll_inner(cx);

        self.metrics.record_poll(poll)
    }
}

/// Stores metrics about the table writer execution.
#[derive(Debug, Clone)]
pub struct SeriesReaderMetrics {
    elapsed_complete_tag_columns_time: Time,
    inner: BaselineMetrics,
}

impl SeriesReaderMetrics {
    /// Create new metrics
    pub fn new(metrics: &ExecutionPlanMetricsSet) -> Self {
        let elapsed_complete_tag_columns_time =
            MetricBuilder::new(metrics).subset_time("elapsed_complete_tag_columns_time", 0);

        let inner = BaselineMetrics::new(metrics);

        Self {
            elapsed_complete_tag_columns_time,
            inner,
        }
    }

    pub fn elapsed_complete_tag_columns_time(&self) -> &Time {
        &self.elapsed_complete_tag_columns_time
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
