use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow::datatypes::{DataType, Field, Fields, Schema, SchemaRef};
use arrow_array::builder::StringBuilder;
use arrow_array::RecordBatch;
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricBuilder, Time};
use futures::{ready, Stream, StreamExt};
use models::datafusion::limit_record_batch::limit_record_batch;
use models::schema::tskv_table_schema::TskvTableSchemaRef;
use models::schema::COLUMN_ID_META_KEY;
use models::{ColumnId, SeriesKey, Tag};
use snafu::{IntoError, ResultExt};

use super::metrics::BaselineMetrics;
use super::{
    BatchReader, BatchReaderRef, SchemableTskvRecordBatchStream,
    SendableSchemableTskvRecordBatchStream,
};
use crate::error::{ArrowSnafu, InvalidUtf8Snafu, TagSnafu};
use crate::TskvResult;

/// 添加 SeriesKey 对应的 tag 列到 RecordBatch
pub struct SeriesReader {
    skey: SeriesKey,
    input: BatchReaderRef,
    query_schema: TskvTableSchemaRef,
    metrics: Arc<ExecutionPlanMetricsSet>,
    limit: Option<usize>,
}

impl SeriesReader {
    pub fn new(
        skey: SeriesKey,
        input: BatchReaderRef,
        query_schema: TskvTableSchemaRef,
        metrics: Arc<ExecutionPlanMetricsSet>,
        limit: Option<usize>,
    ) -> Self {
        Self {
            skey,
            input,
            query_schema,
            metrics,
            limit,
        }
    }
}

impl BatchReader for SeriesReader {
    fn process(&self) -> TskvResult<SendableSchemableTskvRecordBatchStream> {
        let input = self.input.process()?;

        let ori_schema = input.schema();

        let mut append_column = Vec::with_capacity(self.skey.tags().len());
        let mut append_column_values = Vec::with_capacity(self.skey.tags().len());
        for Tag { key, value } in self.skey.tags() {
            let column_id = std::str::from_utf8(key)
                .context(InvalidUtf8Snafu {
                    message: format!("Convert tag {key:?}"),
                })?
                .parse::<ColumnId>()
                .map_err(|err| {
                    TagSnafu {
                        reason: format!(
                            "Convert tag {key:?} to column id failed, because: {}",
                            err
                        ),
                    }
                    .build()
                })?;

            let name = match self.query_schema.column_name(column_id) {
                Some(name) => name.to_string(),
                None => {
                    continue;
                }
            };

            let mut field = Field::new(name, DataType::Utf8, true);
            field.set_metadata(HashMap::from([(
                COLUMN_ID_META_KEY.to_string(),
                column_id.to_string(),
            )]));

            let field = Arc::new(field);
            let array = String::from_utf8(value.to_vec()).map_err(|e| {
                InvalidUtf8Snafu {
                    message: format!("Convert tag {}'s value: {:?}", field.name(), value),
                }
                .into_error(e.utf8_error())
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
            remain: self.limit,
        }))
    }

    fn fmt_as(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "SeriesReader: series=[{}], limit={:#?}",
            self.skey.string(),
            self.limit
        )
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
    remain: Option<usize>,
}

impl SchemableTskvRecordBatchStream for SeriesReaderStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl SeriesReaderStream {
    fn poll_inner(&mut self, cx: &mut Context<'_>) -> Poll<Option<TskvResult<RecordBatch>>> {
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

                let batch = match RecordBatch::try_new(self.schema.clone(), arrays) {
                    Ok(batch) => batch,
                    Err(err) => return Poll::Ready(Some(Err(ArrowSnafu.into_error(err)))),
                };
                Poll::Ready(limit_record_batch(self.remain.as_mut(), batch).map(Ok))
            }
            Some(Err(e)) => Poll::Ready(Some(Err(e))),
            None => Poll::Ready(None),
        }
    }
}

impl Stream for SeriesReaderStream {
    type Item = TskvResult<RecordBatch>;

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
        poll: Poll<Option<TskvResult<RecordBatch>>>,
    ) -> Poll<Option<TskvResult<RecordBatch>>> {
        self.inner.record_poll(poll)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use arrow_array::{Float64Array, Int64Array, RecordBatch, StringArray, UInt64Array};
    use arrow_schema::TimeUnit;
    use datafusion::assert_batches_eq;
    use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
    use futures::TryStreamExt;
    use models::codec::Encoding;
    use models::schema::tskv_table_schema::{
        ColumnType, TableColumn, TskvTableSchema, TskvTableSchemaRef,
    };
    use models::{SeriesKey, Tag, ValueType};

    use crate::reader::series::SeriesReader;
    use crate::reader::{BatchReader, MemoryBatchReader};

    fn input_record_batchs() -> Vec<RecordBatch> {
        let batch = RecordBatch::try_new(
            input_schema(),
            vec![
                Arc::new(Int64Array::from(vec![-1, 2, 4, 18, 8])),
                Arc::new(StringArray::from(vec![
                    Some("z"),
                    Some("y"),
                    Some("x"),
                    Some("w"),
                    None,
                ])),
                Arc::new(Float64Array::from(vec![1.0, 2.0, 4.0, 18.0, 8.0])),
                Arc::new(UInt64Array::from(vec![1, 2, 4, 18, 8])),
            ],
        )
        .expect("create record batch");
        vec![batch]
    }

    fn input_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("time", DataType::Int64, true),
            Field::new("c3", DataType::Utf8, true),
            Field::new("c2", DataType::Float64, true),
            Field::new("c1", DataType::UInt64, true),
        ]))
    }

    fn query_schema() -> TskvTableSchemaRef {
        let schema = TskvTableSchema::new(
            "cnsodb".to_string(),
            "test".to_string(),
            "tbl".to_string(),
            vec![
                TableColumn::new(
                    0,
                    "time".to_string(),
                    ColumnType::Time(TimeUnit::Nanosecond),
                    Encoding::Default,
                ),
                TableColumn::new(1, "tag1".to_string(), ColumnType::Tag, Encoding::Default),
                TableColumn::new(2, "tag2".to_string(), ColumnType::Tag, Encoding::Default),
                TableColumn::new(
                    3,
                    "c3".to_string(),
                    ColumnType::Field(ValueType::String),
                    Encoding::Default,
                ),
                TableColumn::new(
                    4,
                    "c2".to_string(),
                    ColumnType::Field(ValueType::Float),
                    Encoding::Default,
                ),
                TableColumn::new(
                    5,
                    "c1".to_string(),
                    ColumnType::Field(ValueType::Unsigned),
                    Encoding::Default,
                ),
            ],
        );
        Arc::new(schema)
    }

    fn metrics() -> Arc<ExecutionPlanMetricsSet> {
        Arc::new(ExecutionPlanMetricsSet::new())
    }

    #[tokio::test]
    async fn test() {
        let reader = Arc::new(MemoryBatchReader::new(
            input_schema(),
            input_record_batchs(),
        ));

        let series_key = SeriesKey {
            tags: vec![
                Tag::new("1".as_bytes().to_vec(), "t_val1".as_bytes().to_vec()),
                Tag::new("2".as_bytes().to_vec(), "t_val2".as_bytes().to_vec()),
            ],
            table: "tbl".to_string(),
        };
        let reader = SeriesReader::new(series_key, reader, query_schema(), metrics(), None);

        let stream = reader.process().expect("reader");

        let result = stream.try_collect::<Vec<_>>().await.unwrap();

        let expected = [
            "+------+----+------+----+--------+--------+",
            "| time | c3 | c2   | c1 | tag1   | tag2   |",
            "+------+----+------+----+--------+--------+",
            "| -1   | z  | 1.0  | 1  | t_val1 | t_val2 |",
            "| 2    | y  | 2.0  | 2  | t_val1 | t_val2 |",
            "| 4    | x  | 4.0  | 4  | t_val1 | t_val2 |",
            "| 18   | w  | 18.0 | 18 | t_val1 | t_val2 |",
            "| 8    |    | 8.0  | 8  | t_val1 | t_val2 |",
            "+------+----+------+----+--------+--------+",
        ];

        assert_batches_eq!(expected, &result);
    }
}
