use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow::datatypes::{DataType, Field, Fields, Schema, SchemaRef};
use arrow_array::types::Int32Type;
use arrow_array::{ArrayRef, PrimitiveArray, RecordBatch, RunArray};
use datafusion::scalar::ScalarValue;
use futures::{ready, Stream, StreamExt};
use models::{SeriesKey, Tag};

use super::{
    BatchReader, BatchReaderRef, SchemableTskvRecordBatchStream,
    SendableSchemableTskvRecordBatchStream,
};
use crate::{Error, Result};

/// 添加 SeriesKey 对应的 tag 列到 RecordBatch
pub struct SeriesReader {
    skey: SeriesKey,
    input: BatchReaderRef,
}
impl SeriesReader {
    pub fn new(skey: SeriesKey, input: BatchReaderRef) -> Self {
        Self { skey, input }
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
            let array =
                ScalarValue::Utf8(Some(String::from_utf8(value.to_vec()).map_err(|err| {
                    Error::InvalidUtf8 {
                        message: format!("Convert tag {}'s value: {:?}", field.name(), value),
                        source: err.utf8_error(),
                    }
                })?))
                .to_array();
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
        }))
    }
}

struct SeriesReaderStream {
    input: SendableSchemableTskvRecordBatchStream,
    append_column_values: Vec<ArrayRef>,
    schema: SchemaRef,
}

impl SchemableTskvRecordBatchStream for SeriesReaderStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl Stream for SeriesReaderStream {
    type Item = Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match ready!(self.input.poll_next_unpin(cx)) {
            Some(Ok(batch)) => {
                let num_rows = batch.num_rows() as i32;

                let run_ends = PrimitiveArray::<Int32Type>::from_iter_values([num_rows]);

                let mut arrays = batch.columns().to_vec();
                for value in &self.append_column_values {
                    let value_array = Arc::new(RunArray::try_new(&run_ends, value.as_ref())?);
                    arrays.push(value_array);
                }
                let batch = RecordBatch::try_new(self.schema.clone(), arrays).unwrap();
                Poll::Ready(Some(Ok(batch)))
            }
            Some(Err(e)) => Poll::Ready(Some(Err(e))),
            None => Poll::Ready(None),
        }
    }
}
