use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow::datatypes::{Schema, SchemaRef};
use arrow_array::{Int64Array, RecordBatch};
use futures::Stream;
use models::predicate::domain::PushedAggregateFunction;
use snafu::ResultExt;

use super::{
    BatchReader, BatchReaderRef, DataReference, SchemableTskvRecordBatchStream,
    SendableSchemableTskvRecordBatchStream,
};
use crate::error::ArrowSnafu;
use crate::tsm::chunk::Chunk;
use crate::TskvResult;

pub struct PushDownAggregateReader {
    df_schema: Arc<Schema>,
    aggregate: PushedAggregateFunction,
    chunk: DataReference,
}
impl PushDownAggregateReader {
    pub fn try_new(
        df_schema: Arc<Schema>,
        aggregates: PushedAggregateFunction,
        chunk: DataReference,
    ) -> TskvResult<Self> {
        Ok(Self {
            df_schema,
            aggregate: aggregates,
            chunk,
        })
    }
    fn get_rows_number_by_column_name(&self, chunk: &Arc<Chunk>, col_name: &str) -> i64 {
        let mut count: i64 = 0;
        for cg in chunk.column_group().values().collect::<Vec<_>>() {
            cg.pages().iter().for_each(|page| {
                if page.meta().column.name == col_name {
                    count += page.meta().num_values as i64;
                }
            });
        }
        count
    }
}

impl BatchReader for PushDownAggregateReader {
    fn process(&self) -> TskvResult<SendableSchemableTskvRecordBatchStream> {
        match &self.aggregate {
            PushedAggregateFunction::Count(col_name) => {
                let num_count = match &self.chunk {
                    DataReference::Chunk(chunk, ..) => {
                        self.get_rows_number_by_column_name(chunk, col_name.as_str())
                    }
                    DataReference::Memcache(series_data, ..) => {
                        let mut num_count: i64 = 0;
                        series_data.read().groups.iter().for_each(|group| {
                            num_count += group.rows.get_ref_rows().len() as i64;
                        });
                        num_count
                    }
                };

                Ok(Box::pin(PushDownAggregateStream {
                    schema: self.df_schema.clone(),
                    num_count,
                    is_get: false,
                }))
            }
        }
    }

    fn fmt_as(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "PushDownAggregateReader")
    }

    fn children(&self) -> Vec<BatchReaderRef> {
        vec![]
    }
}

struct PushDownAggregateStream {
    schema: SchemaRef,
    num_count: i64,
    is_get: bool,
}

impl SchemableTskvRecordBatchStream for PushDownAggregateStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl PushDownAggregateStream {
    fn poll_inner(&mut self, _cx: &mut Context<'_>) -> Poll<Option<TskvResult<RecordBatch>>> {
        let schema = self.schema.clone();

        if !self.is_get {
            self.is_get = true;
            let num_count = Int64Array::from(vec![self.num_count]);
            Poll::Ready(Some(
                RecordBatch::try_new(schema, vec![Arc::new(num_count)]).context(ArrowSnafu),
            ))
        } else {
            Poll::Ready(None)
        }
    }
}

impl Stream for PushDownAggregateStream {
    type Item = TskvResult<RecordBatch>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.poll_inner(cx)
    }
}
