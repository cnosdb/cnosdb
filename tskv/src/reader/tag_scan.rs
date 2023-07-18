use std::pin::Pin;
use std::task::{Context, Poll};

use datafusion::arrow::array::StringBuilder;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::error::ArrowError;
use datafusion::arrow::record_batch::RecordBatch;
use futures::future::BoxFuture;
use futures::{ready, Stream, StreamExt, TryFutureExt};
use models::arrow::stream::MemoryRecordBatchStream;
use models::arrow_array::build_arrow_array_builders;
use models::meta_data::VnodeId;
use models::SeriesKey;
use trace::SpanRecorder;

use crate::error::Result;
use crate::reader::{QueryOption, SendableTskvRecordBatchStream};
use crate::EngineRef;

pub struct LocalTskvTagScanStream {
    state: StreamState,
    #[allow(unused)]
    span_recorder: SpanRecorder,
}

impl LocalTskvTagScanStream {
    pub fn new(
        vnode_id: VnodeId,
        option: QueryOption,
        kv: EngineRef,
        span_recorder: SpanRecorder,
    ) -> Self {
        let futrue = async move {
            let (tenant, db, table) = (
                option.table_schema.tenant.as_str(),
                option.table_schema.db.as_str(),
                option.table_schema.name.as_str(),
            );

            let mut keys = Vec::new();

            for series_id in kv
                .get_series_id_by_filter(tenant, db, table, vnode_id, option.split.tags_filter())
                .await?
                .into_iter()
            {
                if let Some(key) = kv.get_series_key(tenant, db, vnode_id, series_id).await? {
                    keys.push(key)
                }
            }

            let mut batches = vec![];
            for chunk in keys.chunks(option.batch_size) {
                let record_batch = series_keys_to_record_batch(option.df_schema.clone(), chunk)?;
                batches.push(record_batch)
            }

            Ok(Box::pin(MemoryRecordBatchStream::new(batches)) as SendableTskvRecordBatchStream)
        };

        let state = StreamState::Open(Box::pin(futrue));

        Self {
            state,
            span_recorder,
        }
    }

    fn poll_inner(&mut self, cx: &mut Context<'_>) -> Poll<Option<Result<RecordBatch>>> {
        loop {
            match &mut self.state {
                StreamState::Open(future) => match ready!(future.try_poll_unpin(cx)) {
                    Ok(stream) => {
                        self.state = StreamState::Scan(stream);
                    }
                    Err(err) => return Poll::Ready(Some(Err(err))),
                },
                StreamState::Scan(stream) => return stream.poll_next_unpin(cx),
            }
        }
    }
}

impl Stream for LocalTskvTagScanStream {
    type Item = Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.poll_inner(cx)
    }
}

pub type StreamFuture = BoxFuture<'static, Result<SendableTskvRecordBatchStream>>;

enum StreamState {
    Open(StreamFuture),
    Scan(SendableTskvRecordBatchStream),
}

fn series_keys_to_record_batch(
    schema: SchemaRef,
    series_keys: &[SeriesKey],
) -> Result<RecordBatch, ArrowError> {
    let tag_key_array = schema.fields.iter().map(|f| f.name()).collect::<Vec<_>>();
    let mut array_builders = build_arrow_array_builders(&schema, series_keys.len())?;
    for key in series_keys {
        for (k, array_builder) in tag_key_array.iter().zip(&mut array_builders) {
            let tag_value = key
                .tag_string_val(k)
                .map_err(|e| ArrowError::ExternalError(Box::new(e)))?;

            let builder = array_builder
                .as_any_mut()
                .downcast_mut::<StringBuilder>()
                .expect("Cast failed for List Builder<StringBuilder> during nested data parsing");
            builder.append_option(tag_value)
        }
    }
    let columns = array_builders
        .into_iter()
        .map(|mut b| b.finish())
        .collect::<Vec<_>>();
    let record_batch = RecordBatch::try_new(schema.clone(), columns)?;
    Ok(record_batch)
}
