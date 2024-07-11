use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow_schema::{Fields, Schema};
use datafusion::arrow::array::StringBuilder;
use datafusion::arrow::datatypes::{DataType, SchemaRef};
use datafusion::arrow::error::ArrowError;
use datafusion::arrow::record_batch::RecordBatch;
use futures::future::BoxFuture;
use futures::{ready, Stream, StreamExt, TryFutureExt};
use models::arrow::stream::MemoryRecordBatchStream;
use models::arrow_array::build_arrow_array_builders;
use models::meta_data::VnodeId;
use models::schema::tskv_table_schema::TskvTableSchemaRef;
use models::SeriesKey;
use snafu::{OptionExt, ResultExt};
use trace::Span;

use crate::error::{ArrowSnafu, ColumnNotFoundSnafu, TskvResult};
use crate::reader::{QueryOption, SendableTskvRecordBatchStream};
use crate::{EngineRef, TskvError};

pub struct LocalTskvTagScanStream {
    state: StreamState,
    #[allow(unused)]
    span: Span,
}

pub fn dictionary_filed_to_string(schema: SchemaRef) -> SchemaRef {
    let fields = schema
        .fields()
        .iter()
        .map(|f| match f.data_type() {
            DataType::Dictionary(_, b) => {
                Arc::new(f.as_ref().clone().with_data_type(b.as_ref().clone()))
            }
            _ => f.clone(),
        })
        .collect::<Fields>();

    Arc::new(Schema::new_with_metadata(fields, schema.metadata.clone()))
}

impl LocalTskvTagScanStream {
    pub fn new(vnode_id: VnodeId, option: QueryOption, kv: EngineRef, span: Span) -> Self {
        let futrue = async move {
            let (tenant, db, table) = (
                option.table_schema.tenant.as_str(),
                option.table_schema.db.as_str(),
                option.table_schema.name.as_str(),
            );

            let series_ids = kv
                .get_series_id_by_filter(tenant, db, table, vnode_id, option.split.tags_filter())
                .await?;
            let keys = kv
                .get_series_key(tenant, db, table, vnode_id, &series_ids)
                .await?;

            let mut batches = vec![];
            for chunk in keys.chunks(option.batch_size) {
                let record_batch = series_keys_to_record_batch(
                    option.table_schema.clone(),
                    option.df_schema.clone(),
                    chunk,
                )?;
                batches.push(record_batch)
            }

            Ok(Box::pin(MemoryRecordBatchStream::new(batches)) as SendableTskvRecordBatchStream)
        };

        let state = StreamState::Open(Box::pin(futrue));

        Self { state, span }
    }

    fn poll_inner(&mut self, cx: &mut Context<'_>) -> Poll<Option<TskvResult<RecordBatch>>> {
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
    type Item = TskvResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.poll_inner(cx)
    }
}

pub type StreamFuture = BoxFuture<'static, TskvResult<SendableTskvRecordBatchStream>>;

enum StreamState {
    Open(StreamFuture),
    Scan(SendableTskvRecordBatchStream),
}

fn series_keys_to_record_batch(
    tskv_table_schema: TskvTableSchemaRef,
    schema: SchemaRef,
    series_keys: &[SeriesKey],
) -> TskvResult<RecordBatch, TskvError> {
    let tag_key_array = schema.fields.iter().map(|f| f.name()).collect::<Vec<_>>();
    let new_schema = dictionary_filed_to_string(schema.clone());
    let mut array_builders =
        build_arrow_array_builders(&new_schema, series_keys.len()).context(ArrowSnafu)?;
    for key in series_keys {
        for (k, array_builder) in tag_key_array.iter().zip(&mut array_builders) {
            let c = tskv_table_schema
                .column(k.as_str())
                .context(ColumnNotFoundSnafu {
                    column: k.to_string(),
                })?;

            let tag_value = key
                .tag_string_val(c.id.to_string().as_str())
                .map_err(|e| ArrowError::ExternalError(Box::new(e)))
                .context(ArrowSnafu)?;

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
        .zip(schema.fields().iter().map(|f| f.data_type()))
        .map(|(a, d)| arrow::compute::cast(&a, d))
        .collect::<TskvResult<Vec<_>, ArrowError>>()
        .context(ArrowSnafu)?;

    let record_batch = RecordBatch::try_new(schema.clone(), columns).context(ArrowSnafu)?;
    Ok(record_batch)
}
