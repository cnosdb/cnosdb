use std::collections::BTreeMap;

use datafusion::{
    arrow::{datatypes::SchemaRef, error::ArrowError, record_batch::RecordBatch},
    physical_plan::RecordBatchStream,
};
use futures::Stream;

use tskv::engine::EngineRef;

use tskv::{Error, TimeRange};

use crate::schema::TableSchema;
use crate::{
    iterator::{QueryOption, RowIterator},
    schema::{ColumnType, TableFiled},
};
use crate::{predicate::PredicateRef, schema::TIME_FIELD};

#[allow(dead_code)]
pub struct TableScanStream {
    proj_schema: SchemaRef,
    filter: PredicateRef,
    batch_size: usize,
    store_engine: EngineRef,

    iterator: RowIterator,
}

impl TableScanStream {
    pub fn new(
        table_schema: TableSchema,
        proj_schema: SchemaRef,
        filter: PredicateRef,
        batch_size: usize,
        store_engine: EngineRef,
    ) -> Result<Self, Error> {
        let mut proj_fileds = BTreeMap::new();
        for item in proj_schema.fields().iter() {
            let field_name = item.name();
            if field_name == TIME_FIELD {
                proj_fileds.insert(
                    TIME_FIELD.to_string(),
                    TableFiled::new(0, TIME_FIELD.to_string(), ColumnType::Time),
                );
                continue;
            }

            if let Some(v) = table_schema.fields.get(field_name) {
                proj_fileds.insert(field_name.clone(), v.clone());
            } else {
                return Err(Error::NotFoundField {
                    reason: field_name.clone(),
                });
            }
        }

        let proj_table_schema =
            TableSchema::new(table_schema.db.clone(), table_schema.name, proj_fileds);

        let (min_ts, max_ts) = filter.get_time_range();
        let option = QueryOption {
            time_range: TimeRange { min_ts, max_ts },
            table_schema: proj_table_schema,
            datafusion_schema: proj_schema.clone(),
        };
        println!("========={} {}", min_ts, max_ts);

        let iterator = match RowIterator::new(store_engine.clone(), option, batch_size) {
            Ok(it) => it,
            Err(err) => return Err(err),
        };

        Ok(Self {
            proj_schema,
            filter,
            batch_size,
            store_engine,
            iterator,
        })
    }
}

impl Stream for TableScanStream {
    type Item = Result<RecordBatch, ArrowError>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let this = self.get_mut();

        match this.iterator.next() {
            Some(data) => match data {
                Ok(batch) => std::task::Poll::Ready(Some(Ok(batch))),
                Err(err) => {
                    std::task::Poll::Ready(Some(Err(ArrowError::CastError(err.to_string()))))
                }
            },
            None => std::task::Poll::Ready(None),
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        // todo   (self.data.len(), Some(self.data.len()))
        (0, Some(0))
    }
}

impl RecordBatchStream for TableScanStream {
    fn schema(&self) -> SchemaRef {
        self.proj_schema.clone()
    }
}
