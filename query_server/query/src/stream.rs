use std::sync::Arc;

use datafusion::{
    arrow::{datatypes::SchemaRef, error::ArrowError, record_batch::RecordBatch},
    physical_plan::RecordBatchStream,
};
use futures::Stream;

use trace::{debug, error};
use tskv::engine::EngineRef;

use tskv::{Error, TimeRange};

use crate::iterator::{QueryOption, RowIterator};
use crate::predicate::PredicateRef;
use crate::schema::TableSchema;

pub const TIME_FIELD: &str = "time";

pub enum ArrayType {
    U64(Vec<u64>),
    I64(Vec<i64>),
    Str(Vec<String>),
    F64(Vec<f64>),
    Bool(Vec<bool>),
}

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
        let option = QueryOption {
            db_name: table_schema.db.clone(),
            table_name: table_schema.name.clone(),
            time_range: TimeRange {
                min_ts: i64::MIN,
                max_ts: i64::MAX,
            },
            table_schema,
            datafusion_schema: proj_schema.clone(),
        };

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
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let this = self.get_mut();

        match this.iterator.next() {
            Some(data) => match data {
                Ok(batch) => return std::task::Poll::Ready(Some(Ok(batch))),
                Err(err) => {
                    return std::task::Poll::Ready(Some(Err(ArrowError::CastError(
                        err.to_string(),
                    ))))
                }
            },
            None => return std::task::Poll::Ready(None),
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
