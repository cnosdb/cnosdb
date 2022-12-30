#![allow(clippy::too_many_arguments)]
use coordinator::{reader::ReaderIterator, service::CoordinatorRef};
use std::task::Poll;

use datafusion::{
    arrow::{datatypes::SchemaRef, error::ArrowError, record_batch::RecordBatch},
    physical_plan::RecordBatchStream,
};
use futures::{executor::block_on, Stream};
use models::codec::Encoding;
use models::schema::TskvTableSchemaRef;
use models::{
    predicate::domain::PredicateRef,
    schema::{ColumnType, TableColumn, TskvTableSchema, TIME_FIELD},
};

use spi::query::DEFAULT_CATALOG;
use tskv::iterator::{QueryOption, TableScanMetrics};

use tskv::Error;

#[allow(dead_code)]
pub struct TableScanStream {
    proj_schema: SchemaRef,
    batch_size: usize,
    coord: CoordinatorRef,

    iterator: ReaderIterator,

    metrics: TableScanMetrics,
}

impl TableScanStream {
    pub fn new(
        table_schema: TskvTableSchemaRef,
        proj_schema: SchemaRef,
        coord: CoordinatorRef,
        filter: PredicateRef,
        batch_size: usize,
        metrics: TableScanMetrics,
    ) -> Result<Self, Error> {
        let mut proj_fileds = Vec::with_capacity(proj_schema.fields().len());
        for item in proj_schema.fields().iter() {
            let field_name = item.name();
            if field_name == TIME_FIELD {
                let encoding = match table_schema.column(TIME_FIELD) {
                    None => Encoding::Default,
                    Some(v) => v.encoding,
                };
                proj_fileds.push(TableColumn::new(
                    0,
                    TIME_FIELD.to_string(),
                    ColumnType::Time,
                    encoding,
                ));
                continue;
            }

            if let Some(v) = table_schema.column(field_name) {
                proj_fileds.push(v.clone());
            } else {
                return Err(Error::NotFoundField {
                    reason: field_name.clone(),
                });
            }
        }

        let proj_table_schema = TskvTableSchema::new(
            table_schema.tenant.clone(),
            table_schema.db.clone(),
            table_schema.name.clone(),
            proj_fileds,
        );

        let option = QueryOption::new(
            batch_size,
            DEFAULT_CATALOG.to_string(),
            filter,
            proj_schema.clone(),
            proj_table_schema,
            metrics.tskv_metrics(),
        );

        let iterator = match block_on(coord.read_record(option)) {
            Ok(it) => it,
            Err(err) => {
                return Err(Error::CommonError {
                    reason: err.to_string(),
                })
            }
        };

        Ok(Self {
            proj_schema,
            batch_size,
            coord,
            iterator,
            metrics,
        })
    }
}

impl Stream for TableScanStream {
    type Item = Result<RecordBatch, ArrowError>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        let timer = this.metrics.elapsed_compute().timer();
        let res = block_on(this.iterator.next());
        let result = match res {
            Some(data) => match data {
                Ok(batch) => Poll::Ready(Some(Ok(batch))),
                Err(err) => Poll::Ready(Some(Err(ArrowError::CastError(err.to_string())))),
            },
            None => {
                this.metrics.done();
                Poll::Ready(None)
            }
        };

        timer.done();
        this.metrics.record_poll(result)
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
