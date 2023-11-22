use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow::datatypes::{Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use futures::Stream;
pub use iterator::*;
use models::field_value::DataType;
use models::schema::PhysicalCType;

use self::utils::CombinedRecordBatchStream;
use crate::{Error, Result};

pub mod chunk;
pub mod filter;
mod iterator;
pub mod iterator_v2;
pub mod merge;
pub mod page;
pub mod query_executor;
pub mod schema_alignmenter;
pub mod serialize;
pub mod series;
pub mod table_scan;
pub mod tag_scan;
pub mod utils;

#[derive(Debug, Clone)]
pub struct Projection<'a>(pub Vec<&'a str>);

impl<'a> From<&'a Schema> for Projection<'a> {
    fn from(schema: &'a Schema) -> Self {
        Self(schema.fields().iter().map(|f| f.name().as_str()).collect())
    }
}

pub type SendableTskvRecordBatchStream = Pin<Box<dyn Stream<Item = Result<RecordBatch>> + Send>>;

pub type SendableSchemableTskvRecordBatchStream =
    Pin<Box<dyn SchemableTskvRecordBatchStream + Send>>;
pub trait SchemableTskvRecordBatchStream: Stream<Item = Result<RecordBatch>> {
    fn schema(&self) -> SchemaRef;
}

pub struct EmptySchemableTskvRecordBatchStream {
    schema: SchemaRef,
}
impl EmptySchemableTskvRecordBatchStream {
    pub fn new(schema: SchemaRef) -> Self {
        Self { schema }
    }
}
impl SchemableTskvRecordBatchStream for EmptySchemableTskvRecordBatchStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
impl Stream for EmptySchemableTskvRecordBatchStream {
    type Item = Result<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Poll::Ready(None)
    }
}

pub type BatchReaderRef = Arc<dyn BatchReader>;
pub trait BatchReader {
    fn process(&self) -> Result<SendableSchemableTskvRecordBatchStream>;
}

impl BatchReader for Vec<BatchReaderRef> {
    fn process(&self) -> Result<SendableSchemableTskvRecordBatchStream> {
        let streams = self
            .iter()
            .map(|e| e.process())
            .collect::<Result<Vec<_>>>()?;

        if let Some(s) = streams.first() {
            return Ok(Box::pin(CombinedRecordBatchStream::try_new(
                s.schema(),
                streams,
            )?));
        }

        Err(Error::CommonError {
            reason: "No stream found in CombinedRecordBatchStream".to_string(),
        })
    }
}

#[async_trait::async_trait]
pub trait Cursor: Send + Sync {
    fn name(&self) -> &String;
    fn is_field(&self) -> bool {
        matches!(self.column_type(), PhysicalCType::Field(_))
    }
    fn column_type(&self) -> PhysicalCType;
    async fn next(&mut self) -> Result<Option<DataType>, Error>;
}
