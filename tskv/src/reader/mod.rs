use std::ops::Deref;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow::datatypes::{Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::physical_plan::PhysicalExpr;
use futures::{Stream, StreamExt};
pub use iterator::*;
use models::arrow::stream::{BoxStream, MemoryRecordBatchStream};
use models::field_value::DataType;
use models::predicate::domain::TimeRange;
use models::schema::PhysicalCType;

use self::utils::{CombinedRecordBatchStream, TimeRangeProvider};
use crate::memcache::RowGroup;
use crate::tsm2::page::Chunk;
use crate::tsm2::reader::TSM2Reader;
use crate::{Error, Result};

pub mod chunk;
mod column_group;
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

#[derive(Debug)]
pub struct Predicate {
    expr: Arc<dyn PhysicalExpr>,
    schema: SchemaRef,
}

impl Predicate {
    pub fn new(expr: Arc<dyn PhysicalExpr>, schema: SchemaRef) -> Self {
        Self { expr, schema }
    }

    pub fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    pub fn expr(&self) -> Arc<dyn PhysicalExpr> {
        self.expr.clone()
    }
}

#[derive(Debug, Clone)]
pub struct Projection<'a>(pub Vec<&'a str>);

impl<'a> From<&'a Schema> for Projection<'a> {
    fn from(schema: &'a Schema) -> Self {
        Self(schema.fields().iter().map(|f| f.name().as_str()).collect())
    }
}
impl<'a> Deref for Projection<'a> {
    type Target = [&'a str];

    fn deref(&self) -> &Self::Target {
        &self.0
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

pub struct MemoryBatchReader {
    schema: SchemaRef,
    data: Vec<RecordBatch>,
}

impl MemoryBatchReader {
    pub fn new(schema: SchemaRef, data: Vec<RecordBatch>) -> Self {
        Self { schema, data }
    }
}

impl BatchReader for MemoryBatchReader {
    fn process(&self) -> Result<SendableSchemableTskvRecordBatchStream> {
        struct SchemableMemoryBatchReaderStream {
            schema: SchemaRef,
            stream: BoxStream<Result<RecordBatch>>,
        }
        impl SchemableTskvRecordBatchStream for SchemableMemoryBatchReaderStream {
            fn schema(&self) -> SchemaRef {
                self.schema.clone()
            }
        }
        impl Stream for SchemableMemoryBatchReaderStream {
            type Item = Result<RecordBatch>;

            fn poll_next(
                mut self: Pin<&mut Self>,
                cx: &mut Context<'_>,
            ) -> Poll<Option<Self::Item>> {
                self.stream.poll_next_unpin(cx)
            }
        }

        let stream = Box::pin(MemoryRecordBatchStream::<Error>::new(self.data.clone()));

        Ok(Box::pin(SchemableMemoryBatchReaderStream {
            schema: self.schema.clone(),
            stream,
        }))
    }
}

#[derive(Clone)]
pub enum DataReference {
    Chunk(Arc<Chunk>, Arc<TSM2Reader>),
    Memcache(Arc<RowGroup>),
}

impl DataReference {
    pub fn time_range(&self) -> &TimeRange {
        match self {
            DataReference::Chunk(chunk, _) => chunk.time_range(),
            DataReference::Memcache(row_group) => &row_group.range,
        }
    }
}

impl TimeRangeProvider for DataReference {
    fn time_range(&self) -> &TimeRange {
        self.time_range()
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
