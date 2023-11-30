use std::fmt;
use std::ops::Deref;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{ready, Context, Poll};

use arrow::datatypes::{Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::physical_plan::PhysicalExpr;
use futures::stream::BoxStream;
use futures::{Stream, StreamExt};
pub use iterator::*;
use models::field_value::DataType;
use models::predicate::domain::TimeRange;
use models::schema::PhysicalCType;

use self::utils::{CombinedRecordBatchStream, TimeRangeProvider};
use crate::memcache::RowGroup;
use crate::tsm2::page::Chunk;
use crate::tsm2::reader::TSM2Reader;
use crate::{Error, Result};

mod batch_builder;
mod batch_cut;
pub mod chunk;
mod column_group;
pub mod cut_merge;
pub mod display;
pub mod filter;
mod iterator;
pub mod iterator_v2;
pub mod merge;
pub mod page;
mod paralle_merge;
mod partitioned_stream;
pub mod query_executor;
pub mod schema_alignmenter;
pub mod serialize;
pub mod series;
pub mod sort_merge;
pub mod table_scan;
pub mod tag_scan;
pub mod test_util;
pub mod utils;
pub mod visitor;

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
    Pin<Box<dyn SchemableTskvRecordBatchStream<Item = Result<RecordBatch>> + Send>>;
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
    fn fmt_as(&self, f: &mut fmt::Formatter) -> fmt::Result;
    fn children(&self) -> Vec<BatchReaderRef>;
}

pub type CombinedBatchReader = Vec<BatchReaderRef>;
impl BatchReader for CombinedBatchReader {
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

    fn fmt_as(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "CombinedBatchReader: size={}", self.len())
    }

    fn children(&self) -> Vec<BatchReaderRef> {
        self.clone()
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
        let stream = SchemableMemoryBatchReaderStream::new(self.schema.clone(), self.data.clone());

        Ok(Box::pin(stream))
    }

    fn fmt_as(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "MemoryBatchReader:")
    }

    fn children(&self) -> Vec<BatchReaderRef> {
        vec![]
    }
}

pub struct SchemableMemoryBatchReaderStream {
    schema: SchemaRef,
    stream: BoxStream<'static, RecordBatch>,
}

impl SchemableMemoryBatchReaderStream {
    pub fn new(schema: SchemaRef, batches: Vec<RecordBatch>) -> Self {
        Self {
            schema,
            stream: Box::pin(futures::stream::iter(batches.into_iter())),
        }
    }

    pub fn new_partitions(
        schema: SchemaRef,
        batches: Vec<Vec<RecordBatch>>,
    ) -> Vec<SendableSchemableTskvRecordBatchStream> {
        batches
            .into_iter()
            .map(|b| {
                Box::pin(SchemableMemoryBatchReaderStream::new(schema.clone(), b))
                    as SendableSchemableTskvRecordBatchStream
            })
            .collect::<Vec<_>>()
    }
}

impl SchemableTskvRecordBatchStream for SchemableMemoryBatchReaderStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl Stream for SchemableMemoryBatchReaderStream {
    type Item = Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match ready!(self.stream.poll_next_unpin(cx)) {
            None => Poll::Ready(None),
            Some(r) => Poll::Ready(Some(Ok(r))),
        }
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

/// A wrapper to customize partitioned file display
#[derive(Debug)]
pub struct ProjectSchemaDisplay<'a>(pub &'a SchemaRef);

impl<'a> fmt::Display for ProjectSchemaDisplay<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let parts: Vec<_> = self
            .0
            .fields()
            .iter()
            .map(|x| x.name().to_owned())
            .collect::<Vec<String>>();
        write!(f, "[{}]", parts.join(", "))
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
