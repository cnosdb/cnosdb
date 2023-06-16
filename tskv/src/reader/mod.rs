use std::pin::Pin;

use datafusion::arrow::record_batch::RecordBatch;
use futures::Stream;
pub use iterator::*;
use models::schema::ColumnType;

use crate::memcache::DataType;
use crate::{Error, Result};

mod iterator;
pub mod query_executor;
pub mod serialize;
pub mod status_listener;
pub mod table_scan;
pub mod tag_scan;

pub type SendableTskvRecordBatchStream = Pin<Box<dyn Stream<Item = Result<RecordBatch>> + Send>>;

#[async_trait::async_trait]
pub trait Cursor: Send + Sync {
    fn name(&self) -> &String;
    fn is_field(&self) -> bool {
        matches!(self.column_type(), ColumnType::Field(_))
    }
    fn column_type(&self) -> ColumnType;
    async fn next(&mut self, ts: i64);
    async fn peek(&mut self) -> Result<Option<DataType>, Error>;
}
