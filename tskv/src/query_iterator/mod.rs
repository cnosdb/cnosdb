use datafusion::arrow::datatypes::TimeUnit;
pub use iterator::*;
use models::ValueType;

use crate::memcache::DataType;
use crate::Error;

mod iterator;

#[async_trait::async_trait]
pub trait Cursor: Send + Sync {
    fn name(&self) -> &String;
    fn is_field(&self) -> bool;
    fn val_type(&self) -> ValueType;
    fn unit(&self) -> Option<TimeUnit>;

    async fn next(&mut self, ts: i64);
    async fn peek(&mut self) -> Result<Option<DataType>, Error>;
}
