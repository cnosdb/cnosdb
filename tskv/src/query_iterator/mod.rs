pub use iterator::*;
use models::schema::ColumnType;

use crate::memcache::DataType;
use crate::Error;

mod iterator;

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
