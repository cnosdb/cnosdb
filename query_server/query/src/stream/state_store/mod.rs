use std::sync::Arc;

use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::Result;
use datafusion::physical_plan::PhysicalExpr;

use self::memory::MemoryStateStoreFactory;
pub mod memory;

pub fn create_memory_state_store_factory() -> Arc<MemoryStateStoreFactory> {
    Arc::new(MemoryStateStoreFactory::default())
}

pub trait StateStoreFactory {
    type SS: StateStore;

    /// TODO 通过query_id, partition_id, operator_id来唯一标识一个state
    fn get_or_default(
        &self,
        query_id: String,
        partition_id: usize,
        operator_id: usize,
    ) -> Result<Arc<Self::SS>>;
}

pub type StateStoreRef = Arc<dyn StateStore>;

pub trait StateStore {
    /// Put a new non-null value for a non-null key. Implementations must be aware that the UnsafeRows \
    /// in the params can be reused, and must make copies of the data as needed for persistence.
    fn put(&self, batch: RecordBatch) -> Result<()>;

    /// 移除匹配predicate的数据
    fn expire(&self, predicate: Arc<dyn PhysicalExpr>) -> Result<Vec<RecordBatch>>;

    /// Commit all the updates that have been made to the store, and return the new version.
    /// Implementations should ensure that no more updates (puts, removes) can be after a commit in
    /// order to avoid incorrect usage.
    fn commit(&self) -> Result<i64>;

    /// Return an iterator containing all the key-value pairs in the StateStore. Implementations must
    /// ensure that updates (puts, removes) can be made while iterating over this iterator.
    fn state(&self) -> Result<Vec<RecordBatch>>;
}
