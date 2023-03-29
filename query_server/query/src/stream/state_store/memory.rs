use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::Result;
use datafusion::physical_plan::expressions::NotExpr;
use datafusion::physical_plan::PhysicalExpr;
use parking_lot::RwLock;

use super::{StateStore, StateStoreFactory};
use crate::extension::utils::batch_filter;

#[derive(Debug, Default)]
pub struct MemoryStateStoreFactory {
    state_store_map: RwLock<HashMap<(String, usize, usize), Arc<MemoryStateStore>>>,
}

impl StateStoreFactory for MemoryStateStoreFactory {
    type SS = MemoryStateStore;

    fn get_or_default(
        &self,
        query_id: String,
        partition_id: usize,
        operator_id: usize,
    ) -> Result<Arc<Self::SS>> {
        let key = (query_id, partition_id, operator_id);
        if let Some(state_store) = self.state_store_map.read().get(&key) {
            return Ok(state_store.clone());
        }

        let state_store = Arc::new(MemoryStateStore::default());
        self.state_store_map
            .write()
            .insert(key, state_store.clone());

        Ok(state_store)
    }
}

#[derive(Debug, Default)]
struct Container {
    committed: RwLock<Arc<RwLock<Vec<RecordBatch>>>>,
    uncommitted: RwLock<Arc<RwLock<Vec<RecordBatch>>>>,
}

#[derive(Debug, Default)]
pub struct MemoryStateStore {
    states: Container,
}

impl StateStore for MemoryStateStore {
    fn put(&self, batch: RecordBatch) -> Result<()> {
        trace::trace!("Write batch to MemoryStateStore: {:?}", batch);
        self.states.uncommitted.read().write().push(batch);

        Ok(())
    }

    /// TODO 待优化
    fn expire(&self, predicate: Arc<dyn PhysicalExpr>) -> Result<Vec<RecordBatch>> {
        trace::debug!("Remove batches match {} from MemoryStateStore", predicate);
        // 保留未过期的数据
        let remained: Arc<dyn PhysicalExpr> = Arc::new(NotExpr::new(predicate.clone()));
        let remained_data = self
            .states
            .uncommitted
            .read()
            .read()
            .iter()
            .map(|e| batch_filter(e, &remained))
            .collect::<Result<Vec<_>>>()?;
        *self.states.uncommitted.write() = Arc::new(RwLock::new(remained_data));

        // 返回过期的数据
        let expired_data = self
            .states
            .uncommitted
            .read()
            .read()
            .iter()
            .map(|e| batch_filter(e, &predicate))
            .collect::<Result<Vec<_>>>()?;

        Ok(expired_data)
    }

    /// TODO 待优化
    fn commit(&self) -> Result<i64> {
        trace::trace!("MemoryStateStore commit");
        // TODO 切换committed和uncommitted
        // 清空committed数据，降为uncommitted
        // uncommitted升为committed
        *self.states.committed.write() = self.states.uncommitted.read().clone();
        *self.states.uncommitted.write() = Default::default();

        Ok(0)
    }

    fn state(&self) -> Result<Vec<RecordBatch>> {
        trace::trace!("Read all states from MemoryStateStore");

        Ok(self.states.committed.read().read().clone())
    }
}
