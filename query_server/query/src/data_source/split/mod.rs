use std::sync::Arc;

use coordinator::service::CoordinatorRef;
use datafusion::execution::context::SessionState;
use datafusion::sql::TableReference;
use models::object_reference::Resolve;
use models::predicate::PlacedSplit;
use spi::{QueryError, Result};
use trace::debug;

use self::tskv::TableLayoutHandle;

pub mod tskv;

pub type SplitManagerRef = Arc<SplitManager>;

#[cfg(test)]
pub fn default_split_manager_ref_only_for_test() -> SplitManagerRef {
    use coordinator::service_mock::MockCoordinator;
    Arc::new(SplitManager::new(Arc::new(MockCoordinator::default())))
}

#[non_exhaustive]
pub struct SplitManager {
    coord: CoordinatorRef,
}

impl SplitManager {
    pub fn new(coord: CoordinatorRef) -> Self {
        Self { coord }
    }

    pub async fn splits(
        &self,
        _ctx: &SessionState,
        table_layout: TableLayoutHandle,
    ) -> Result<Vec<PlacedSplit>> {
        let TableLayoutHandle {
            table, predicate, ..
        } = table_layout;

        let table_name =
            TableReference::bare(&table.name).resolve_object(&table.tenant, &table.db)?;
        debug!(
            "Get table {}'s splits, predicate: {:?}",
            table.name, predicate
        );

        let limit = predicate.limit();

        let resolved_predicate = predicate
            .resolve(&table)
            .map_err(|reason| QueryError::AnalyzePushedFilter { reason })?;

        let shards = self
            .coord
            .table_vnodes(&table_name, resolved_predicate.clone())
            .await?;

        let splits = shards
            .into_iter()
            .enumerate()
            .map(|(idx, e)| PlacedSplit::new(idx, resolved_predicate.clone(), limit, e))
            .collect::<Vec<_>>();

        debug!(
            "Table {}'s {} splits: {:?}",
            table.name,
            splits.len(),
            splits
        );

        Ok(splits)
    }
}
