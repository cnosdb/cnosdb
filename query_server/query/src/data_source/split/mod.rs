use std::sync::Arc;

use coordinator::service::CoordinatorRef;
use datafusion::catalog::Session;
use datafusion::sql::TableReference;
use models::object_reference::Resolve;
use models::predicate::PlacedSplit;
use snafu::ResultExt;
use spi::{AnalyzePushedFilterSnafu, CoordinatorSnafu, QueryResult};
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
        _state: &dyn Session,
        table_layout: TableLayoutHandle,
    ) -> QueryResult<Vec<PlacedSplit>> {
        let TableLayoutHandle {
            table, predicate, ..
        } = table_layout;

        let table_name = TableReference::bare(table.name.clone())
            .resolve_object(table.tenant.clone(), table.db.clone())?;
        debug!(
            "Get table {}'s splits, predicate: {:?}",
            table.name, predicate
        );

        let limit = predicate.limit();

        let resolved_predicate = predicate
            .resolve(&table)
            .context(AnalyzePushedFilterSnafu)?;

        let shards = self
            .coord
            .table_vnodes(&table_name, resolved_predicate.clone())
            .await
            .context(CoordinatorSnafu)?;

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
