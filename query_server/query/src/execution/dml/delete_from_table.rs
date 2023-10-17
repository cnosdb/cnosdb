use std::sync::Arc;

use async_trait::async_trait;
use models::predicate::domain::{ColumnDomains, ResolvedPredicate, TimeRanges};
use models::predicate::transformation::DeleteSelectionExpressionToDomainsVisitor;
use models::predicate::utils::filter_to_time_ranges;
use spi::query::execution::{Output, QueryStateMachineRef};
use spi::query::logical_planner::DeleteFromTable;
use spi::Result;

use super::DMLDefinitionTask;

pub struct DeleteFromTableTask {
    stmt: DeleteFromTable,
}

impl DeleteFromTableTask {
    pub fn new(stmt: DeleteFromTable) -> Self {
        Self { stmt }
    }
}

#[async_trait]
impl DMLDefinitionTask for DeleteFromTableTask {
    async fn execute(&self, query_state_machine: QueryStateMachineRef) -> Result<Output> {
        let DeleteFromTable {
            table_name,
            selection,
        } = &self.stmt;

        let (tags_filter, time_ranges) = if let Some(expr) = selection {
            let (tag, time) =
                DeleteSelectionExpressionToDomainsVisitor::expr_to_tag_and_time_domains(expr)?;
            let time_range = filter_to_time_ranges(&time);
            (tag, TimeRanges::new(time_range))
        } else {
            (ColumnDomains::all(), TimeRanges::all())
        };

        let predicate =
            ResolvedPredicate::new(Arc::new(time_ranges), tags_filter, ColumnDomains::all());

        trace::info!("Delete from table: {table_name}, filter: {predicate:?}");

        query_state_machine
            .coord
            .delete_from_table(table_name, &predicate)
            .await?;

        Ok(Output::Nil(()))
    }
}
