use std::sync::Arc;

use async_trait::async_trait;
use meta::error::{DatabaseNotFoundSnafu, TenantNotFoundSnafu};
use models::predicate::domain::{ColumnDomains, ResolvedPredicate, TimeRanges};
use models::predicate::transformation::DeleteSelectionExpressionToDomainsVisitor;
use models::predicate::utils::filter_to_time_ranges;
use snafu::ResultExt;
use spi::query::execution::{Output, QueryStateMachineRef};
use spi::query::logical_planner::DeleteFromTable;
use spi::{CoordinatorSnafu, InvalidParamSnafu, MetaSnafu, ModelsSnafu, QueryResult};
use utils::precision::{timestamp_convert, Precision};

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
    async fn execute(&self, query_state_machine: QueryStateMachineRef) -> QueryResult<Output> {
        let DeleteFromTable {
            table_name,
            selection,
        } = &self.stmt;

        let db_schema = query_state_machine
            .meta
            .tenant_meta(table_name.tenant())
            .await
            .ok_or_else(|| {
                TenantNotFoundSnafu {
                    tenant: table_name.tenant().to_string(),
                }
                .build()
            })
            .context(MetaSnafu)?
            .get_db_schema(table_name.database())
            .context(MetaSnafu)?
            .ok_or_else(|| {
                DatabaseNotFoundSnafu {
                    database: table_name.to_string(),
                }
                .build()
            })
            .context(MetaSnafu)?;
        let precision = *db_schema.config().precision();

        let (tags_filter, time_ranges) = if let Some(expr) = selection {
            let (tag, time) =
                DeleteSelectionExpressionToDomainsVisitor::expr_to_tag_and_time_domains(expr)?;
            let mut time_ranges = filter_to_time_ranges(&time);
            for range in time_ranges.iter_mut() {
                let min =
                    timestamp_convert(Precision::NS, precision, range.min_ts).ok_or_else(|| {
                        InvalidParamSnafu {
                            reason: "Invalid timestamp".to_string(),
                        }
                        .build()
                    })?;
                let max =
                    timestamp_convert(Precision::NS, precision, range.max_ts).ok_or_else(|| {
                        InvalidParamSnafu {
                            reason: "Invalid timestamp".to_string(),
                        }
                        .build()
                    })?;
                range.max_ts = max;
                range.min_ts = min;
            }
            (tag, TimeRanges::new(time_ranges))
        } else {
            (ColumnDomains::all(), TimeRanges::all())
        };

        let predicate = ResolvedPredicate::new(Arc::new(time_ranges), tags_filter, None)
            .context(ModelsSnafu)?;

        trace::info!("Delete from table: {table_name}, filter: {predicate:?}");

        query_state_machine
            .coord
            .delete_from_table(table_name, &predicate)
            .await
            .context(CoordinatorSnafu)?;

        Ok(Output::Nil(()))
    }
}
