use std::sync::Arc;

use datafusion::datasource::source_as_provider;
use datafusion::error::Result;
use datafusion::logical_expr::{Extension, LogicalPlan, LogicalPlanBuilder, TableScan};
use datafusion::optimizer::optimizer::ApplyOrder;
use datafusion::optimizer::{OptimizerConfig, OptimizerRule};

use crate::data_source::batch::tskv::ClusterTable;
use crate::extension::logical::plan_node::tag_scan::TagScanPlanNode;

/// Convert query statement to query tag operation
///
/// Triggering conditions:
/// 1. The projection contains only the tag column
pub struct RewriteTagScan {}

impl OptimizerRule for RewriteTagScan {
    fn try_optimize(
        &self,
        plan: &LogicalPlan,
        _optimizer_config: &dyn OptimizerConfig,
    ) -> Result<Option<LogicalPlan>> {
        if let LogicalPlan::TableScan(TableScan {
            table_name,
            source,
            projection,
            projected_schema,
            filters,
            agg_with_grouping,
            fetch,
        }) = plan
        {
            if let Some(cluster_table) = source_as_provider(source)?
                .as_any()
                .downcast_ref::<ClusterTable>()
            {
                let table_schema = cluster_table.table_schema();
                // Only handle the table of ClusterTable
                if let Some(e) = projection.as_ref() {
                    let mut contain_time = false;
                    let mut contain_tag = false;
                    let mut contain_field = false;

                    // Find non-tag columns from projection
                    e.iter()
                        .flat_map(|i| table_schema.get_column_by_index(*i))
                        .for_each(|c| {
                            if c.column_type.is_tag() {
                                contain_tag = true;
                            } else if c.column_type.is_field() {
                                contain_field = true;
                            } else if c.column_type.is_time() {
                                contain_time = true;
                            }
                        });

                    if contain_tag && !contain_field && !contain_time && agg_with_grouping.is_none()
                    {
                        // If it does not contain non-tag columns, convert TableScan to TagScan
                        let tag_plan = LogicalPlan::Extension(Extension {
                            node: Arc::new(TagScanPlanNode {
                                table_name: table_name.to_string(),
                                source: Arc::new(cluster_table.clone()),
                                projection: projection.clone(),
                                projected_schema: projected_schema.clone(),
                                filters: filters.clone(),
                                fetch: *fetch,
                            }),
                        });
                        return Ok(Some(
                            LogicalPlanBuilder::from(tag_plan).distinct()?.build()?,
                        ));
                    }
                }
            }
        }

        Ok(None)
    }

    fn name(&self) -> &str {
        "rewrite_tag_scan"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::BottomUp)
    }
}
