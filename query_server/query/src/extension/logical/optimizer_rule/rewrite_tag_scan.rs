use std::sync::Arc;

use datafusion::{
    logical_plan::{
        plan::Extension, source_as_provider, LogicalPlan, LogicalPlanBuilder, TableScan,
    },
    optimizer::{OptimizerConfig, OptimizerRule},
};

use crate::{extension::logical::plan_node::tag_scan::TagScanPlanNode, table::ClusterTable};

use datafusion::error::Result;

/// Convert query statement to query tag operation
///
/// Triggering conditions:
/// 1. The projection contains only the tag column
pub struct RewriteTagScan {}

impl OptimizerRule for RewriteTagScan {
    fn optimize(
        &self,
        plan: &LogicalPlan,
        optimizer_config: &mut OptimizerConfig,
    ) -> Result<LogicalPlan> {
        if let LogicalPlan::TableScan(TableScan {
            table_name,
            source,
            projection,
            projected_schema,
            filters,
            fetch,
        }) = plan
        {
            if let Some(cluster_table) = source_as_provider(source)?
                .as_any()
                .downcast_ref::<ClusterTable>()
            {
                // Only handle the table of ClusterTable
                if let Some(e) = projection.as_ref() {
                    // Find non-tag columns from projection
                    let non_tag_column = e.iter().find(|idx| {
                        if let Some(c) = cluster_table.table_schema().column_by_index(**idx) {
                            !c.column_type.is_tag()
                        } else {
                            false
                        }
                    });

                    if non_tag_column.is_none() {
                        // If it does not contain non-tag columns, convert TableScan to TagScan
                        let tag_plan = LogicalPlan::Extension(Extension {
                            node: Arc::new(TagScanPlanNode {
                                table_name: table_name.clone(),
                                source: Arc::new(cluster_table.clone()),
                                projection: projection.clone(),
                                projected_schema: projected_schema.clone(),
                                filters: filters.clone(),
                                fetch: *fetch,
                            }),
                        });
                        // The result of tag scan needs to be deduplicated
                        return LogicalPlanBuilder::from(tag_plan).distinct()?.build();
                    }
                }
            }
        }

        // If we didn't find the Limit/Sort combination, recurse as
        // normal and build the result.
        datafusion::optimizer::utils::optimize_children(self, plan, optimizer_config)
    }

    fn name(&self) -> &str {
        "rewrite_tag_scan"
    }
}
