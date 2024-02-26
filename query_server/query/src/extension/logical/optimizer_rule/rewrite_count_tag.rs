use std::sync::Arc;

use datafusion::datasource::source_as_provider;
use datafusion::error::Result;
use datafusion::logical_expr::{
    aggregate_function, Expr, Extension, LogicalPlan, LogicalPlanBuilder, TableScan,
};
use datafusion::optimizer::optimizer::ApplyOrder;
use datafusion::optimizer::{OptimizerConfig, OptimizerRule};
use models::schema::TskvTableSchema;

use crate::data_source::batch::tskv::ClusterTable;
use crate::extension::logical::plan_node::tag_scan::TagScanPlanNode;

pub struct RewriteCountTag {}

impl OptimizerRule for RewriteCountTag {
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
            ..
        }) = plan
        {
            if let Some(cluster_table) = source_as_provider(source)?
                .as_any()
                .downcast_ref::<ClusterTable>()
            {
                if let Some(agg_with_grouping) = agg_with_grouping {
                    let table_schema = cluster_table.table_schema();
                    if let Some(count_col_name) =
                        RewriteCountTag::is_count_tag(&agg_with_grouping.agg_expr, table_schema)
                    {
                        let tag_plan = LogicalPlan::Extension(Extension {
                            node: Arc::new(TagScanPlanNode {
                                table_name: table_name.to_string(),
                                source: Arc::new(cluster_table.clone()),
                                projection: projection.clone(),
                                projected_schema: projected_schema.clone(),
                                filters: filters.clone(),
                                count_col_name: Some(count_col_name),
                                fetch: *fetch,
                            }),
                        });
                        return Ok(Some(LogicalPlanBuilder::from(tag_plan).build()?));
                    }
                }
            }
        }
        Ok(None)
    }

    fn name(&self) -> &str {
        "rewrite_count_tag"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::BottomUp)
    }
}

impl RewriteCountTag {
    fn is_count_tag(agg_expr: &Vec<Expr>, table_schema: Arc<TskvTableSchema>) -> Option<String> {
        if agg_expr.len() == 1 {
            if let Expr::AggregateFunction(agg) = &agg_expr[0] {
                if agg.fun == aggregate_function::AggregateFunction::Count && agg.args.len() == 1 {
                    if let Expr::Column(c) = &agg.args[0] {
                        let opt = table_schema.column(&c.name);
                        if let Some(col) = opt {
                            if col.column_type.is_tag() {
                                return Some(col.name.clone());
                            }
                        }
                    }
                }
            }
        }

        None
    }
}
