use std::sync::Arc;

use datafusion::error::Result;
use datafusion::logical_expr::{Extension, Limit, LogicalPlan, Sort};
use datafusion::optimizer::{OptimizerConfig, OptimizerRule};

use super::super::plan_node::topk::TopKPlanNode;

pub struct MergeLimitWithSortRule {}

impl OptimizerRule for MergeLimitWithSortRule {
    // Example rewrite pass to insert a user defined LogicalPlanNode
    fn try_optimize(
        &self,
        plan: &LogicalPlan,
        optimizer_config: &dyn OptimizerConfig,
    ) -> Result<Option<LogicalPlan>> {
        // Note: this code simply looks for the pattern of a Limit followed by a
        // Sort and replaces it by a TopK node. It does not handle many
        // edge cases (e.g multiple sort columns, sort ASC / DESC), etc.
        if let LogicalPlan::Limit(Limit {
            skip,
            fetch: Some(fetch),
            input,
        }) = plan
        {
            if let LogicalPlan::Sort(Sort {
                ref expr,
                ref input,
                ..
            }) = **input
            {
                // If k is too large, no topk optimization is performed
                if skip + fetch <= 255 {
                    // we found a sort with a single sort expr, replace with a a TopK
                    return Ok(Some(LogicalPlan::Extension(Extension {
                        node: Arc::new(TopKPlanNode::new(
                            expr.clone(),
                            self.try_optimize(input.as_ref(), optimizer_config)?
                                .map(Arc::new)
                                .unwrap_or_else(|| input.clone()),
                            Some(*skip),
                            *fetch,
                        )),
                    })));
                }
            }
        }

        // If we didn't find the Limit/Sort combination, recurse as
        // normal and build the result.
        datafusion::optimizer::utils::optimize_children(self, plan, optimizer_config)
    }

    fn name(&self) -> &str {
        "merge_limit_with_sort"
    }
}
