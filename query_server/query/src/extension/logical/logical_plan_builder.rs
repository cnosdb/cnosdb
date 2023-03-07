use std::sync::Arc;

use datafusion::error::Result;
use datafusion::logical_expr::{Extension, LogicalPlan, LogicalPlanBuilder};
use datafusion::prelude::Expr;

use super::plan_node::expand::ExpandNode;

/// Used to extend the function of datafusion's [`LogicalPlanBuilder`]
pub trait LogicalPlanBuilderExt: Sized {
    /// Apply a expand with specific projections
    fn expand(self, projections: Vec<Vec<Expr>>) -> Result<Self>;
}

impl LogicalPlanBuilderExt for LogicalPlanBuilder {
    fn expand(self, projections: Vec<Vec<Expr>>) -> Result<Self> {
        let input = Arc::new(self.build()?);

        let expand = Arc::new(ExpandNode::try_new(projections, input)?);

        let plan = LogicalPlan::Extension(Extension { node: expand });

        Ok(Self::from(plan))
    }
}
