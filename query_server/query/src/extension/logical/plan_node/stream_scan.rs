use std::any::Any;
use std::collections::HashSet;
use std::fmt::{self, Debug};
use std::sync::Arc;

use datafusion::common::DFSchemaRef;
use datafusion::logical_expr::logical_plan::AggWithGrouping;
use datafusion::logical_expr::{LogicalPlan, UserDefinedLogicalNode};
use datafusion::prelude::Expr;
use spi::query::datasource::stream::StreamProviderRef;

#[derive(Clone)]
pub struct StreamScanPlanNode {
    /// The name of the table
    pub table_name: String,
    /// The source of the table
    pub source: StreamProviderRef,
    /// Optional column indices to use as a projection
    pub projection: Option<Vec<usize>>,
    /// The schema description of the output
    pub projected_schema: DFSchemaRef,
    /// Optional expressions to be used as filters by the table provider
    pub filters: Vec<Expr>,
    pub agg_with_grouping: Option<AggWithGrouping>,
    /// Optional number of rows to read
    pub fetch: Option<usize>,
}

impl Debug for StreamScanPlanNode {
    /// For TopK, use explain format for the Debug format. Other types
    /// of nodes may
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.fmt_for_explain(f)
    }
}

impl UserDefinedLogicalNode for StreamScanPlanNode {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![]
    }

    /// Schema for TopK is the same as the input
    fn schema(&self) -> &DFSchemaRef {
        &self.projected_schema
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "StreamScan: [{}]",
            self.projected_schema.field_names().join(",")
        )
    }

    fn from_template(
        &self,
        exprs: &[Expr],
        inputs: &[LogicalPlan],
    ) -> Arc<dyn UserDefinedLogicalNode> {
        assert_eq!(inputs.len(), 0, "input size inconsistent");
        assert_eq!(exprs.len(), 0, "expr size inconsistent");
        Arc::new(self.clone())
    }

    fn prevent_predicate_push_down_columns(&self) -> std::collections::HashSet<String> {
        // default (safe) is all columns in the schema.
        let name = self.source.watermark().column.clone();
        HashSet::from_iter([name])
    }
}
