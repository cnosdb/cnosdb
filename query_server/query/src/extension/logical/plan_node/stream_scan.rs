use std::collections::HashSet;
use std::fmt::{self, Debug};
use std::hash::{Hash, Hasher};

use datafusion::common::{DFSchemaRef, OwnedTableReference};
use datafusion::logical_expr::logical_plan::AggWithGrouping;
use datafusion::logical_expr::{LogicalPlan, UserDefinedLogicalNodeCore};
use datafusion::prelude::Expr;
use spi::query::datasource::stream::StreamProviderRef;

#[derive(Clone)]
pub struct StreamScanPlanNode {
    /// The name of the table
    pub table_name: OwnedTableReference,
    /// The source of the table
    pub source: StreamProviderRef,
    /// Optional column indices to use as a projection
    pub projection: Option<Vec<usize>>,
    /// The schema description of the output
    pub projected_schema: DFSchemaRef,
    /// Optional expressions to be used as filters by the table provider
    pub filters: Vec<Expr>,
    pub agg_with_grouping: Option<AggWithGrouping>,
}

impl Debug for StreamScanPlanNode {
    /// For TopK, use explain format for the Debug format. Other types
    /// of nodes may
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.fmt_for_explain(f)
    }
}

impl Hash for StreamScanPlanNode {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.table_name.hash(state);
        self.projection.hash(state);
        self.projected_schema.hash(state);
        self.filters.hash(state);
        self.agg_with_grouping.hash(state);
    }
}

impl PartialEq for StreamScanPlanNode {
    fn eq(&self, other: &Self) -> bool {
        self.table_name == other.table_name
            && self.projection == other.projection
            && self.projected_schema == other.projected_schema
            && self.filters == other.filters
            && self.agg_with_grouping == other.agg_with_grouping
    }
}

impl Eq for StreamScanPlanNode {}

impl UserDefinedLogicalNodeCore for StreamScanPlanNode {
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

    fn from_template(&self, exprs: &[Expr], inputs: &[LogicalPlan]) -> Self {
        assert_eq!(inputs.len(), 0, "input size inconsistent");
        assert_eq!(exprs.len(), 0, "expr size inconsistent");
        self.clone()
    }

    fn prevent_predicate_push_down_columns(&self) -> std::collections::HashSet<String> {
        // default (safe) is all columns in the schema.
        let name = self.source.watermark().column.clone();
        HashSet::from_iter([name])
    }

    fn name(&self) -> &str {
        "StreamScan"
    }
}
