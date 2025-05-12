use std::collections::HashSet;
use std::fmt::{self, Debug};
use std::hash::{Hash, Hasher};

use datafusion::common::{DFSchemaRef, OwnedTableReference};
use datafusion::logical_expr::logical_plan::TableScanAggregate;
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
    pub aggregate: Option<TableScanAggregate>,
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
        self.aggregate.hash(state);
    }
}

impl PartialEq for StreamScanPlanNode {
    fn eq(&self, other: &Self) -> bool {
        self.table_name == other.table_name
            && self.projection == other.projection
            && self.projected_schema == other.projected_schema
            && self.filters == other.filters
            && self.aggregate == other.aggregate
    }
}

impl Eq for StreamScanPlanNode {}

impl UserDefinedLogicalNodeCore for StreamScanPlanNode {
    fn name(&self) -> &str {
        "StreamScan"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![]
    }

    /// Schema for TopK is the same as the input
    fn schema(&self) -> &DFSchemaRef {
        &self.projected_schema
    }

    fn expressions(&self) -> Vec<Expr> {
        if self.aggregate.is_none() {
            return self.filters.to_vec();
        }

        vec![]
    }

    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "StreamScan: [{}]",
            self.projected_schema.field_names().join(",")
        )
    }

    fn with_exprs_and_inputs(&self, exprs: Vec<Expr>, inputs: Vec<LogicalPlan>) -> Self {
        assert_eq!(inputs.len(), 0, "input size inconsistent");

        if self.aggregate.is_some() {
            self.clone()
        } else {
            Self {
                table_name: self.table_name.clone(),
                source: self.source.clone(),
                projection: self.projection.clone(),
                projected_schema: self.projected_schema.clone(),
                filters: exprs,
                aggregate: self.aggregate.clone(),
            }
        }
    }

    fn prevent_predicate_push_down_columns(&self) -> std::collections::HashSet<String> {
        // default (safe) is all columns in the schema.
        self.source
            .schema()
            .fields()
            .iter()
            .map(|f| f.name())
            .cloned()
            .collect::<HashSet<_>>()
    }
}
