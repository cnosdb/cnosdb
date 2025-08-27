use std::fmt::{self, Debug};
use std::sync::Arc;

use datafusion::common::{DFSchema, DFSchemaRef};
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::logical_expr::utils::exprlist_to_fields;
use datafusion::logical_expr::{Extension, LogicalPlan, UserDefinedLogicalNodeCore};
use datafusion::prelude::Expr;

#[derive(Clone, PartialEq, Eq, Hash)]
pub struct TableWriterMergePlanNode {
    pub input: Arc<LogicalPlan>,
    pub agg_expr: Vec<Expr>,
    pub schema: DFSchemaRef,
}

impl PartialOrd for TableWriterMergePlanNode {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        match self.input.partial_cmp(&other.input) {
            Some(core::cmp::Ordering::Equal) => {}
            ord => return ord,
        }
        match self.agg_expr.partial_cmp(&other.agg_expr) {
            Some(core::cmp::Ordering::Equal) => {}
            ord => return ord,
        }
        self.schema.fields().partial_cmp(other.schema.fields())
    }
}

impl TableWriterMergePlanNode {
    pub fn try_new(input: Arc<LogicalPlan>, agg_expr: Vec<Expr>) -> Result<Self, DataFusionError> {
        let schema = Arc::new(DFSchema::new_with_metadata(
            exprlist_to_fields(&agg_expr, input.as_ref())?,
            input.schema().metadata().clone(),
        )?);

        Ok(Self {
            input,
            agg_expr,
            schema,
        })
    }
}

impl Debug for TableWriterMergePlanNode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.fmt_for_explain(f)
    }
}

impl UserDefinedLogicalNodeCore for TableWriterMergePlanNode {
    fn name(&self) -> &str {
        "TableWriterMerge"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<Expr> {
        self.agg_expr.clone()
    }

    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let out_exprs: Vec<String> = self.agg_expr.iter().map(|e| e.to_string()).collect();
        write!(f, "TableWriterMerge: {}", out_exprs.join(","))?;

        Ok(())
    }

    fn with_exprs_and_inputs(&self, exprs: Vec<Expr>, inputs: Vec<LogicalPlan>) -> DFResult<Self> {
        if inputs.len() != 1 {
            return Err(DataFusionError::Plan(
                "TableWriterMergePlanNode should have exactly one input".to_string(),
            ));
        }
        Ok(TableWriterMergePlanNode {
            input: Arc::new(inputs[0].clone()),
            agg_expr: exprs,
            schema: self.schema.clone(),
        })
    }
}

impl From<TableWriterMergePlanNode> for LogicalPlan {
    fn from(val: TableWriterMergePlanNode) -> Self {
        LogicalPlan::Extension(Extension {
            node: Arc::new(val),
        })
    }
}
