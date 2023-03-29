use std::any::Any;
use std::fmt::{self, Debug};
use std::sync::Arc;

use datafusion::common::{DFSchema, DFSchemaRef};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::utils::exprlist_to_fields;
use datafusion::logical_expr::{Extension, LogicalPlan, UserDefinedLogicalNode};
use datafusion::prelude::Expr;

#[derive(Clone)]
pub struct TableWriterMergePlanNode {
    pub input: Arc<LogicalPlan>,
    pub agg_expr: Vec<Expr>,
    pub schema: DFSchemaRef,
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

impl UserDefinedLogicalNode for TableWriterMergePlanNode {
    fn as_any(&self) -> &dyn Any {
        self
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

    fn from_template(
        &self,
        exprs: &[Expr],
        inputs: &[LogicalPlan],
    ) -> Arc<dyn UserDefinedLogicalNode> {
        debug_assert_eq!(inputs.len(), 1, "input size inconsistent");
        Arc::new(TableWriterMergePlanNode {
            input: Arc::new(inputs[0].clone()),
            agg_expr: exprs.to_vec(),
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
