use std::fmt::{self, Debug};
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use datafusion::common::{DFSchema, DFSchemaRef};
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::logical_expr::utils::exprlist_to_fields;
use datafusion::logical_expr::{Extension, LogicalPlan, TableSource, UserDefinedLogicalNodeCore};
use datafusion::prelude::Expr;

#[derive(Clone, PartialOrd)]
pub struct TableWriterPlanNode {
    pub target_table_name: String,
    pub target_table: Arc<dyn TableSource>,
    pub input: Arc<LogicalPlan>,
    pub exprs: Vec<Expr>,
    pub schema: DFSchemaRef,
}

impl TableWriterPlanNode {
    pub fn try_new(
        target_table_name: String,
        target_table: Arc<dyn TableSource>,
        input: Arc<LogicalPlan>,
        exprs: Vec<Expr>,
    ) -> Result<Self, DataFusionError> {
        let schema = Arc::new(DFSchema::new_with_metadata(
            exprlist_to_fields(&exprs, input.as_ref())?,
            input.schema().metadata().clone(),
        )?);

        Ok(Self {
            target_table_name,
            target_table,
            input,
            exprs,
            schema,
        })
    }

    pub fn target_table(&self) -> Arc<dyn TableSource> {
        self.target_table.clone()
    }

    pub fn target_table_name(&self) -> &str {
        self.target_table_name.as_str()
    }
}

impl Debug for TableWriterPlanNode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.fmt_for_explain(f)
    }
}

impl Hash for TableWriterPlanNode {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.target_table_name.hash(state);
        self.input.hash(state);
        self.exprs.hash(state);
        self.schema.hash(state);
    }
}

impl PartialEq for TableWriterPlanNode {
    fn eq(&self, other: &Self) -> bool {
        self.target_table_name == other.target_table_name
            && self.input == other.input
            && self.exprs == other.exprs
            && self.schema == other.schema
    }
}

impl Eq for TableWriterPlanNode {}

impl UserDefinedLogicalNodeCore for TableWriterPlanNode {
    fn name(&self) -> &str {
        "TableWriter"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<Expr> {
        self.exprs.clone()
    }

    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let out_exprs: Vec<String> = self.exprs.iter().map(|e| e.to_string()).collect();
        write!(
            f,
            "TableWriter: table={}, {}",
            self.target_table_name,
            out_exprs.join(",")
        )?;

        Ok(())
    }

    fn with_exprs_and_inputs(&self, exprs: Vec<Expr>, inputs: Vec<LogicalPlan>) -> DFResult<Self> {
        if inputs.len() != 1 {
            return Err(DataFusionError::Plan(
                "TableWriterPlanNode: input size inconsistent".to_string(),
            ));
        }
        Ok(TableWriterPlanNode {
            target_table_name: self.target_table_name.clone(),
            target_table: self.target_table.clone(),
            input: Arc::new(inputs[0].clone()),
            exprs,
            schema: self.schema.clone(),
        })
    }
}

impl From<TableWriterPlanNode> for LogicalPlan {
    fn from(val: TableWriterPlanNode) -> Self {
        LogicalPlan::Extension(Extension {
            node: Arc::new(val),
        })
    }
}
