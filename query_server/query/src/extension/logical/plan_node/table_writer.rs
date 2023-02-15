use std::any::Any;
use std::fmt::{self, Debug};
use std::sync::Arc;

use datafusion::common::{DFSchema, DFSchemaRef};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::utils::exprlist_to_fields;
use datafusion::logical_expr::{LogicalPlan, TableSource, UserDefinedLogicalNode};
use datafusion::prelude::Expr;

#[derive(Clone)]
pub struct TableWriterPlanNode {
    pub target_table_name: String,
    pub target_table: Arc<dyn TableSource>,
    pub input: Arc<LogicalPlan>,
    pub output_exprs: Vec<Expr>,
    pub schema: DFSchemaRef,
}

impl TableWriterPlanNode {
    pub fn try_new(
        target_table_name: String,
        target_table: Arc<dyn TableSource>,
        input: Arc<LogicalPlan>,
        output_exprs: Vec<Expr>,
    ) -> Result<Self, DataFusionError> {
        let schema = Arc::new(DFSchema::new_with_metadata(
            exprlist_to_fields(&output_exprs, input.as_ref())?,
            input.schema().metadata().clone(),
        )?);

        Ok(Self {
            target_table_name,
            target_table,
            input,
            output_exprs,
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

impl UserDefinedLogicalNode for TableWriterPlanNode {
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
        self.output_exprs.clone()
    }

    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let out_exprs: Vec<String> = self.output_exprs.iter().map(|e| e.to_string()).collect();
        write!(f, "TableWriter: {}", out_exprs.join(","))?;
        for field in self.target_table.schema().fields() {
            write!(
                f,
                "\n    {} := {}({:?})",
                field.name(),
                field.data_type(),
                field.metadata()
            )?;
        }

        Ok(())
    }

    fn from_template(
        &self,
        exprs: &[Expr],
        inputs: &[LogicalPlan],
    ) -> Arc<dyn UserDefinedLogicalNode> {
        debug_assert_eq!(inputs.len(), 1, "input size inconsistent");
        Arc::new(TableWriterPlanNode {
            target_table_name: self.target_table_name.clone(),
            target_table: self.target_table.clone(),
            input: Arc::new(inputs[0].clone()),
            output_exprs: exprs.to_vec(),
            schema: self.schema.clone(),
        })
    }
}

pub fn as_table_writer_plan_node(
    node: &dyn UserDefinedLogicalNode,
) -> Option<&TableWriterPlanNode> {
    node.as_any().downcast_ref::<TableWriterPlanNode>()
}
