use std::{
    any::Any,
    fmt::{self, Debug},
    sync::Arc,
};

use datafusion::{
    common::DFSchemaRef,
    logical_expr::TableSource,
    logical_expr::{LogicalPlan, UserDefinedLogicalNode},
    prelude::Expr,
};

#[derive(Clone)]
pub struct TableWriterPlanNode {
    pub target_table_name: String,
    pub target_table: Arc<dyn TableSource>,
    pub input: Arc<LogicalPlan>,
    pub output_exprs: Vec<Expr>,
}

impl TableWriterPlanNode {
    pub fn new(
        target_table_name: String,
        target_table: Arc<dyn TableSource>,
        input: Arc<LogicalPlan>,
        output_exprs: Vec<Expr>,
    ) -> Self {
        Self {
            target_table_name,
            target_table,
            input,
            output_exprs,
        }
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
        self.input.schema()
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
        assert_eq!(inputs.len(), 1, "input size inconsistent");
        Arc::new(TableWriterPlanNode {
            target_table_name: self.target_table_name.clone(),
            target_table: self.target_table.clone(),
            input: Arc::new(inputs[0].clone()),
            output_exprs: exprs.to_vec(),
        })
    }
}
