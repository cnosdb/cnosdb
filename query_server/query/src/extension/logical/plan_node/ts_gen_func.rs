use std::sync::Arc;

use datafusion::common::DFSchemaRef;
use datafusion::logical_expr::{Expr, ExprSchemable, LogicalPlan, UserDefinedLogicalNodeCore};
use models::arrow::DataType;

use crate::extension::expr::TSGenFunc;

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct TSGenFuncNode {
    pub time_expr: Expr,
    pub field_exprs: Vec<Expr>,
    pub arg_expr: Option<Expr>,
    pub input: Arc<LogicalPlan>,
    pub symbol: TSGenFunc,
    pub schema: DFSchemaRef,
}

impl UserDefinedLogicalNodeCore for TSGenFuncNode {
    fn name(&self) -> &str {
        "TSGenFunc"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![self.input.as_ref()]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<Expr> {
        let mut exprs = vec![self.time_expr.clone()];
        exprs.extend(self.field_exprs.clone());
        if let Some(arg_expr) = &self.arg_expr {
            exprs.push(arg_expr.clone());
        }
        exprs
    }

    fn fmt_for_explain(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "{}: time_expr={}, field_exprs=[{}], arg_expr={:?}, func={}",
            self.name(),
            self.time_expr,
            self.field_exprs
                .iter()
                .map(|expr| expr.to_string())
                .collect::<Vec<_>>()
                .join(","),
            self.arg_expr.as_ref().map(|expr| expr.to_string()),
            self.symbol.name(),
        )
    }

    fn with_exprs_and_inputs(&self, mut exprs: Vec<Expr>, inputs: Vec<LogicalPlan>) -> Self {
        assert_eq!(inputs.len(), 1, "input size inconsistent");
        let arg_expr =
            if exprs[exprs.len() - 1].get_type(inputs[0].schema()).unwrap() == DataType::Utf8 {
                exprs.pop()
            } else {
                None
            };
        let time_expr = exprs.remove(0);
        let field_exprs = exprs;
        Self {
            time_expr,
            field_exprs,
            arg_expr,
            input: Arc::new(inputs[0].clone()),
            symbol: self.symbol,
            schema: self.schema.clone(),
        }
    }
}
