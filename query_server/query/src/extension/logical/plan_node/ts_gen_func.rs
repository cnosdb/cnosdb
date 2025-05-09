use std::sync::Arc;

use datafusion::common::DFSchemaRef;
use datafusion::logical_expr::{Expr, ExprSchemable, LogicalPlan, UserDefinedLogicalNodeCore};
use models::arrow::DataType;

use crate::extension::expr::TimeSeriesGenFunc;

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct TimeSeriesGenFuncNode {
    pub time_expr: Expr,
    pub field_expr: Expr,
    pub arg_expr: Option<Expr>,
    pub input: Arc<LogicalPlan>,
    pub symbol: TimeSeriesGenFunc,
    pub schema: DFSchemaRef,
}

impl UserDefinedLogicalNodeCore for TimeSeriesGenFuncNode {
    fn name(&self) -> &str {
        "TimeSeriesGenFunc"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![self.input.as_ref()]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<Expr> {
        let mut exprs = vec![self.time_expr.clone(), self.field_expr.clone()];
        if let Some(arg_expr) = &self.arg_expr {
            exprs.push(arg_expr.clone());
        }
        exprs
    }

    fn fmt_for_explain(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "{}: time_expr={}, field_expr={}, arg_expr={}, func={}",
            self.name(),
            self.time_expr,
            self.field_expr,
            self.arg_expr.as_ref().map(|expr| expr.to_string()),
            self.symbol.name(),
        )
    }

    fn with_exprs_and_inputs(&self, mut exprs: Vec<Expr>, inputs: Vec<LogicalPlan>) -> Self {
        assert_eq!(inputs.len(), 1, "input size inconsistent");

        let input = inputs.remove(0);
        let ext_arg_expr = if exprs.len() >= 3 {
            match exprs[exprs.len() - 1].get_type(inputs.schema()) {
                Ok(DataType::Utf8) => exprs.pop(),
                _ => None,
            }
        } else {
            None
        };
        let time_expr = exprs[0].clone();
        let field_expr = exprs[1].clone();
        Self {
            time_expr,
            field_expr,
            arg_expr,
            input: Arc::new(input),
            symbol: self.symbol,
            schema: self.schema.clone(),
        }
    }
}
