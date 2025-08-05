use std::sync::Arc;

use datafusion::common::DFSchemaRef;
use datafusion::error::{DataFusionError, Result as DFResult};
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

impl PartialOrd for TimeSeriesGenFuncNode {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        match self.time_expr.partial_cmp(&other.time_expr) {
            Some(core::cmp::Ordering::Equal) => {}
            ord => return ord,
        }
        match self.field_expr.partial_cmp(&other.field_expr) {
            Some(core::cmp::Ordering::Equal) => {}
            ord => return ord,
        }
        match self.arg_expr.partial_cmp(&other.arg_expr) {
            Some(core::cmp::Ordering::Equal) => {}
            ord => return ord,
        }
        match self.input.partial_cmp(&other.input) {
            Some(core::cmp::Ordering::Equal) => {}
            ord => return ord,
        }
        self.schema.fields().partial_cmp(other.schema.fields())
    }
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
            "{}: time_expr={}, field_expr={}, arg_expr={:?}, func={}",
            self.name(),
            self.time_expr,
            self.field_expr,
            self.arg_expr.as_ref().map(|expr| expr.to_string()),
            self.symbol.name(),
        )
    }

    fn with_exprs_and_inputs(
        &self,
        mut exprs: Vec<Expr>,
        mut inputs: Vec<LogicalPlan>,
    ) -> DFResult<Self> {
        if inputs.len() != 1 {
            return Err(DataFusionError::Plan(
                "TimeSeriesGenFuncNode should have exactly one input".to_string(),
            ));
        }

        let input = inputs.remove(0);
        let arg_expr = if exprs.len() >= 3 {
            match exprs[exprs.len() - 1].get_type(input.schema()) {
                Ok(DataType::Utf8) => exprs.pop(),
                _ => None,
            }
        } else {
            None
        };
        let time_expr = exprs[0].clone();
        let field_expr = exprs[1].clone();
        Ok(Self {
            time_expr,
            field_expr,
            arg_expr,
            input: Arc::new(input),
            symbol: self.symbol,
            schema: self.schema.clone(),
        })
    }
}
