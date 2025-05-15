use datafusion::common::tree_node::TreeNode;
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::logical_expr::LogicalPlan;
use datafusion::prelude::Expr;

use crate::extension::expr::expr_rewriter::ExprReplacer;

pub mod expand;
pub mod stream_scan;
pub mod table_writer;
pub mod table_writer_merge;
pub mod tag_scan;
pub mod ts_gen_func;
pub mod update;
pub mod update_tag;
pub mod watermark;

pub trait LogicalPlanExt: Sized {
    type Error;
    fn transform_expressions_down<F>(&self, f: &F) -> Result<Self, Self::Error>
    where
        F: Fn(&Expr) -> Option<Expr>;
}

impl LogicalPlanExt for LogicalPlan {
    type Error = DataFusionError;

    fn transform_expressions_down<F>(&self, f: &F) -> Result<Self, Self::Error>
    where
        F: Fn(&Expr) -> Option<Expr>,
    {
        let mut replacer = ExprReplacer::new(f);

        let exprs = self
            .expressions()
            .into_iter()
            .map(|e| e.rewrite(&mut replacer))
            .collect::<DFResult<Vec<_>>>()?;

        let new_inputs = self.inputs().into_iter().cloned().collect::<Vec<_>>();

        self.with_new_exprs(exprs, new_inputs)
    }
}
