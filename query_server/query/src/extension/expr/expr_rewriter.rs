use datafusion::common::tree_node::{Transformed, TreeNodeRewriter};
use datafusion::error::Result;
use datafusion::prelude::Expr;

pub struct ExprReplacer<'a, F> {
    replacer: &'a F,
}

impl<'a, F> ExprReplacer<'a, F> {
    pub fn new(replacer: &'a F) -> Self {
        Self { replacer }
    }
}

impl<'a, F> TreeNodeRewriter for ExprReplacer<'a, F>
where
    F: Fn(&Expr) -> Option<Expr>,
{
    type Node = Expr;
    /// Invoked after all children of `expr` have been mutated and
    /// returns a potentially modified expr.
    fn f_up(&mut self, expr: Expr) -> Result<Transformed<Self::Node>> {
        if let Some(new_expr) = (self.replacer)(&expr) {
            return Ok(Transformed::yes(new_expr));
        }

        Ok(Transformed::no(expr))
    }
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::datatypes::DataType;
    use datafusion::common::tree_node::TreeNode;
    use datafusion::error::Result;
    use datafusion::prelude::{cast, col, lit, Column, Expr};

    use super::ExprReplacer;
    use crate::extension::expr::expr_fn::plus;

    #[test]
    fn test() -> Result<()> {
        let expr = cast(plus(col("a"), lit(1)), DataType::Float64);

        let mut rewriter = ExprReplacer::new(&|expr: &Expr| {
            if matches!(expr, Expr::Column( Column {
                name,
                ..
            }) if name == "a")
            {
                Some(col("b"))
            } else {
                None
            }
        });

        let expr = expr.rewrite(&mut rewriter)?;

        assert_eq!("CAST(b + Int32(1) AS Float64)", format!("{}", expr.data));

        Ok(())
    }
}
