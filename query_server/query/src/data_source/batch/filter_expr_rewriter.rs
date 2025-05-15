use datafusion::common::tree_node::{
    Transformed, TreeNode, TreeNodeRecursion, TreeNodeRewriter, TreeNodeVisitor,
};
use datafusion::common::{DFSchemaRef, ScalarValue};
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::context::ExecutionProps;
use datafusion::logical_expr::{Expr, Operator};
use datafusion::optimizer::simplify_expressions::{ExprSimplifier, SimplifyContext};
use datafusion::physical_expr::utils::conjunction;

pub fn is_udf_function(expr: &Expr) -> bool {
    matches!(expr, Expr::ScalarUDF(_) | Expr::AggregateUDF(_))
}

pub fn has_udf_function(expr: &Expr) -> Result<bool, DataFusionError> {
    let mut has_udf_visitor = UDFVisitor::default();
    expr.visit(&mut has_udf_visitor)?;
    Ok(has_udf_visitor.has_udf())
}

pub fn rewrite_filters(filters: &[Expr], df_schema: DFSchemaRef) -> Result<Option<Expr>> {
    let mut filter_expr_rewriter = FilterExprRewriter {};
    let props = ExecutionProps::new();
    let simplify_cxt = SimplifyContext::new(&props).with_schema(df_schema);
    let simplifier = ExprSimplifier::new(simplify_cxt);

    let expr = conjunction(filters.iter().cloned())
        .map(|e| {
            e.rewrite(&mut filter_expr_rewriter)
                .and_then(|e| simplifier.simplify(e))
        })
        .transpose()?
        .map(|e| {
            let mut udf_visitor = UDFVisitor::default();
            e.visit(&mut udf_visitor)
                .map(|_| if udf_visitor.has_udf() { None } else { Some(e) })
        })
        .transpose()?
        .flatten();

    if matches!(expr, Some(Expr::Literal(ScalarValue::Boolean(Some(true))))) {
        Ok(None)
    } else {
        Ok(expr)
    }
}

// Visitor expr if has udf expr
#[derive(Default)]
pub struct UDFVisitor {
    has_udf: bool,
}

impl UDFVisitor {
    pub fn has_udf(&self) -> bool {
        self.has_udf
    }
}

impl<'a> TreeNodeVisitor<'a> for UDFVisitor {
    type Node = Expr;

    fn f_down(&mut self, node: &Self::Node) -> Result<TreeNodeRecursion> {
        if is_udf_function(node) {
            self.has_udf = true;
        }
        Ok(TreeNodeRecursion::Continue)
    }
}

pub struct FilterExprRewriter {}

impl TreeNodeRewriter for FilterExprRewriter {
    type Node = Expr;

    fn f_up(&mut self, node: Self::Node) -> Result<Transformed<Self::Node>> {
        match &node {
            Expr::BinaryExpr(bin) => {
                if matches!(bin.op, Operator::And | Operator::Or) {
                    let mut udf_visitor = UDFVisitor::default();
                    bin.left.visit(&mut udf_visitor)?;
                    let left_has_udf = udf_visitor.has_udf();

                    let mut udf_visitor = UDFVisitor::default();
                    bin.right.visit(&mut udf_visitor)?;
                    let right_has_udf = udf_visitor.has_udf();

                    if matches!(bin.op, Operator::And) {
                        match (left_has_udf, right_has_udf) {
                            (false, false) => return Ok(node),
                            (true, true) => {
                                return Ok(Expr::Literal(ScalarValue::Boolean(Some(true))))
                            }
                            (true, _) => return Ok(bin.right.as_ref().clone()),
                            (_, true) => return Ok(bin.left.as_ref().clone()),
                        }
                    }

                    if matches!(bin.op, Operator::Or) {
                        match (left_has_udf, right_has_udf) {
                            (false, false) => return Ok(node),
                            _ => return Ok(Expr::Literal(ScalarValue::Boolean(Some(true)))),
                        }
                    }
                }
                Ok(node)
            }
            _ => Ok(node),
        }
    }
}
