use datafusion::arrow::datatypes::DataType;
use datafusion::common::tree_node::{
    RewriteRecursion, TreeNode, TreeNodeRewriter, TreeNodeVisitor, VisitRecursion,
};
use datafusion::common::{DFSchemaRef, ScalarValue};
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::context::ExecutionProps;
use datafusion::logical_expr::{Expr, ExprSchemable};
use datafusion::optimizer::simplify_expressions::{ExprSimplifier, SimplifyContext};
use datafusion::optimizer::utils::conjunction;
use datafusion::prelude::lit;

pub fn is_udf_function(expr: &Expr) -> bool {
    matches!(expr, Expr::ScalarUDF(_) | Expr::AggregateUDF(_))
}

pub fn has_udf_function(expr: &Expr) -> Result<bool, DataFusionError> {
    let mut has_udf_visitor = UDFVisitor::default();
    expr.visit(&mut has_udf_visitor)?;
    Ok(has_udf_visitor.has_udf())
}

pub fn rewrite_filters(filters: &[Expr], df_schema: DFSchemaRef) -> Result<Option<Expr>> {
    let mut filter_expr_rewriter = FilterExprRewriter::new(df_schema.clone());
    let props = ExecutionProps::new();
    let simplify_cxt = SimplifyContext::new(&props).with_schema(df_schema);
    let simplifier = ExprSimplifier::new(simplify_cxt);

    let expr = conjunction(filters.iter().cloned())
        .map(|e| {
            e.rewrite(&mut filter_expr_rewriter)
                .and_then(|e| simplifier.simplify(e))
        })
        .transpose()?;
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

impl TreeNodeVisitor for UDFVisitor {
    type N = Expr;

    fn pre_visit(&mut self, node: &Self::N) -> Result<VisitRecursion> {
        if is_udf_function(node) {
            self.has_udf = true;
        }
        Ok(VisitRecursion::Continue)
    }
}

pub struct FilterExprRewriter {
    schema: DFSchemaRef,
    udf_cnt: usize,
}

impl FilterExprRewriter {
    pub fn new(schema: DFSchemaRef) -> Self {
        Self { schema, udf_cnt: 0 }
    }
}

impl TreeNodeRewriter for FilterExprRewriter {
    type N = Expr;
    fn pre_visit(&mut self, node: &Self::N) -> Result<RewriteRecursion> {
        if is_udf_function(node) {
            self.udf_cnt += 1;
        }
        Ok(RewriteRecursion::Continue)
    }

    fn mutate(&mut self, node: Self::N) -> Result<Self::N> {
        if self.udf_cnt.gt(&0) {
            let data_type = node.get_type(self.schema.as_ref())?;
            if matches!(data_type, DataType::Boolean) {
                self.udf_cnt -= 1;
                Ok(lit(true))
            } else {
                Ok(node)
            }
        } else {
            Ok(node)
        }
    }
}
