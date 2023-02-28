use datafusion::logical_expr::Operator;
use datafusion::prelude::{binary_expr, Expr};

/// Return a new expression `left - right`
pub fn minus(left: Expr, right: Expr) -> Expr {
    binary_expr(left, Operator::Minus, right)
}

/// Return a new expression `left / right`
pub fn divide(left: Expr, right: Expr) -> Expr {
    binary_expr(left, Operator::Divide, right)
}

/// Return a new expression `left + right`
pub fn plus(left: Expr, right: Expr) -> Expr {
    binary_expr(left, Operator::Plus, right)
}

/// Return a new expression `left % right`
pub fn modulo(left: Expr, right: Expr) -> Expr {
    binary_expr(left, Operator::Modulo, right)
}

/// Return a new expression `left % right`
pub fn multiply(left: Expr, right: Expr) -> Expr {
    binary_expr(left, Operator::Multiply, right)
}
