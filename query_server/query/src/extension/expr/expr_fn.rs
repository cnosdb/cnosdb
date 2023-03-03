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

/// Return a new expression `left * right`
pub fn multiply(left: Expr, right: Expr) -> Expr {
    binary_expr(left, Operator::Multiply, right)
}

/// Create is not null expression
pub fn is_not_null(expr: Expr) -> Expr {
    Expr::IsNotNull(Box::new(expr))
}

/// Return a new expression `left >= right`
pub fn ge(left: Expr, right: Expr) -> Expr {
    binary_expr(left, Operator::GtEq, right)
}

/// Return a new expression `left < right`
pub fn lt(left: Expr, right: Expr) -> Expr {
    binary_expr(left, Operator::Lt, right)
}
