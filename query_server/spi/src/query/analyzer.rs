use std::sync::Arc;

use datafusion::logical_expr::LogicalPlan;

use super::session::SessionCtx;
use crate::Result;

pub type AnalyzerRef = Arc<dyn Analyzer + Send + Sync>;

pub trait Analyzer {
    fn analyze(&self, plan: &LogicalPlan, session: &SessionCtx) -> Result<LogicalPlan>;
}
