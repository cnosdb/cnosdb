use datafusion::common::Result as DFResult;
use datafusion::logical_expr::LogicalPlan;

pub mod stream_checker;

pub trait AnalyzerRule {
    /// Rewrite `plan`
    fn analyze(&self, plan: &LogicalPlan) -> DFResult<Option<LogicalPlan>>;

    /// A human readable name for this analyzer rule
    fn name(&self) -> &str;
}
