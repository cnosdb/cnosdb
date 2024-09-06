use datafusion::common::Result as DFResult;
use datafusion::logical_expr::LogicalPlan;

pub mod initial_plan_checker;
pub mod stream_checker;
pub mod transform_bottom_func_to_topk_node;
pub mod transform_exact_count_to_count;
pub mod transform_time_window;
pub mod transform_topk_func_to_topk_node;
pub mod transform_ts_gen_func;
pub mod transform_update;

pub trait AnalyzerRule {
    /// Rewrite `plan`
    fn analyze(&self, plan: &LogicalPlan) -> DFResult<Option<LogicalPlan>>;

    /// A human readable name for this analyzer rule
    fn name(&self) -> &str;
}
