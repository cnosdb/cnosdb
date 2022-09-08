use std::sync::Arc;

use datafusion::{
    logical_plan::LogicalPlan,
    optimizer::{
        common_subexpr_eliminate::CommonSubexprEliminate, eliminate_filter::EliminateFilter,
        eliminate_limit::EliminateLimit, filter_null_join_keys::FilterNullJoinKeys,
        filter_push_down::FilterPushDown, limit_push_down::LimitPushDown,
        projection_push_down::ProjectionPushDown, simplify_expressions::SimplifyExpressions,
        single_distinct_to_groupby::SingleDistinctToGroupBy,
        subquery_filter_to_join::SubqueryFilterToJoin, OptimizerRule,
    },
};

use spi::query::{session::IsiphoSessionCtx, Result};

use snafu::ResultExt;
use spi::query::LogicalOptimizeSnafu;

use crate::extension::logical::optimizer_rule::{
    merge_limit_with_sort::MergeLimitWithSortRule,
    transform_bottom_func_to_topk_node::TransformBottomFuncToTopkNodeRule,
    transform_topk_func_to_topk_node::TransformTopkFuncToTopkNodeRule,
};

pub trait LogicalOptimizer: Send + Sync {
    fn optimize(&self, plan: &LogicalPlan, session: &IsiphoSessionCtx) -> Result<LogicalPlan>;

    fn inject_optimizer_rule(&mut self, optimizer_rule: Arc<dyn OptimizerRule + Send + Sync>);
}

pub struct DefaultLogicalOptimizer {
    rules: Vec<Arc<dyn OptimizerRule + Send + Sync>>,
}

impl DefaultLogicalOptimizer {
    #[allow(dead_code)]
    fn with_optimizer_rules(mut self, rules: Vec<Arc<dyn OptimizerRule + Send + Sync>>) -> Self {
        self.rules = rules;
        self
    }
}

impl Default for DefaultLogicalOptimizer {
    fn default() -> Self {
        // additional optimizer rule
        let rules: Vec<Arc<dyn OptimizerRule + Send + Sync>> = vec![
            // df default rules
            Arc::new(SimplifyExpressions::new()),
            Arc::new(SubqueryFilterToJoin::new()),
            Arc::new(EliminateFilter::new()),
            Arc::new(CommonSubexprEliminate::new()),
            Arc::new(EliminateLimit::new()),
            Arc::new(ProjectionPushDown::new()),
            Arc::new(FilterNullJoinKeys::default()),
            Arc::new(FilterPushDown::new()),
            Arc::new(LimitPushDown::new()),
            Arc::new(SingleDistinctToGroupBy::new()),
            // cnosdb rules
            Arc::new(MergeLimitWithSortRule {}),
            Arc::new(TransformBottomFuncToTopkNodeRule {}),
            Arc::new(TransformTopkFuncToTopkNodeRule {}),
        ];

        Self { rules }
    }
}

impl LogicalOptimizer for DefaultLogicalOptimizer {
    fn optimize(&self, plan: &LogicalPlan, session: &IsiphoSessionCtx) -> Result<LogicalPlan> {
        session
            .inner()
            .state()
            .with_optimizer_rules(self.rules.clone())
            .optimize(plan)
            .context(LogicalOptimizeSnafu)
    }

    fn inject_optimizer_rule(&mut self, optimizer_rule: Arc<dyn OptimizerRule + Send + Sync>) {
        self.rules.push(optimizer_rule);
    }
}
