use std::sync::Arc;

use datafusion::{
    logical_expr::LogicalPlan,
    optimizer::{
        common_subexpr_eliminate::CommonSubexprEliminate,
        decorrelate_where_exists::DecorrelateWhereExists, decorrelate_where_in::DecorrelateWhereIn,
        eliminate_filter::EliminateFilter, eliminate_limit::EliminateLimit,
        filter_null_join_keys::FilterNullJoinKeys, filter_push_down::FilterPushDown,
        limit_push_down::LimitPushDown, reduce_cross_join::ReduceCrossJoin,
        reduce_outer_join::ReduceOuterJoin,
        rewrite_disjunctive_predicate::RewriteDisjunctivePredicate,
        scalar_subquery_to_join::ScalarSubqueryToJoin, simplify_expressions::SimplifyExpressions,
        single_distinct_to_groupby::SingleDistinctToGroupBy,
        subquery_filter_to_join::SubqueryFilterToJoin, type_coercion::TypeCoercion,
        unwrap_cast_in_comparison::UnwrapCastInComparison, OptimizerRule,
    },
};

use spi::query::session::IsiphoSessionCtx;
use spi::Result;

use crate::extension::logical::optimizer_rule::{
    implicit_type_conversion::ImplicitTypeConversion,
    projection_push_down::ProjectionPushDownAdapter, reject_cross_join::RejectCrossJoin,
    rewrite_tag_scan::RewriteTagScan,
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
            Arc::new(RejectCrossJoin {}),
            // data type conv
            Arc::new(ImplicitTypeConversion {}),
            // df default rules start
            Arc::new(TypeCoercion::new()),
            Arc::new(SimplifyExpressions::new()),
            Arc::new(UnwrapCastInComparison::new()),
            Arc::new(DecorrelateWhereExists::new()),
            Arc::new(DecorrelateWhereIn::new()),
            Arc::new(ScalarSubqueryToJoin::new()),
            Arc::new(SubqueryFilterToJoin::new()),
            Arc::new(EliminateFilter::new()),
            Arc::new(ReduceCrossJoin::new()),
            Arc::new(CommonSubexprEliminate::new()),
            Arc::new(EliminateLimit::new()),
            Arc::new(ProjectionPushDownAdapter::new()),
            // df default rules end
            // cnosdb rules
            Arc::new(RewriteTagScan {}),
            // df default rules start
            Arc::new(RewriteDisjunctivePredicate::new()),
            Arc::new(FilterNullJoinKeys::default()),
            Arc::new(ReduceOuterJoin::new()),
            Arc::new(FilterPushDown::new()),
            Arc::new(LimitPushDown::new()),
            Arc::new(SingleDistinctToGroupBy::new()),
            // df default rules end
            // cnosdb rules
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
            .map_err(|e| e.into())
    }

    fn inject_optimizer_rule(&mut self, optimizer_rule: Arc<dyn OptimizerRule + Send + Sync>) {
        self.rules.push(optimizer_rule);
    }
}
