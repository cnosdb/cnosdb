use std::sync::Arc;

use datafusion::{
    logical_expr::LogicalPlan,
    optimizer::{
        common_subexpr_eliminate::CommonSubexprEliminate,
        decorrelate_where_exists::DecorrelateWhereExists, decorrelate_where_in::DecorrelateWhereIn,
        eliminate_cross_join::EliminateCrossJoin, eliminate_filter::EliminateFilter,
        eliminate_limit::EliminateLimit, eliminate_outer_join::EliminateOuterJoin,
        extract_equijoin_predicate::ExtractEquijoinPredicate,
        filter_null_join_keys::FilterNullJoinKeys, inline_table_scan::InlineTableScan,
        propagate_empty_relation::PropagateEmptyRelation, push_down_filter::PushDownFilter,
        push_down_limit::PushDownLimit, rewrite_disjunctive_predicate::RewriteDisjunctivePredicate,
        scalar_subquery_to_join::ScalarSubqueryToJoin, simplify_expressions::SimplifyExpressions,
        single_distinct_to_groupby::SingleDistinctToGroupBy, type_coercion::TypeCoercion,
        unwrap_cast_in_comparison::UnwrapCastInComparison, OptimizerRule,
    },
};

use spi::query::session::IsiphoSessionCtx;
use spi::Result;

use crate::extension::logical::optimizer_rule::{
    implicit_type_conversion::ImplicitTypeConversion,
    push_down_projection::PushDownProjectionAdapter, reject_cross_join::RejectCrossJoin,
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
            Arc::new(InlineTableScan::new()),
            Arc::new(TypeCoercion::new()),
            Arc::new(SimplifyExpressions::new()),
            Arc::new(UnwrapCastInComparison::new()),
            Arc::new(DecorrelateWhereExists::new()),
            Arc::new(DecorrelateWhereIn::new()),
            Arc::new(ScalarSubqueryToJoin::new()),
            Arc::new(ExtractEquijoinPredicate::new()),
            // simplify expressions does not simplify expressions in subqueries, so we
            // run it again after running the optimizations that potentially converted
            // subqueries to joins
            Arc::new(SimplifyExpressions::new()),
            Arc::new(RewriteDisjunctivePredicate::new()),
            Arc::new(EliminateFilter::new()),
            Arc::new(EliminateCrossJoin::new()),
            Arc::new(CommonSubexprEliminate::new()),
            Arc::new(EliminateLimit::new()),
            Arc::new(PropagateEmptyRelation::new()),
            Arc::new(FilterNullJoinKeys::default()),
            Arc::new(EliminateOuterJoin::new()),
            // Filters can't be pushed down past Limits, we should do PushDownFilter after LimitPushDown
            Arc::new(PushDownLimit::new()),
            Arc::new(PushDownFilter::new()),
            Arc::new(SingleDistinctToGroupBy::new()),
            // The previous optimizations added expressions and projections,
            // that might benefit from the following rules
            Arc::new(SimplifyExpressions::new()),
            Arc::new(UnwrapCastInComparison::new()),
            Arc::new(CommonSubexprEliminate::new()),
            Arc::new(PushDownProjectionAdapter::new()),
            // df default rules end
            // cnosdb rules
            Arc::new(RewriteTagScan {}),
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
