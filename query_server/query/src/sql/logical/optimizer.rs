use std::sync::Arc;

use datafusion::logical_expr::LogicalPlan;
use datafusion::optimizer::common_subexpr_eliminate::CommonSubexprEliminate;
use datafusion::optimizer::decorrelate_where_exists::DecorrelateWhereExists;
use datafusion::optimizer::decorrelate_where_in::DecorrelateWhereIn;
use datafusion::optimizer::eliminate_cross_join::EliminateCrossJoin;
use datafusion::optimizer::eliminate_filter::EliminateFilter;
use datafusion::optimizer::eliminate_limit::EliminateLimit;
use datafusion::optimizer::eliminate_outer_join::EliminateOuterJoin;
use datafusion::optimizer::extract_equijoin_predicate::ExtractEquijoinPredicate;
use datafusion::optimizer::filter_null_join_keys::FilterNullJoinKeys;
use datafusion::optimizer::inline_table_scan::InlineTableScan;
use datafusion::optimizer::propagate_empty_relation::PropagateEmptyRelation;
use datafusion::optimizer::push_down_aggregation::PushDownAggregation;
use datafusion::optimizer::push_down_filter::PushDownFilter;
use datafusion::optimizer::push_down_limit::PushDownLimit;
use datafusion::optimizer::rewrite_disjunctive_predicate::RewriteDisjunctivePredicate;
use datafusion::optimizer::scalar_subquery_to_join::ScalarSubqueryToJoin;
use datafusion::optimizer::simplify_expressions::SimplifyExpressions;
use datafusion::optimizer::single_distinct_to_groupby::SingleDistinctToGroupBy;
use datafusion::optimizer::type_coercion::TypeCoercion;
use datafusion::optimizer::unwrap_cast_in_comparison::UnwrapCastInComparison;
use datafusion::optimizer::OptimizerRule;
use spi::query::session::SessionCtx;
use spi::Result;

use crate::extension::logical::optimizer_rule::implicit_type_conversion::ImplicitTypeConversion;
use crate::extension::logical::optimizer_rule::push_down_projection::PushDownProjectionAdapter;
use crate::extension::logical::optimizer_rule::reject_cross_join::RejectCrossJoin;
use crate::extension::logical::optimizer_rule::rewrite_tag_scan::RewriteTagScan;
use crate::extension::logical::optimizer_rule::transform_bottom_func_to_topk_node::TransformBottomFuncToTopkNodeRule;
use crate::extension::logical::optimizer_rule::transform_time_window::TransformTimeWindowRule;
use crate::extension::logical::optimizer_rule::transform_topk_func_to_topk_node::TransformTopkFuncToTopkNodeRule;

pub trait LogicalOptimizer: Send + Sync {
    fn optimize(&self, plan: &LogicalPlan, session: &SessionCtx) -> Result<LogicalPlan>;

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
            Arc::new(PushDownAggregation::new()),
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
            Arc::new(TransformTimeWindowRule),
        ];

        Self { rules }
    }
}

impl LogicalOptimizer for DefaultLogicalOptimizer {
    fn optimize(&self, plan: &LogicalPlan, session: &SessionCtx) -> Result<LogicalPlan> {
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
