use std::sync::Arc;

use datafusion::logical_expr::LogicalPlan;
use datafusion::optimizer::common_subexpr_eliminate::CommonSubexprEliminate;
use datafusion::optimizer::decorrelate_where_exists::DecorrelateWhereExists;
use datafusion::optimizer::decorrelate_where_in::DecorrelateWhereIn;
use datafusion::optimizer::eliminate_filter::EliminateFilter;
use datafusion::optimizer::eliminate_limit::EliminateLimit;
use datafusion::optimizer::filter_null_join_keys::FilterNullJoinKeys;
use datafusion::optimizer::filter_push_down::FilterPushDown;
use datafusion::optimizer::limit_push_down::LimitPushDown;
use datafusion::optimizer::reduce_cross_join::ReduceCrossJoin;
use datafusion::optimizer::reduce_outer_join::ReduceOuterJoin;
use datafusion::optimizer::rewrite_disjunctive_predicate::RewriteDisjunctivePredicate;
use datafusion::optimizer::scalar_subquery_to_join::ScalarSubqueryToJoin;
use datafusion::optimizer::simplify_expressions::SimplifyExpressions;
use datafusion::optimizer::single_distinct_to_groupby::SingleDistinctToGroupBy;
use datafusion::optimizer::subquery_filter_to_join::SubqueryFilterToJoin;
use datafusion::optimizer::type_coercion::TypeCoercion;
use datafusion::optimizer::unwrap_cast_in_comparison::UnwrapCastInComparison;
use datafusion::optimizer::OptimizerRule;
use spi::query::session::IsiphoSessionCtx;
use spi::Result;

use crate::extension::logical::optimizer_rule::implicit_type_conversion::ImplicitTypeConversion;
use crate::extension::logical::optimizer_rule::projection_push_down::ProjectionPushDownAdapter;
use crate::extension::logical::optimizer_rule::reject_cross_join::RejectCrossJoin;
use crate::extension::logical::optimizer_rule::rewrite_tag_scan::RewriteTagScan;
use crate::extension::logical::optimizer_rule::transform_bottom_func_to_topk_node::TransformBottomFuncToTopkNodeRule;
use crate::extension::logical::optimizer_rule::transform_topk_func_to_topk_node::TransformTopkFuncToTopkNodeRule;

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
