use std::sync::Arc;

use datafusion::logical_expr::LogicalPlan;
use datafusion::optimizer::common_subexpr_eliminate::CommonSubexprEliminate;
use datafusion::optimizer::decorrelate_predicate_subquery::DecorrelatePredicateSubquery;
use datafusion::optimizer::eliminate_cross_join::EliminateCrossJoin;
use datafusion::optimizer::eliminate_duplicated_expr::EliminateDuplicatedExpr;
use datafusion::optimizer::eliminate_filter::EliminateFilter;
use datafusion::optimizer::eliminate_join::EliminateJoin;
use datafusion::optimizer::eliminate_limit::EliminateLimit;
use datafusion::optimizer::eliminate_outer_join::EliminateOuterJoin;
use datafusion::optimizer::eliminate_project::EliminateProjection;
use datafusion::optimizer::extract_equijoin_predicate::ExtractEquijoinPredicate;
use datafusion::optimizer::filter_null_join_keys::FilterNullJoinKeys;
use datafusion::optimizer::merge_projection::MergeProjection;
use datafusion::optimizer::propagate_empty_relation::PropagateEmptyRelation;
use datafusion::optimizer::push_down_filter::PushDownFilter;
use datafusion::optimizer::push_down_limit::PushDownLimit;
use datafusion::optimizer::push_down_projection::PushDownProjection;
use datafusion::optimizer::replace_distinct_aggregate::ReplaceDistinctWithAggregate;
use datafusion::optimizer::rewrite_disjunctive_predicate::RewriteDisjunctivePredicate;
use datafusion::optimizer::scalar_subquery_to_join::ScalarSubqueryToJoin;
use datafusion::optimizer::simplify_expressions::SimplifyExpressions;
use datafusion::optimizer::single_distinct_to_groupby::SingleDistinctToGroupBy;
use datafusion::optimizer::unwrap_cast_in_comparison::UnwrapCastInComparison;
use datafusion::optimizer::OptimizerRule;
use spi::query::analyzer::AnalyzerRef;
use spi::query::logical_planner::QueryPlan;
use spi::query::session::SessionCtx;
use spi::QueryResult;
use trace::debug;
use trace::span_ext::SpanExt;

use crate::extension::logical::optimizer_rule::push_down_aggregation::PushDownAggregation;
use crate::extension::logical::optimizer_rule::rewrite_tag_scan::RewriteTagScan;
use crate::sql::analyzer::DefaultAnalyzer;

const PUSH_DOWN_PROJECTION_INDEX: usize = 24; // index of PushDownProjection in rules

pub trait LogicalOptimizer: Send + Sync {
    fn optimize(&self, plan: &QueryPlan, session: &SessionCtx) -> QueryResult<LogicalPlan>;

    fn inject_optimizer_rule(&mut self, optimizer_rule: Arc<dyn OptimizerRule + Send + Sync>);
}

pub struct DefaultLogicalOptimizer {
    // fit datafusion
    // TODO refactor
    analyzer: AnalyzerRef,
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
        let analyzer = Arc::new(DefaultAnalyzer::default());

        // additional optimizer rule
        let rules: Vec<Arc<dyn OptimizerRule + Send + Sync>> = vec![
            // df default rules start
            Arc::new(SimplifyExpressions::new()),
            Arc::new(UnwrapCastInComparison::new()),
            Arc::new(ReplaceDistinctWithAggregate::new()),
            Arc::new(EliminateJoin::new()),
            Arc::new(DecorrelatePredicateSubquery::new()),
            Arc::new(ScalarSubqueryToJoin::new()),
            Arc::new(ExtractEquijoinPredicate::new()),
            // simplify expressions does not simplify expressions in subqueries, so we
            // run it again after running the optimizations that potentially converted
            // subqueries to joins
            Arc::new(SimplifyExpressions::new()),
            Arc::new(MergeProjection::new()),
            Arc::new(RewriteDisjunctivePredicate::new()),
            Arc::new(EliminateDuplicatedExpr::new()),
            Arc::new(EliminateFilter::new()),
            Arc::new(EliminateCrossJoin::new()),
            Arc::new(CommonSubexprEliminate::new()),
            Arc::new(EliminateLimit::new()),
            Arc::new(PropagateEmptyRelation::new()),
            Arc::new(FilterNullJoinKeys::default()),
            Arc::new(EliminateOuterJoin::new()),
            // Filters can't be pushed down past Limits, we should do PushDownFilter after PushDownLimit
            Arc::new(PushDownLimit::new()),
            Arc::new(PushDownFilter::new()),
            Arc::new(SingleDistinctToGroupBy::new()),
            // The previous optimizations added expressions and projections,
            // that might benefit from the following rules
            Arc::new(SimplifyExpressions::new()),
            Arc::new(UnwrapCastInComparison::new()),
            Arc::new(CommonSubexprEliminate::new()),
            Arc::new(PushDownProjection::new(false)),
            Arc::new(EliminateProjection::new()),
            Arc::new(PushDownAggregation::new()),
            // PushDownProjection can pushdown Projections through Limits, do PushDownLimit again.
            Arc::new(PushDownLimit::new()),
            // df default rules end
            // cnosdb rules
            Arc::new(RewriteTagScan {}),
        ];

        Self { analyzer, rules }
    }
}

impl LogicalOptimizer for DefaultLogicalOptimizer {
    fn optimize(&self, plan: &QueryPlan, session: &SessionCtx) -> QueryResult<LogicalPlan> {
        let analyzed_plan = {
            let mut span = session.get_child_span("analyze plan");

            self.analyzer
                .analyze(&plan.df_plan, session)
                .inspect(|p| {
                    span.ok("analyzer");
                    span.add_property(|| {
                        (
                            "analyzed logical plan",
                            p.display_indent_schema().to_string(),
                        )
                    });
                })
                .inspect_err(|e| {
                    span.error(e.to_string());
                })?
        };

        debug!(
            "Analyzed logical plan:\n{}\n",
            plan.df_plan.display_indent_schema(),
        );

        let rules: Vec<Arc<dyn OptimizerRule + Send + Sync>> = if plan.is_tag_scan {
            let mut rules = self.rules.clone();
            rules[PUSH_DOWN_PROJECTION_INDEX] = Arc::new(PushDownProjection::new(plan.is_tag_scan));
            rules
        } else {
            self.rules.clone()
        };

        let optimizeed_plan = {
            let mut span = session.get_child_span("optimize logical plan");
            session
                .inner()
                .clone()
                .with_optimizer_rules(rules)
                .optimize(&analyzed_plan)
                .inspect(|p| {
                    span.ok("optimize");
                    span.add_property(|| {
                        (
                            "optimized logical plan",
                            p.display_indent_schema().to_string(),
                        )
                    });
                })
                .inspect_err(|e| {
                    span.error(e.to_string());
                })?
        };

        Ok(optimizeed_plan)
    }

    fn inject_optimizer_rule(&mut self, optimizer_rule: Arc<dyn OptimizerRule + Send + Sync>) {
        self.rules.push(optimizer_rule);
    }
}
