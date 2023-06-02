use std::sync::Arc;

use datafusion::logical_expr::LogicalPlan;
use datafusion::optimizer::common_subexpr_eliminate::CommonSubexprEliminate;
use datafusion::optimizer::decorrelate_where_exists::DecorrelateWhereExists;
use datafusion::optimizer::decorrelate_where_in::DecorrelateWhereIn;
use datafusion::optimizer::eliminate_cross_join::EliminateCrossJoin;
use datafusion::optimizer::eliminate_duplicated_expr::EliminateDuplicatedExpr;
use datafusion::optimizer::eliminate_filter::EliminateFilter;
use datafusion::optimizer::eliminate_limit::EliminateLimit;
use datafusion::optimizer::eliminate_outer_join::EliminateOuterJoin;
use datafusion::optimizer::eliminate_project::EliminateProjection;
use datafusion::optimizer::extract_equijoin_predicate::ExtractEquijoinPredicate;
use datafusion::optimizer::filter_null_join_keys::FilterNullJoinKeys;
use datafusion::optimizer::merge_projection::MergeProjection;
use datafusion::optimizer::propagate_empty_relation::PropagateEmptyRelation;
use datafusion::optimizer::push_down_aggregation::PushDownAggregation;
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
use spi::query::session::SessionCtx;
use spi::Result;
use trace::debug;

use crate::extension::logical::optimizer_rule::rewrite_tag_scan::RewriteTagScan;
use crate::sql::analyzer::DefaultAnalyzer;

pub trait LogicalOptimizer: Send + Sync {
    fn optimize(&self, plan: &LogicalPlan, session: &SessionCtx) -> Result<LogicalPlan>;

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
            Arc::new(DecorrelateWhereExists::new()),
            Arc::new(DecorrelateWhereIn::new()),
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
            Arc::new(PushDownProjection::new()),
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
    fn optimize(&self, plan: &LogicalPlan, session: &SessionCtx) -> Result<LogicalPlan> {
        let analyzed_plan = {
            let mut span_recorder = session.get_child_span_recorder("analyze plan");

            self.analyzer
                .analyze(plan, session)
                .map(|p| {
                    span_recorder.ok("analyzer");
                    span_recorder.set_metadata(
                        "analyzed logical plan",
                        p.display_indent_schema().to_string(),
                    );
                    p
                })
                .map_err(|e| {
                    span_recorder.error(e.to_string());
                    e
                })?
        };

        debug!("Analyzed logical plan:\n{}\n", plan.display_indent_schema(),);

        let optimizeed_plan = {
            let mut span_recorder = session.get_child_span_recorder("optimize logical plan");
            session
                .inner()
                .state()
                .with_optimizer_rules(self.rules.clone())
                .optimize(&analyzed_plan)
                .map(|p| {
                    span_recorder.ok("optimize");
                    span_recorder.set_metadata(
                        "optimized logical plan",
                        p.display_indent_schema().to_string(),
                    );
                    p
                })
                .map_err(|e| {
                    span_recorder.error(e.to_string());
                    e
                })?
        };

        Ok(optimizeed_plan)
    }

    fn inject_optimizer_rule(&mut self, optimizer_rule: Arc<dyn OptimizerRule + Send + Sync>) {
        self.rules.push(optimizer_rule);
    }
}
